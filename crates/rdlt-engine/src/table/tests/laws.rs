//! What every schema resolution keeps, checked over drawn tables, settings and batches.
//!
//! Every resolution that is not refused:
//!
//! 1. Loses no value: each column goes to a column of its own, or a variant of it, that holds its
//!    values as they are or casts them exactly (integers no float rounds into floats); a column of
//!    only nulls the table lacks is skipped; JSON text splits over its own column of another type,
//!    the rest going to a JSON variant or dropped by policy; anything else is dropped by policy.
//!    A merge key's values land only in its own column.
//! 2. Only grows the table: existing columns keep their order, identifiers, source columns and
//!    nullability, and each type only widens; names only extend; the changes are exactly the
//!    columns added and widened; the version advances by one exactly when there are changes, or
//!    from 0 to 1 when a normalized stream's table is created.
//! 3. Is idempotent: resolving the same batch against the resolved model changes nothing and
//!    routes it alike.
//! 4. Is order-free: the batch's columns in another order are refused alike, or give the same
//!    version, identifiers and types, and route each column to the same identifiers; only the
//!    added columns' order follows the batch's.
//!
//! Resolution is not order-free across batches: two batches resolved in either order may give
//! different layouts, as the last test shows.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use proptest::prelude::*;
use rdlt_connector::strategy::{number_type, scalar_type};
use rdlt_connector::{
    Capabilities, ColumnKey, ColumnPath, Field, LogicalType, SchemaChanges, StreamName,
    TableSchema, TypeKind,
};

use super::{capabilities, columns, created, plan, resolver, schema};
use crate::error::ErrorKind;
use crate::naming::Naming;
use crate::plan::StreamPlan;
use crate::policy::{OnUnsupported, SchemaPolicy, SchemaSettings};
use crate::table::lower::{LineageColumns, MetaNames};
use crate::table::model::{Creation, Model};
use crate::table::resolve::{Change, Incoming, Resolution, Resolver, Rest, Route, Settings};

/// Every column a batch may hold; a table's history holds the first six, and the last two take
/// the identifiers a variant of `a` and of `b` would.
const PATHS: [&str; 8] = ["a", "b", "c", "d", "e", "f", "a__json", "b__float64"];

/// The columns a table's history draws from.
const HISTORY_PATHS: usize = 6;

/// The most columns a batch holds.
const BATCH_COLUMNS: usize = 6;

/// One drawn case: a destination, a stream's settings, the batches that made its table from
/// nothing, and the batch resolved against it.
#[derive(Clone, Debug)]
struct Case {
    capabilities: Capabilities,
    stream: StreamSettings,
    history: Vec<Batch>,
    batch: Batch,
}

/// A stream's settings, as the checks read them.
#[derive(Clone, Debug)]
struct StreamSettings {
    /// The settings of each of [`PATHS`], in order.
    columns: Vec<ColumnSettings>,
    key: Vec<ColumnPath>,
    lineage: LineageColumns,
    unwidened: BTreeSet<ColumnKey>,
}

#[derive(Clone, Debug)]
struct ColumnSettings {
    policy: SchemaPolicy,
    on_unsupported: OnUnsupported,
    hint: Option<LogicalType>,
}

/// A batch's columns, as resolution reads them, and the same columns in another order.
#[derive(Clone, Debug)]
struct Batch {
    columns: Vec<Column>,
    creation: Creation,
    /// A permutation of the columns: positions into `columns`.
    order: Vec<usize>,
}

#[derive(Clone, Debug)]
struct Column {
    path: ColumnPath,
    logical: LogicalType,
    /// Whether the column holds an integer a 64-bit float would round.
    rounding: bool,
}

/// Which settings a resolver follows: the drawn ones, or those a table's history followed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Policies {
    Drawn,
    Default,
}

/// What the checks read of a resolver's settings.
struct Expected<'a> {
    stream: &'a StreamSettings,
    policies: Policies,
}

impl Expected<'_> {
    fn policy(&self, path: &ColumnPath) -> SchemaPolicy {
        match self.policies {
            Policies::Default => SchemaPolicy::Evolve,
            Policies::Drawn => {
                let index = PATHS
                    .iter()
                    .position(|name| ColumnPath::from(*name) == *path);
                index.map_or(SchemaPolicy::Evolve, |index| {
                    self.stream.columns[index].policy
                })
            }
        }
    }

    fn is_key(&self, path: &ColumnPath) -> bool {
        self.stream.key.contains(path)
    }

    /// Whether `path` follows `policy` and may drop its values by it: a key's values never drop.
    fn drops(&self, path: &ColumnPath, policy: SchemaPolicy) -> bool {
        !self.is_key(path) && self.policy(path) == policy
    }
}

/// One incoming column's values, as a route must keep them.
struct Values<'a> {
    path: &'a ColumnPath,
    logical: &'a LogicalType,
    /// Whether they hold an integer a 64-bit float would round.
    rounding: bool,
}

impl Values<'_> {
    /// Whether a column of `column` holds them as they are.
    fn fit(&self, column: &LogicalType) -> bool {
        column.join(self.logical) == *column
    }

    /// Whether a column of `column` holds them cast with nothing rounded: 64-bit integers, none
    /// beyond what a 64-bit float holds exactly, as floats.
    fn cast_exactly(&self, column: &LogicalType) -> bool {
        *column == LogicalType::Float64 && *self.logical == LogicalType::Int64 && !self.rounding
    }
}

/// A column's type: any scalar, `Null` and `Json` among them, or twice as often a number, whose
/// joins and casts have the most rules.
fn column_type() -> impl Strategy<Value = LogicalType> {
    prop_oneof![1 => scalar_type(), 2 => number_type()]
}

/// A batch of one to [`BATCH_COLUMNS`] distinct columns of the first `paths` of [`PATHS`].
fn batch(paths: usize) -> impl Strategy<Value = Batch> {
    let names: Vec<&'static str> = PATHS[..paths].to_vec();
    let creation = prop_oneof![Just(Creation::AsModeled), Just(Creation::ByFlush)];
    (
        proptest::sample::subsequence(names, 1..=BATCH_COLUMNS).prop_shuffle(),
        creation,
    )
        .prop_flat_map(|(names, creation)| {
            let count = names.len();
            let typed = proptest::collection::vec((column_type(), any::<bool>()), count);
            let order = Just((0..count).collect::<Vec<usize>>()).prop_shuffle();
            (Just(names), typed, Just(creation), order)
        })
        .prop_map(|(names, typed, creation, order)| Batch {
            columns: names
                .into_iter()
                .zip(typed)
                .map(|(name, (logical, rounding))| Column {
                    path: ColumnPath::from(name),
                    logical,
                    rounding,
                })
                .collect(),
            creation,
            order,
        })
}

/// A destination storing a subset of the scalar kinds, widening a subset of the lattice's
/// widenings in place, and mostly adding columns.
fn destination() -> impl Strategy<Value = Capabilities> {
    let mut kinds: Vec<TypeKind> = Capabilities::minimal().types.into_iter().collect();
    kinds.extend([TypeKind::Uuid, TypeKind::Json]);
    let widenings: Vec<(TypeKind, TypeKind)> = SchemaChanges::all().widenings.into_iter().collect();
    let (all_kinds, all_widenings) = (kinds.len(), widenings.len());
    (
        proptest::sample::subsequence(kinds, 0..=all_kinds),
        proptest::sample::subsequence(widenings, 0..=all_widenings),
        proptest::bool::weighted(0.8),
    )
        .prop_map(|(types, widenings, add_column)| {
            let mut capabilities = Capabilities::minimal();
            capabilities.types = types.into_iter().collect();
            capabilities.schema_changes = SchemaChanges {
                add_column,
                widenings: widenings.into_iter().collect(),
            };
            capabilities
        })
}

/// A stream's settings: each column's policy, what a change the destination cannot apply does,
/// and a hint, mostly unset; a merge key of at most one column; normalized or not; and the
/// columns the destination refused to widen.
fn stream_settings() -> impl Strategy<Value = StreamSettings> {
    use SchemaPolicy as P;
    let policy = prop_oneof![
        3 => Just(P::Evolve),
        1 => Just(P::Freeze),
        1 => Just(P::DiscardRow),
        1 => Just(P::DiscardValue),
    ];
    let on_unsupported = prop_oneof![
        3 => Just(OnUnsupported::VariantColumn),
        1 => Just(OnUnsupported::Refuse),
    ];
    let hint = proptest::option::weighted(
        0.2,
        scalar_type().prop_filter("a plan refuses a null hint", |logical| {
            *logical != LogicalType::Null
        }),
    );
    let column =
        (policy, on_unsupported, hint).prop_map(|(policy, on_unsupported, hint)| ColumnSettings {
            policy,
            on_unsupported,
            hint,
        });
    let path = (0..PATHS.len()).prop_map(|index| ColumnPath::from(PATHS[index]));
    let unwidened = prop_oneof![
        path.clone().prop_map(ColumnKey::Source),
        (path, scalar_type()).prop_map(|(column, logical)| ColumnKey::Variant {
            column,
            kind: logical.kind(),
        }),
    ];
    let lineage = prop_oneof![Just(LineageColumns::None), Just(LineageColumns::Root)];
    (
        proptest::collection::vec(column, PATHS.len()),
        proptest::sample::subsequence(PATHS.to_vec(), 0..=1),
        lineage,
        proptest::collection::btree_set(unwidened, 0..=3),
    )
        .prop_map(|(columns, key, lineage, unwidened)| StreamSettings {
            columns,
            key: key.into_iter().map(ColumnPath::from).collect(),
            lineage,
            unwidened,
        })
}

fn case() -> impl Strategy<Value = Case> {
    (
        destination(),
        stream_settings(),
        proptest::collection::vec(batch(HISTORY_PATHS), 2..=4),
        batch(PATHS.len()),
    )
        .prop_map(|(capabilities, stream, history, batch)| Case {
            capabilities,
            stream,
            history,
            batch,
        })
}

/// The resolver of `case`'s stream following `policies`: a table's history follows the default
/// policies with nothing refused a widen, as a table evolves before its settings change.
fn case_resolver(case: &Case, policies: Policies) -> Resolver {
    let stream = &case.stream;
    let mut plan = StreamPlan::new(StreamName::new("s").unwrap());
    for (name, settings) in PATHS.iter().zip(&stream.columns) {
        if policies == Policies::Drawn {
            let set = SchemaSettings::new()
                .policy(settings.policy)
                .on_unsupported(settings.on_unsupported);
            plan = plan.column(*name, set);
        }
        if let Some(hint) = &settings.hint {
            plan = plan.hint(*name, hint.clone());
        }
    }
    let naming = Naming::new(case.capabilities.identifiers.clone());
    Resolver {
        stream: StreamName::new("s").unwrap(),
        settings: Settings {
            pipeline: SchemaSettings::default(),
            stream: plan,
            key: stream.key.clone(),
            owner: None,
        },
        meta: MetaNames::assign(&naming, !stream.key.is_empty(), stream.lineage),
        naming,
        capabilities: Arc::new(case.capabilities.clone()),
        root: None,
        columns: u64::MAX,
        unwidened: match policies {
            Policies::Drawn => stream.unwidened.clone(),
            Policies::Default => BTreeSet::new(),
        },
    }
}

/// `batch`'s columns in the order `order` gives, as positions into them.
fn incoming(batch: &Batch, order: &[usize]) -> Incoming {
    let columns: Vec<&Column> = order.iter().map(|index| &batch.columns[*index]).collect();
    let fields = columns
        .iter()
        .map(|column| Field::new(column.path.to_string(), column.logical.clone(), true))
        .collect();
    Incoming {
        schema: Arc::new(TableSchema::new(fields).unwrap()),
        paths: columns.iter().map(|column| column.path.clone()).collect(),
        rounding: columns
            .iter()
            .filter(|column| column.rounding)
            .map(|column| column.path.clone())
            .collect(),
        creation: batch.creation,
    }
}

/// `resolver`'s resolution of `incoming` against `model`, or `None` where it refuses the batch.
fn resolved(
    resolver: &Resolver,
    model: &Model,
    incoming: &Incoming,
) -> Result<Option<Resolution>, TestCaseError> {
    match resolver.resolve(model, incoming) {
        Ok(resolution) => Ok(Some(resolution)),
        Err(error) if error.kind() == ErrorKind::Schema => Ok(None),
        Err(error) => Err(TestCaseError::fail(format!(
            "resolving failed other than by refusing: {error:?}"
        ))),
    }
}

/// Checks what every resolution keeps on `batch` resolved against `model`, and returns the
/// resolved model, or `None` where the batch is refused.
fn check(
    resolver: &Resolver,
    expected: &Expected<'_>,
    model: &Model,
    batch: &Batch,
) -> Result<Option<Model>, TestCaseError> {
    let arrival: Vec<usize> = (0..batch.columns.len()).collect();
    let incoming = incoming(batch, &arrival);
    let resolution = resolved(resolver, model, &incoming)?;
    order_free(resolver, model, batch, resolution.as_ref())?;
    let Some(resolution) = resolution else {
        return Ok(None);
    };
    loses_no_value(expected, &incoming, &resolution)?;
    only_grows(expected, model, &incoming, &resolution)?;
    idempotent(resolver, &incoming, &resolution)?;
    Ok(Some(resolution.model))
}

/// The type of the resolved model's column at `column`, and the source column it holds.
fn column_at(model: &Model, column: usize) -> Result<(&LogicalType, &ColumnKey), TestCaseError> {
    let field = model.columns.get(column).ok_or_else(|| {
        TestCaseError::fail(format!("a route names column {column}, past the model"))
    })?;
    let key = model.names.owner(field.name()).ok_or_else(|| {
        TestCaseError::fail(format!("column {} holds no source column", field.name()))
    })?;
    Ok((field.logical_type(), key))
}

/// Checks that `resolution` routes each column of `incoming` where it loses no value.
fn loses_no_value(
    expected: &Expected<'_>,
    incoming: &Incoming,
    resolution: &Resolution,
) -> Result<(), TestCaseError> {
    let fields = incoming.schema.fields();
    prop_assert_eq!(
        resolution.routes.len(),
        fields.len(),
        "a column has no route"
    );
    for ((field, path), route) in fields
        .iter()
        .zip(incoming.paths.iter())
        .zip(&resolution.routes)
    {
        let values = Values {
            path,
            logical: field.logical_type(),
            rounding: incoming.rounding.contains(path),
        };
        keeps_values(expected, &resolution.model, &values, *route)?;
    }
    Ok(())
}

/// Checks that `route` keeps `values`, sending them to a column of their own, or a variant of
/// it, that holds them as they are or casts them exactly, skipping a column of only nulls the
/// table lacks, splitting JSON text as [`splits_whole`] checks, or dropping them by their policy.
///
/// A merge key's values land only in its own column.
fn keeps_values(
    expected: &Expected<'_>,
    model: &Model,
    values: &Values<'_>,
    route: Route,
) -> Result<(), TestCaseError> {
    let (path, logical) = (values.path, values.logical);
    let own = ColumnKey::Source(path.clone());
    match route {
        Route::Column(column) => {
            let (stored, key) = column_at(model, column)?;
            prop_assert_eq!(key.column(), path, "{} goes to another's column", path);
            prop_assert!(
                values.fit(stored) || values.cast_exactly(stored),
                "{path} of {logical} goes to a column of {stored}, which does not hold it"
            );
            prop_assert!(
                !expected.is_key(path) || *key == own,
                "the key {path} goes to its variant {key:?}"
            );
        }
        Route::Skip => {
            prop_assert_eq!(logical, &LogicalType::Null, "{} is skipped", path);
            let owned = model
                .names
                .get(&own)
                .is_some_and(|name| model.columns.iter().any(|field| field.name() == name));
            prop_assert!(!owned, "{path} is skipped though the table has its column");
        }
        Route::DiscardRows => {
            let by_policy = expected.drops(path, SchemaPolicy::DiscardRow);
            prop_assert!(by_policy, "{path}'s rows drop by no policy");
        }
        Route::DiscardValues => {
            let by_policy = expected.drops(path, SchemaPolicy::DiscardValue);
            prop_assert!(by_policy, "{path}'s values drop by no policy");
        }
        Route::Split { own: column, rest } => splits_whole(expected, model, values, column, rest)?,
    }
    Ok(())
}

/// Checks that a split of `values` is of JSON text over their own column, at `own`, of another
/// type, the rest going to a JSON variant of it or dropped by their policy.
fn splits_whole(
    expected: &Expected<'_>,
    model: &Model,
    values: &Values<'_>,
    own: usize,
    rest: Rest,
) -> Result<(), TestCaseError> {
    let path = values.path;
    prop_assert_eq!(values.logical, &LogicalType::Json, "{} splits", path);
    prop_assert!(!expected.is_key(path), "the key {path} splits");
    let (stored, key) = column_at(model, own)?;
    prop_assert_eq!(
        key,
        &ColumnKey::Source(path.clone()),
        "{} splits over another column",
        path
    );
    prop_assert_ne!(
        stored,
        &LogicalType::Json,
        "{} splits over a JSON column",
        path
    );
    match rest {
        Rest::Column(variant) => {
            let (stored, key) = column_at(model, variant)?;
            let of_own = matches!(key, ColumnKey::Variant { column, .. } if column == path);
            prop_assert!(of_own, "{path}'s rest goes to {key:?}, no variant of it");
            prop_assert!(
                values.fit(stored),
                "{path}'s rest goes to a column of {stored}"
            );
        }
        Rest::DiscardRows => {
            let by_policy = expected.drops(path, SchemaPolicy::DiscardRow);
            prop_assert!(by_policy, "{path}'s rest drops its rows by no policy");
        }
        Rest::DiscardValues => {
            let by_policy = expected.drops(path, SchemaPolicy::DiscardValue);
            prop_assert!(by_policy, "{path}'s rest drops its values by no policy");
        }
    }
    Ok(())
}

/// Checks that `resolution` only grows the table: existing columns keep their order,
/// identifiers, source columns and nullability, and each type only widens; names only extend;
/// the changes are exactly the columns added and widened; and the version advances by one
/// exactly when there are changes, or from 0 to 1 when a normalized stream's table is created.
fn only_grows(
    expected: &Expected<'_>,
    model: &Model,
    incoming: &Incoming,
    resolution: &Resolution,
) -> Result<(), TestCaseError> {
    let resolved = &resolution.model;
    let widened = widened(model, resolved)?;
    for (key, name) in model.names.iter() {
        prop_assert_eq!(resolved.names.get(key), Some(name), "{:?} was renamed", key);
    }
    prop_assert!(
        TableSchema::new(resolved.columns.clone()).is_ok(),
        "two columns share an identifier"
    );
    let mut added = Vec::new();
    for column in model.columns.len()..resolved.columns.len() {
        let (_, key) = column_at(resolved, column)?;
        prop_assert!(
            incoming.paths.contains(key.column()),
            "added column {column} holds none of the batch's columns"
        );
        added.push(key.clone());
    }
    let mut adds = Vec::new();
    let mut widens = BTreeMap::new();
    for change in &resolution.changes {
        match change {
            Change::Add { key } => adds.push(key.clone()),
            Change::Widen { column, from } => {
                let again = widens.insert(*column, from.clone());
                prop_assert!(again.is_none(), "column {column} widens twice");
            }
        }
    }
    prop_assert_eq!(
        adds,
        added,
        "the changes add other columns than the table gained"
    );
    prop_assert_eq!(
        widens,
        widened,
        "the changes widen other columns than the table widened"
    );
    let created = expected.stream.lineage == LineageColumns::Root && model.version == 0;
    let version = if resolution.changes.is_empty() && !created {
        model.version
    } else {
        model.version + 1
    };
    prop_assert_eq!(
        resolved.version,
        version,
        "the version went from {} with {} changes",
        model.version,
        resolution.changes.len()
    );
    Ok(())
}

/// The columns of `model` whose type `resolved` widened, each with the type it had, once each of
/// them is checked to keep its position, identifier, source column and nullability, and to have
/// only widened.
fn widened(model: &Model, resolved: &Model) -> Result<BTreeMap<usize, LogicalType>, TestCaseError> {
    prop_assert!(
        resolved.columns.len() >= model.columns.len(),
        "the table lost columns"
    );
    let mut widened = BTreeMap::new();
    for (index, (before, after)) in model.columns.iter().zip(&resolved.columns).enumerate() {
        prop_assert_eq!(before.name(), after.name(), "column {} moved", index);
        prop_assert_eq!(before.is_nullable(), after.is_nullable());
        prop_assert_eq!(
            model.names.owner(before.name()),
            resolved.names.owner(after.name()),
            "column {} holds another source column",
            before.name()
        );
        let (from, to) = (before.logical_type(), after.logical_type());
        prop_assert_eq!(&from.join(to), to, "column {} narrowed", before.name());
        if from != to {
            widened.insert(index, from.clone());
        }
    }
    Ok(widened)
}

/// Checks that resolving the batch again, against the model `resolution` made, changes
/// nothing and routes it alike.
fn idempotent(
    resolver: &Resolver,
    incoming: &Incoming,
    resolution: &Resolution,
) -> Result<(), TestCaseError> {
    let again = resolved(resolver, &resolution.model, incoming)?;
    let Some(again) = again else {
        return Err(TestCaseError::fail(
            "the batch is refused by the table it made",
        ));
    };
    prop_assert!(
        again.changes.is_empty(),
        "the batch changes the table it made"
    );
    prop_assert_eq!(&again.model, &resolution.model);
    prop_assert_eq!(&again.routes, &resolution.routes);
    Ok(())
}

/// Checks that `batch` in its other order, against `model`, is refused alike or resolved as
/// `resolution` resolves it in its own: the same version, identifiers and types, and each column
/// routed to the same identifiers.
fn order_free(
    resolver: &Resolver,
    model: &Model,
    batch: &Batch,
    resolution: Option<&Resolution>,
) -> Result<(), TestCaseError> {
    let reordered = resolved(resolver, model, &incoming(batch, &batch.order))?;
    let (resolution, reordered) = match (resolution, reordered) {
        (None, None) => return Ok(()),
        (Some(resolution), Some(reordered)) => (resolution, reordered),
        (resolution, _) => {
            let refused = if resolution.is_some() {
                "the other"
            } else {
                "its own"
            };
            return Err(TestCaseError::fail(format!(
                "the batch is refused in {refused} order alone"
            )));
        }
    };
    let (ours, theirs) = (&resolution.model, &reordered.model);
    prop_assert_eq!(
        ours.version,
        theirs.version,
        "the version depends on the columns' order"
    );
    prop_assert_eq!(
        &ours.names,
        &theirs.names,
        "the names depend on the columns' order"
    );
    prop_assert_eq!(
        typed(ours),
        typed(theirs),
        "the types depend on the columns' order"
    );
    let existing = model.columns.len();
    prop_assert_eq!(
        &ours.columns[..existing],
        &theirs.columns[..existing],
        "existing columns moved with the columns' order"
    );
    let positions = ours.positions();
    prop_assert_eq!(
        reordered.routes.len(),
        batch.order.len(),
        "a column has no route"
    );
    for (position, index) in batch.order.iter().enumerate() {
        let route = translated(reordered.routes[position], theirs, &positions);
        prop_assert_eq!(
            route,
            Some(resolution.routes[*index]),
            "column {} routes apart",
            index
        );
    }
    Ok(())
}

/// Each of `model`'s columns by identifier, with its type and nullability.
fn typed(model: &Model) -> BTreeMap<&str, (&LogicalType, bool)> {
    model
        .columns
        .iter()
        .map(|field| (field.name(), (field.logical_type(), field.is_nullable())))
        .collect()
}

/// `route`, into the columns of `from`, as the route into the columns at `positions`, by
/// identifier.
fn translated(route: Route, from: &Model, positions: &BTreeMap<Arc<str>, usize>) -> Option<Route> {
    let at = |column: usize| positions.get(from.columns.get(column)?.name()).copied();
    Some(match route {
        Route::Column(column) => Route::Column(at(column)?),
        Route::Split { own, rest } => Route::Split {
            own: at(own)?,
            rest: match rest {
                Rest::Column(variant) => Rest::Column(at(variant)?),
                other => other,
            },
        },
        other => other,
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(256)))]

    #[test]
    fn resolutions_lose_no_value_only_grow_and_are_idempotent_and_order_free(case in case()) {
        let history = case_resolver(&case, Policies::Default);
        let expected = Expected { stream: &case.stream, policies: Policies::Default };
        let mut model = Model::default();
        for batch in &case.history {
            if let Some(resolved) = check(&history, &expected, &model, batch)? {
                model = resolved;
            }
        }
        let drawn = case_resolver(&case, Policies::Drawn);
        let expected = Expected { stream: &case.stream, policies: Policies::Drawn };
        check(&drawn, &expected, &model, &case.batch)?;
    }
}

#[test]
fn two_batches_resolved_in_either_order_may_give_different_layouts() {
    let resolver = resolver(capabilities(), plan(), &[]);
    let integers = [("x", LogicalType::Int64)];
    let text = [("x", LogicalType::Utf8)];
    let layout = |first: &[(&str, LogicalType)], then: &[(&str, LogicalType)]| {
        let model = created(&resolver, first);
        columns(&resolver.resolve(&model, &schema(then)).unwrap().model)
    };
    assert_ne!(layout(&integers, &text), layout(&text, &integers));
}

#[test]
fn columns_asking_for_one_identifier_are_named_alike_in_either_order() {
    let resolver = resolver(capabilities(), plan(), &[]);
    let model = created(&resolver, &[("a", LogicalType::Int64)]);
    // Text at `a` takes its JSON variant, whose identifier the new column `a__json` asks for too.
    let named = |batch: &[(&str, LogicalType)]| resolver.resolve(&model, &schema(batch)).unwrap();
    let first = named(&[("a__json", LogicalType::Utf8), ("a", LogicalType::Utf8)]);
    let second = named(&[("a", LogicalType::Utf8), ("a__json", LogicalType::Utf8)]);
    let variant = ColumnKey::Variant {
        column: ColumnPath::from("a"),
        kind: TypeKind::Json,
    };
    assert!(
        first.model.names.get(&variant).is_some(),
        "a variant was added"
    );
    assert_eq!(first.model.names, second.model.names);
}
