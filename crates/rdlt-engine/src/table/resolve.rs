//! Schema resolution (spec §8.4): how a batch's columns fit its table, and the changes the table
//! needs first.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::RecordBatch;

use rdlt_connector::{
    Capabilities, ColumnKey, ColumnPath, LogicalType, RootKey, StreamName, TableSchema, TypeKind,
};

mod draft;
mod width;

use super::lower::{LineageColumns, MetaNames, lower};
use super::model::Model;
use crate::error::Error;
use crate::naming::Naming;
use crate::plan::StreamPlan;
use crate::policy::{self, Nested, OnUnsupported, Resolved, SchemaPolicy, SchemaSettings};
use draft::Draft;

/// Where one incoming column's values go.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Route {
    /// Into the model's column at this position.
    Column(usize),
    /// Nowhere, and every row holding a value in the column is dropped.
    DiscardRows,
    /// Nowhere: the rows load without the column's values.
    DiscardValues,
    /// Nowhere: the column holds only nulls and the table has no column for it.
    Skip,
    /// A column of JSON whose values the model's column at `own`, of another type, holds go
    /// there, read into its type; the others go where `rest` says.
    Split { own: usize, rest: Rest },
}

/// Where the values of a split column its own column does not hold go.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Rest {
    /// Into the model's column at this position, a variant.
    Column(usize),
    /// Nowhere, and every row holding one is dropped.
    DiscardRows,
    /// Nowhere: those rows load without them.
    DiscardValues,
}

impl Route {
    /// Whether rows holding a value the route sends nowhere are dropped.
    pub(crate) fn drops_rows(self) -> bool {
        matches!(
            self,
            Self::DiscardRows
                | Self::Split {
                    rest: Rest::DiscardRows,
                    ..
                }
        )
    }
}

/// A change to a table's model.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Change {
    /// The column for `key`, appended to the model.
    Add { key: ColumnKey },
    /// The column at `column` widens from `from`.
    Widen { column: usize, from: LogicalType },
}

/// How a batch fits its table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Resolution {
    /// The changes the table needs, in order; none when the batch fits as it is.
    pub(crate) changes: Vec<Change>,
    /// Where each incoming column goes, in the batch's column order.
    pub(crate) routes: Vec<Route>,
    /// The model once the changes apply.
    pub(crate) model: Model,
}

/// A stream's schema settings, as resolution reads them.
#[derive(Clone, Debug)]
pub(crate) struct Settings {
    pub(crate) pipeline: SchemaSettings,
    pub(crate) stream: StreamPlan,
    /// The merge key's columns; empty for streams that do not merge.
    pub(crate) key: Vec<ColumnPath>,
    /// For a child table, the stream's column whose arrays it holds, whose settings every column
    /// of the table takes.
    pub(crate) owner: Option<ColumnPath>,
}

impl Settings {
    /// The settings that apply to `column`: those of the stream's column it is, or was flattened
    /// from, or, in a child table, those of the column whose arrays the table holds.
    pub(crate) fn column(&self, column: &ColumnPath) -> Resolved {
        let top = column.segments().next().map(ColumnPath::from);
        let owner = self.owner.as_ref().or(top.as_ref()).unwrap_or(column);
        policy::resolve([
            self.stream.column_settings(owner),
            None,
            Some(self.stream.schema_settings()),
            Some(&self.pipeline),
        ])
    }

    /// How the column holding `key` stores nested values.
    pub(crate) fn nested(&self, key: &ColumnKey) -> Nested {
        self.column(key.column()).nested
    }
}

/// Resolves batches of one stream's table.
#[derive(Clone, Debug)]
pub(crate) struct Resolver {
    pub(crate) stream: StreamName,
    pub(crate) settings: Settings,
    pub(crate) capabilities: Arc<Capabilities>,
    pub(crate) naming: Naming,
    pub(crate) meta: MetaNames,
    /// For a child table of a merge stream, the stream's table, whose merges replace its rows.
    pub(crate) root: Option<RootKey>,
    /// Columns: the most the table may hold, its nested fields counted.
    pub(crate) columns: u64,
    /// The columns whose widening in place the destination refused: a value they cannot hold
    /// goes to a variant column instead.
    pub(crate) unwidened: BTreeSet<ColumnKey>,
}

/// A batch's columns as they arrive: their types, and each column's path within its table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Incoming {
    pub(crate) schema: TableSchema,
    /// Each column's path, in the schema's order.
    pub(crate) paths: Vec<ColumnPath>,
    /// The columns of 64-bit integers holding a value a 64-bit float would round.
    pub(crate) rounding: BTreeSet<ColumnPath>,
}

impl Incoming {
    /// The columns of a declared schema, top-level ones named as it names them, which hold no
    /// values: a batch with values is read with [`Incoming::of`], which judges them.
    pub(crate) fn declared(schema: TableSchema) -> Self {
        let paths = schema
            .fields()
            .iter()
            .map(|field| ColumnPath::from(field.name()))
            .collect();
        Self {
            schema,
            paths,
            rounding: BTreeSet::new(),
        }
    }

    /// The columns of `batches`, which are `schema`'s at `paths`, in order: those of 64-bit
    /// integers noted where a value a 64-bit float would round.
    pub(crate) fn of(schema: TableSchema, paths: Vec<ColumnPath>, batches: &[RecordBatch]) -> Self {
        let rounding = super::exact::rounding(&schema, &paths, batches);
        Self {
            schema,
            paths,
            rounding,
        }
    }

    /// The same columns, those at `rounding` holding a value a 64-bit float would round.
    #[cfg(test)]
    pub(crate) fn rounding(self, rounding: BTreeSet<ColumnPath>) -> Self {
        Self { rounding, ..self }
    }
}

/// One incoming column, with what applies to it.
struct Arriving<'a> {
    path: ColumnPath,
    logical: &'a LogicalType,
    settings: Resolved,
    is_key: bool,
    hinted: bool,
    /// Whether it holds an integer a 64-bit float would round.
    rounding: bool,
}

impl Resolver {
    /// The resolver of a child table of the same stream holding the arrays of the stream's column
    /// `owner`: with the settings of `owner` for every column, no hints or merge key, a child's
    /// lineage columns, and none of the root's columns kept from widening; a merge stream's child
    /// table follows `root`, its root table, with a sequence column.
    pub(crate) fn child(&self, root: Option<RootKey>, owner: ColumnPath) -> Result<Self, Error> {
        let settings = Settings {
            stream: self.settings.stream.without_hints(),
            key: Vec::new(),
            owner: Some(owner),
            ..self.settings.clone()
        };
        let merge = root.is_some();
        Ok(Self {
            settings,
            meta: MetaNames::assign(&self.naming, merge, LineageColumns::Child)?,
            root,
            unwidened: BTreeSet::new(),
            ..self.clone()
        })
    }

    /// The same resolver, widening none of `columns` in place.
    pub(crate) fn unwidening(&self, columns: impl IntoIterator<Item = ColumnKey>) -> Self {
        let mut unwidened = self.unwidened.clone();
        unwidened.extend(columns);
        Self {
            unwidened,
            ..self.clone()
        }
    }

    /// The same resolver, appending a hash seeded with `salt` to every identifier it assigns.
    pub(crate) fn hashing(&self, salt: u64) -> Self {
        Self {
            naming: self.naming.hashing(salt),
            ..self.clone()
        }
    }

    /// How `incoming` fits `model`: the changes the table needs and where each column goes.
    ///
    /// A table not yet created takes every column of its first batch. After that, a column the
    /// table lacks or a value its column cannot hold is a change, which the column's policy
    /// applies, refuses or discards. The identifiers of added columns are assigned together, so
    /// they do not depend on the batch's column order.
    pub(crate) fn resolve(&self, model: &Model, incoming: &Incoming) -> Result<Resolution, Error> {
        let mut draft = Draft::new(model);
        let fields = incoming.schema.fields();
        let mut routes = Vec::with_capacity(fields.len());
        for (field, path) in fields.iter().zip(&incoming.paths) {
            let path = path.clone();
            let column = Arriving {
                settings: self.settings.column(&path),
                is_key: self.settings.key.contains(&path),
                hinted: self.settings.stream.hinted(&path).is_some(),
                logical: field.logical_type(),
                rounding: incoming.rounding.contains(&path),
                path,
            };
            routes.push(self.route(&mut draft, &column, model.created())?);
        }
        let stream = |error: Error| error.with_stream(&self.stream);
        let mut resolution = draft
            .finish(routes, &self.naming, &self.meta.all())
            .map_err(stream)?;
        // A normalized stream's table holds its rows' lineage even where they hold no other value,
        // so its first batch creates it.
        if self.meta.id.is_some() && !resolution.model.created() {
            resolution.model.version = 1;
            resolution.model.revision =
                draft::advanced(resolution.model.revision).map_err(stream)?;
        }
        if resolution.model.version != model.version {
            width::admit(&resolution.model, self.columns).map_err(stream)?;
        }
        Ok(resolution)
    }

    /// Where `column` goes, recording the changes it needs.
    fn route(
        &self,
        draft: &mut Draft,
        column: &Arriving<'_>,
        created: bool,
    ) -> Result<Route, Error> {
        let key = ColumnKey::Source(column.path.clone());
        let original = draft.find(&key);
        if *column.logical == LogicalType::Null {
            return Ok(original.map_or(Route::Skip, Route::Column));
        }
        let original = if let Some(original) = original {
            original
        } else {
            if let Some(route) = self.admit(column, created)? {
                return Ok(route);
            }
            let hint = self.settings.stream.hinted(&column.path);
            let logical = hint.unwrap_or(column.logical).clone();
            self.identifies(column, &logical)?;
            draft.add(key, logical, !column.is_key, !column.rounding)
        };
        self.place(draft, column, original)
    }

    /// Refuses a merge key column of `logical`, JSON, for a normalized stream's table: its rows
    /// merge by the key as stored, where JSON's `1` and `1.0` differ, while their child rows
    /// follow the root id the key's values give, where they are one value.
    fn identifies(&self, column: &Arriving<'_>, logical: &LogicalType) -> Result<(), Error> {
        if column.is_key && self.meta.id.is_some() && *logical == LogicalType::Json {
            let detail = "a merge key stored as JSON cannot identify a normalized stream's rows";
            return Err(self.refused(column, "merge_key_json", detail));
        }
        Ok(())
    }

    /// Where a column the table lacks goes instead of a new column of its own, if anywhere.
    ///
    /// A table being created takes every column; an existing one follows the column's policy.
    fn admit(&self, column: &Arriving<'_>, created: bool) -> Result<Option<Route>, Error> {
        if !created {
            return Ok(None);
        }
        if !column.is_key {
            match column.settings.policy {
                SchemaPolicy::Freeze => {
                    return Err(self.refused(column, "schema_frozen", "a new column appeared"));
                }
                SchemaPolicy::DiscardRow => return Ok(Some(Route::DiscardRows)),
                SchemaPolicy::DiscardValue => return Ok(Some(Route::DiscardValues)),
                SchemaPolicy::Evolve => {}
            }
        }
        if !self.capabilities.schema_changes.add_column {
            let detail = "the destination cannot add columns";
            return Err(self.refused(column, "schema_change_unsupported", detail));
        }
        Ok(None)
    }

    /// Where values of `column` go once it has its `original` column.
    fn place(
        &self,
        draft: &mut Draft,
        column: &Arriving<'_>,
        original: usize,
    ) -> Result<Route, Error> {
        // The column's own comes first, holding the values as they are or cast, as floats hold
        // integers every one of which they hold exactly; then a variant holding them as they are.
        let own = draft.column_type(original);
        let fitting = if fits(&own, column.logical) || holds(&own, column) {
            Some(original)
        } else {
            draft
                .variants(&column.path)
                .into_iter()
                .find(|candidate| fits(&draft.column_type(*candidate), column.logical))
        };
        let split = splits(draft, column, original);
        let rest = |rest: Rest| routed(split, rest);
        if let Some(fitting) = fitting {
            if column.rounding {
                draft.round(fitting);
            }
            if fitting == original {
                return Ok(Route::Column(original));
            }
            return Ok(rest(Rest::Column(fitting)));
        }
        let current = draft.column_type(original);
        let joined = draft.join(original, column);
        let widens = self.widens_in_place(column, &current, &joined);
        let cannot = |what: &str| format!("the column is {current} and {what} {}", column.logical);
        if column.is_key {
            // A frozen schema changes for no column, its key's included.
            if column.settings.policy == SchemaPolicy::Freeze {
                return Err(self.refused(column, "schema_frozen", &cannot("cannot hold")));
            }
            // A key keeps matching its stored rows only where its values are stored alike: a
            // decimal stored as text renders 1.50 as 1.5000 once its scale grows.
            if widens && self.keeps_matching(&current, &joined, column.settings.nested) {
                draft.widen(original, joined, !column.rounding);
                return Ok(Route::Column(original));
            }
            return Err(self.refused(column, "merge_key_changed", &cannot("the key cannot hold")));
        }
        match column.settings.policy {
            SchemaPolicy::Freeze => {
                return Err(self.refused(column, "schema_frozen", &cannot("cannot hold")));
            }
            SchemaPolicy::DiscardRow => return Ok(rest(Rest::DiscardRows)),
            SchemaPolicy::DiscardValue => return Ok(rest(Rest::DiscardValues)),
            SchemaPolicy::Evolve => {}
        }
        if widens {
            draft.widen(original, joined, !column.rounding);
            return Ok(Route::Column(original));
        }
        if column.settings.on_unsupported == OnUnsupported::Refuse
            || !self.capabilities.schema_changes.add_column
        {
            let detail =
                cannot("the destination cannot change it, without a variant column, to hold");
            return Err(self.refused(column, "schema_change_unsupported", &detail));
        }
        Ok(rest(Rest::Column(self.variant(draft, column, &joined))))
    }

    /// Whether `column`'s values widen its column of `current` in place to `joined`.
    ///
    /// Only the lattice's joins do: a column of integers joined to floats by its values takes a
    /// variant, since a partition's plan made before may still write it integers.
    fn widens_in_place(
        &self,
        column: &Arriving<'_>,
        current: &LogicalType,
        joined: &LogicalType,
    ) -> bool {
        !column.hinted
            && *joined == current.join(column.logical)
            && *joined != LogicalType::Json
            && !self
                .unwidened
                .contains(&ColumnKey::Source(column.path.clone()))
            && self.widens(current, joined, column.settings.nested)
    }

    /// The variant column that takes `column`'s values: the variant of the joined type's kind,
    /// widened or added, or else the `Json` variant, which holds anything.
    fn variant(&self, draft: &mut Draft, column: &Arriving<'_>, joined: &LogicalType) -> usize {
        let key = |kind| ColumnKey::Variant {
            column: column.path.clone(),
            kind,
        };
        let kind = joined.kind();
        let Some(existing) = draft.find(&key(kind)) else {
            return draft.add(key(kind), joined.clone(), true, !column.rounding);
        };
        let current = draft.column_type(existing);
        let wider = current.join(column.logical);
        if wider != LogicalType::Json
            && wider.kind() == kind
            && !self.unwidened.contains(&key(kind))
            && self.widens(&current, &wider, column.settings.nested)
        {
            // A variant's kind is its type's, so it never widens to 64-bit integers.
            draft.widen(existing, wider, false);
            return existing;
        }
        let json = key(TypeKind::Json);
        draft
            .find(&json)
            .unwrap_or_else(|| draft.add(json, LogicalType::Json, true, false))
    }

    /// Whether the destination can change a column of `from` to `to` in place: they are stored
    /// alike, or it declares the widening.
    fn widens(&self, from: &LogicalType, to: &LogicalType, nested: Nested) -> bool {
        let from = lower(from, nested, &self.capabilities);
        let to = lower(to, nested, &self.capabilities);
        from == to
            || self
                .capabilities
                .schema_changes
                .widens(from.kind(), to.kind())
    }

    /// Whether a key column of `current`, widened to `joined`, keeps matching the rows it stored:
    /// the destination stores both types by value, or renders both into one type alike, as whole
    /// numbers or decimals of one scale render.
    fn keeps_matching(&self, current: &LogicalType, joined: &LogicalType, nested: Nested) -> bool {
        let by_value = self.by_value(current, nested) && self.by_value(joined, nested);
        let rendered = lower(current, nested, &self.capabilities)
            == lower(joined, nested, &self.capabilities)
            && scale(current).is_some()
            && scale(current) == scale(joined);
        by_value || rendered
    }

    /// Whether the destination stores values of `logical` by value, as themselves or as integers
    /// of another width, rather than rendered into another type, whose rendering the type decides.
    fn by_value(&self, logical: &LogicalType, nested: Nested) -> bool {
        let stored = lower(logical, nested, &self.capabilities);
        family(stored.kind()) == family(logical.kind())
    }

    fn refused(&self, column: &Arriving<'_>, code: &str, detail: &str) -> Error {
        Error::schema(format!(
            "stream {}: column {}: {detail}",
            self.stream, column.path
        ))
        .with_code(code)
        .with_stream(&self.stream)
    }
}

/// Whether a column of `current` holds every value of `incoming` as it is.
fn fits(current: &LogicalType, incoming: &LogicalType) -> bool {
    current.join(incoming) == *current
}

/// Whether a column of `current`, 64-bit floats, holds `column`'s values cast: 64-bit integers
/// every one of which a float holds exactly.
fn holds(current: &LogicalType, column: &Arriving<'_>) -> bool {
    *current == LogicalType::Float64 && *column.logical == LogicalType::Int64 && !column.rounding
}

/// The digits after the point every value of `logical` renders with, for integers and decimals.
fn scale(logical: &LogicalType) -> Option<u8> {
    match logical {
        LogicalType::Int8 | LogicalType::Int16 | LogicalType::Int32 | LogicalType::Int64 => Some(0),
        LogicalType::Decimal(decimal) => Some(decimal.scale()),
        _ => None,
    }
}

/// The kind values of `kind` keep their identity among: integers of every width are one.
fn family(kind: TypeKind) -> TypeKind {
    match kind {
        TypeKind::Int8 | TypeKind::Int16 | TypeKind::Int32 => TypeKind::Int64,
        other => other,
    }
}

/// The own column of `column`, at `original`, where it is JSON text and its own column is of
/// another type: one value of another kind makes a flush's column JSON, and each value its own
/// column holds still goes there, only the others taking a variant or the policy.
fn splits(draft: &mut Draft, column: &Arriving<'_>, original: usize) -> Option<usize> {
    let own = draft.column_type(original);
    if *column.logical != LogicalType::Json || own == LogicalType::Json {
        return None;
    }
    // Its integers are read only once the plan lowers them, after the model records which
    // columns hold only integers a float holds exactly: a column of integers may hold any.
    if own == LogicalType::Int64 {
        draft.round(original);
    }
    Some(original)
}

/// The route of a column's values that `rest` takes, where none goes to its own column `split`
/// splits them with.
fn routed(split: Option<usize>, rest: Rest) -> Route {
    match (split, rest) {
        (Some(own), rest) => Route::Split { own, rest },
        (None, Rest::Column(column)) => Route::Column(column),
        (None, Rest::DiscardRows) => Route::DiscardRows,
        (None, Rest::DiscardValues) => Route::DiscardValues,
    }
}
