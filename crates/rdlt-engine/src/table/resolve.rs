//! Schema resolution (spec §8.4): how a batch's columns fit its table, and the changes the table
//! needs first.

use std::collections::BTreeSet;
use std::sync::Arc;

use rdlt_connector::{
    Capabilities, ColumnKey, ColumnPath, Field, LogicalType, RootKey, StreamName, TableSchema,
    TypeKind,
};

use super::lower::{LineageColumns, MetaNames, lower};
use super::model::Model;
use crate::error::Error;
use crate::naming::Naming;
use crate::plan::StreamPlan;
use crate::policy::{self, Nested, OnUnsupported, Resolved, SchemaPolicy, SchemaSettings};

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
}

/// A batch's columns as they arrive: their types, and each column's path within its table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Incoming {
    pub(crate) schema: TableSchema,
    /// Each column's path, in the schema's order.
    pub(crate) paths: Vec<ColumnPath>,
}

impl From<TableSchema> for Incoming {
    /// A batch whose columns are top-level ones named as the schema names them.
    fn from(schema: TableSchema) -> Self {
        let paths = schema
            .fields()
            .iter()
            .map(|field| ColumnPath::from(field.name()))
            .collect();
        Self { schema, paths }
    }
}

/// One incoming column, with what applies to it.
struct Arriving<'a> {
    path: ColumnPath,
    logical: &'a LogicalType,
    settings: Resolved,
    is_key: bool,
    hinted: bool,
}

impl Resolver {
    /// The resolver of a child table of the same stream holding the arrays of the stream's column
    /// `owner`: with the settings of `owner` for every column, no hints or merge key, and a
    /// child's lineage columns; a merge stream's child table follows `root`, its root table, with
    /// a sequence column.
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
            ..self.clone()
        })
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
                path,
            };
            routes.push(self.route(&mut draft, &column, model.created())?);
        }
        let mut resolution = draft.finish(routes, &self.naming, &self.meta.all())?;
        // A normalized stream's table holds its rows' lineage even where they hold no other value,
        // so its first batch creates it.
        if self.meta.id.is_some() && !resolution.model.created() {
            resolution.model.version = 1;
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
            draft.add(key, logical, !column.is_key)
        };
        self.place(draft, column, original)
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
        let mut candidates = vec![original];
        candidates.extend(draft.variants(&column.path));
        if let Some(fitting) = candidates
            .into_iter()
            .find(|candidate| fits(&draft.column_type(*candidate), column.logical))
        {
            return Ok(Route::Column(fitting));
        }
        let current = draft.column_type(original);
        let joined = current.join(column.logical);
        let widens = !column.hinted
            && joined != LogicalType::Json
            && self.widens(&current, &joined, column.settings.nested);
        let cannot = |what: &str| format!("the column is {current} and {what} {}", column.logical);
        if column.is_key {
            if widens {
                draft.widen(original, joined);
                return Ok(Route::Column(original));
            }
            return Err(self.refused(column, "merge_key_changed", &cannot("the key cannot hold")));
        }
        match column.settings.policy {
            SchemaPolicy::Freeze => {
                return Err(self.refused(column, "schema_frozen", &cannot("cannot hold")));
            }
            SchemaPolicy::DiscardRow => return Ok(Route::DiscardRows),
            SchemaPolicy::DiscardValue => return Ok(Route::DiscardValues),
            SchemaPolicy::Evolve => {}
        }
        if widens {
            draft.widen(original, joined);
            return Ok(Route::Column(original));
        }
        if column.settings.on_unsupported == OnUnsupported::Refuse
            || !self.capabilities.schema_changes.add_column
        {
            let detail =
                cannot("the destination cannot change it, without a variant column, to hold");
            return Err(self.refused(column, "schema_change_unsupported", &detail));
        }
        Ok(Route::Column(self.variant(draft, column, &joined)))
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
            return draft.add(key(kind), joined.clone(), true);
        };
        let current = draft.column_type(existing);
        let wider = current.join(column.logical);
        if wider != LogicalType::Json
            && wider.kind() == kind
            && self.widens(&current, &wider, column.settings.nested)
        {
            draft.widen(existing, wider);
            return existing;
        }
        let json = key(TypeKind::Json);
        draft
            .find(&json)
            .unwrap_or_else(|| draft.add(json, LogicalType::Json, true))
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

/// A resolution in progress: the model, the columns it adds and the changes decided so far.
struct Draft {
    model: Model,
    /// Added columns, before they have identifiers: key, type and nullability.
    adds: Vec<(ColumnKey, LogicalType, bool)>,
    changes: Vec<Change>,
}

impl Draft {
    fn new(model: &Model) -> Self {
        Self {
            model: model.clone(),
            adds: Vec::new(),
            changes: Vec::new(),
        }
    }

    /// The position of the table's column holding `key`.
    ///
    /// A batch holds each column once, so a column this resolution adds is never looked up again.
    fn find(&self, key: &ColumnKey) -> Option<usize> {
        self.model.column(key).map(|(index, _)| index)
    }

    /// The positions of the table's variant columns of `path`, in kind order.
    fn variants(&self, path: &ColumnPath) -> Vec<usize> {
        let mut kinds: Vec<(TypeKind, usize)> = self
            .model
            .names
            .iter()
            .filter_map(|(key, _)| match key {
                ColumnKey::Variant { column, kind } if column == path => {
                    self.find(key).map(|index| (*kind, index))
                }
                _ => None,
            })
            .collect();
        kinds.sort_unstable();
        kinds.into_iter().map(|(_, index)| index).collect()
    }

    fn column_type(&self, column: usize) -> LogicalType {
        match self.model.columns.get(column) {
            Some(field) => field.logical_type().clone(),
            None => self.adds[column - self.model.columns.len()].1.clone(),
        }
    }

    /// Adds the column for `key`; returns its position.
    fn add(&mut self, key: ColumnKey, logical: LogicalType, nullable: bool) -> usize {
        self.adds.push((key.clone(), logical, nullable));
        self.changes.push(Change::Add { key });
        self.model.columns.len() + self.adds.len() - 1
    }

    /// Widens the table's column at `column` to `to`.
    ///
    /// Only existing columns widen: a column this resolution adds takes its final type when added.
    fn widen(&mut self, column: usize, to: LogicalType) {
        let field = &mut self.model.columns[column];
        let from = field.logical_type().clone();
        *field = Field::new(field.name(), to, field.is_nullable());
        self.changes.push(Change::Widen { column, from });
    }

    /// Names the added columns, in one sorted push, and appends them to the model.
    fn finish(
        mut self,
        routes: Vec<Route>,
        naming: &Naming,
        reserved: &[&str],
    ) -> Result<Resolution, Error> {
        let keys: BTreeSet<ColumnKey> = self.adds.iter().map(|(key, ..)| key.clone()).collect();
        naming.assign_columns(&mut self.model.names, &keys, reserved)?;
        for (key, logical, nullable) in self.adds {
            let name = self
                .model
                .names
                .get(&key)
                .expect("every added column was just named")
                .to_owned();
            self.model.columns.push(Field::new(name, logical, nullable));
        }
        if !self.changes.is_empty() {
            self.model.version += 1;
        }
        Ok(Resolution {
            changes: self.changes,
            routes,
            model: self.model,
        })
    }
}
