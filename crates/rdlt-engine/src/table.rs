//! Tables as the engine loads them: each stream's table, its schema versions and names, and how a
//! batch becomes rows the destination stores.

pub(crate) mod convert;
mod exact;
mod lower;
mod lowering;
mod model;
mod registry;
mod resolve;
mod session;
mod temporal;
#[cfg(test)]
pub(crate) mod testing;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_schema::SchemaRef;
use rdlt_connector::{
    ChangeColumns, ColumnKey, Deletion, Field, HistoryColumns, LogicalType, MergeKey,
    SchemaVersion, TableRef, TableSchema,
};

#[cfg(test)]
pub(crate) use convert::normalize as plain;
pub(crate) use exact::EXACT_IN_FLOAT;
pub(crate) use lower::{ChangeLayout, LineageColumns, MetaNames};
pub(crate) use lowering::{ChangeRows, LoweringPlan, Prepared, Stamp, data_ordinals};
pub(crate) use model::Model;
pub(crate) use registry::{Admission, Tables};
pub(crate) use resolve::{Incoming, Resolver, Settings};
pub(crate) use session::SharedSession;

/// A table at one schema version: everything needed to prepare batches for it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TableView {
    /// The table as writers and changes name it.
    pub(crate) table: TableRef,
    pub(crate) model: Model,
    /// How the destination stores each of the model's columns.
    pub(crate) lowered: Vec<LogicalType>,
    /// The destination's columns: the model's lowered, then the metadata columns.
    pub(crate) physical: Vec<Field>,
    /// The Arrow schema of prepared batches, whose load id and load start columns are
    /// dictionaries of one value.
    pub(crate) schema: SchemaRef,
    pub(crate) meta: MetaNames,
    /// The positions of the merge key's columns, once the table has them; empty for tables that
    /// do not merge.
    pub(crate) key: Vec<usize>,
    /// How many columns the merge key has.
    pub(crate) key_len: usize,
}

impl TableView {
    /// `model` of `table`, as `resolver` lowers and names it.
    pub(crate) fn new(table: &TableRef, model: Model, resolver: &Resolver) -> Self {
        let nested: Vec<_> = model
            .columns
            .iter()
            .map(|column| {
                model
                    .names
                    .owner(column.name())
                    .map(|key| resolver.settings.nested(key))
                    .unwrap_or_default()
            })
            .collect();
        let logical = lower::logical_fields(&model, &resolver.meta);
        let physical = lower::physical_fields(&logical, &nested, &resolver.capabilities);
        let lowered = physical
            .iter()
            .take(model.columns.len())
            .map(|field| field.logical_type().clone())
            .collect();
        let key_names: Vec<&str> = resolver
            .settings
            .key
            .iter()
            .filter_map(|path| model.names.get(&ColumnKey::Source(path.clone())))
            .collect();
        let merge = merge_key(resolver, &key_names);
        let key: Vec<usize> = key_names
            .iter()
            .filter_map(|name| {
                model
                    .columns
                    .iter()
                    .position(|column| column.name() == *name)
            })
            .collect();
        Self {
            table: TableRef {
                version: SchemaVersion(model.version),
                merge,
                ..table.clone()
            },
            schema: with_directives(
                &lower::prepared_schema(&physical, &logical, model.columns.len()),
                &resolver.meta,
                &key,
            ),
            lowered,
            physical,
            meta: resolver.meta.clone(),
            key,
            key_len: resolver.settings.key.len(),
            model,
        }
    }

    /// Whether batches keep only the last row of each key: a merge table's do, but not a child
    /// table's, which keeps every row of its roots' winning rows for the destination to pick, nor
    /// a history table's, which keeps every version.
    pub(crate) fn compacts(&self) -> bool {
        self.table
            .merge
            .as_ref()
            .is_some_and(|key| key.root.is_none() && key.history.is_none())
    }

    /// The destination's columns as a schema, as a table is created.
    pub(crate) fn physical_schema(&self) -> TableSchema {
        TableSchema::new(self.physical.clone())
            .expect("identifiers are distinct, metadata ones included")
    }
}

/// `schema`, the stored columns of prepared batches, with the columns that only direct a merge
/// after them; a change stream's merge batches may hold a null in its `key` columns, where a
/// truncate names no key.
fn with_directives(schema: &SchemaRef, meta: &MetaNames, key: &[usize]) -> SchemaRef {
    let directives = lower::directive_fields(meta);
    if directives.is_empty() {
        return Arc::clone(schema);
    }
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let field = field.as_ref().clone();
            if key.contains(&index) {
                field.with_nullable(true)
            } else {
                field
            }
        })
        .chain(directives)
        .collect();
    Arc::new(arrow_schema::Schema::new(fields))
}

/// How a table of `resolver`'s merges, if it does: by the columns `key_names`, or, for a child
/// table of a merge stream, by its rows' root id, following its root; a change stream's merge
/// table says what each row does.
fn merge_key(resolver: &Resolver, key_names: &[&str]) -> Option<MergeKey> {
    if resolver.root.is_none() && resolver.settings.key.is_empty() {
        return None;
    }
    let seq = resolver.meta.seq.as_ref()?;
    let changes = resolver
        .meta
        .changes
        .as_ref()
        .filter(|changes| !changes.stored)
        .map(|changes| ChangeColumns {
            op: Arc::clone(&changes.op),
            unchanged: resolver
                .meta
                .history
                .is_none()
                .then(|| Arc::clone(&changes.unchanged)),
            deletion: match &changes.deleted_at {
                Some(at) => Deletion::Soft { at: Arc::clone(at) },
                None => Deletion::Hard,
            },
        });
    Some(match (&resolver.root, &resolver.meta.parent) {
        (Some(root), Some([_, root_id, _])) => MergeKey {
            columns: vec![Arc::clone(root_id)],
            seq: Arc::clone(seq),
            root: Some(root.clone()),
            changes: None,
            history: None,
        },
        _ => MergeKey {
            columns: key_names.iter().map(|name| (*name).into()).collect(),
            seq: Arc::clone(seq),
            root: None,
            changes,
            history: resolver
                .meta
                .history
                .as_ref()
                .map(|history| HistoryColumns {
                    valid_from: Arc::clone(&history.valid_from),
                    valid_to: Arc::clone(&history.valid_to),
                    is_current: Arc::clone(&history.is_current),
                    row_hash: Arc::clone(&history.row_hash),
                }),
        },
    })
}
