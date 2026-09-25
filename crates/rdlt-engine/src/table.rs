//! Tables as the engine loads them: each stream's table, its schema versions and names, and how a
//! batch becomes rows the destination stores.

mod convert;
mod lower;
mod lowering;
mod model;
mod registry;
mod resolve;
mod session;
#[cfg(test)]
pub(crate) mod testing;
#[cfg(test)]
mod tests;

use arrow_schema::SchemaRef;
use rdlt_connector::{
    ColumnKey, Field, LogicalType, MergeKey, SchemaVersion, TableRef, TableSchema,
};

pub(crate) use lower::{LineageColumns, MetaNames};
pub(crate) use lowering::{LoweringPlan, Prepared, Stamp};
pub(crate) use model::Model;
pub(crate) use registry::Tables;
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
        let physical =
            lower::physical_fields(&model, &nested, &resolver.meta, &resolver.capabilities);
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
        let merge = resolver.meta.seq.as_ref().map(|seq| MergeKey {
            columns: key_names.iter().map(|name| (*name).into()).collect(),
            seq: seq.clone(),
        });
        let key = key_names
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
            schema: lower::prepared_schema(&physical, model.columns.len()),
            lowered,
            physical,
            meta: resolver.meta.clone(),
            key,
            key_len: resolver.settings.key.len(),
            model,
        }
    }

    /// The destination's columns as a schema, as a table is created.
    pub(crate) fn physical_schema(&self) -> TableSchema {
        TableSchema::new(self.physical.clone())
            .expect("identifiers are distinct, metadata ones included")
    }
}
