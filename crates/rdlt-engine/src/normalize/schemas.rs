//! The schemas of the parts batches of one schema normalize into, kept so a stream whose batches
//! share a schema builds each part's fields, names and table schema once, not once a batch.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::{Field as ArrowField, FieldRef, Schema, SchemaRef};
use rdlt_connector::{ColumnPath, SchemaError, TableSchema};

use super::name;

/// The schemas of the parts the batches of one Arrow schema split into, by each part's path below
/// the stream's table.
///
/// A part's schema is kept for its columns' paths and types, which one shape and its batches'
/// values decide, and is used again only where they are the same.
#[derive(Debug, Default)]
pub(crate) struct PartSchemas {
    /// The Arrow schema whose parts are kept.
    of: Option<SchemaRef>,
    parts: BTreeMap<Vec<Arc<str>>, Kept>,
    /// How many part schemas were built, not found kept.
    #[cfg(test)]
    built: usize,
}

/// A part's Arrow schema, its columns' paths and, once asked for, its table schema.
#[derive(Debug)]
struct Kept {
    arrow: SchemaRef,
    paths: Arc<[ColumnPath]>,
    table: Option<Arc<TableSchema>>,
}

impl PartSchemas {
    /// Keeps the parts of batches of `schema`, letting go of any other's: the same `Arc` or a
    /// schema equal to it, its metadata included, keeps them.
    pub(super) fn keep(&mut self, schema: &SchemaRef) {
        let kept = self
            .of
            .as_ref()
            .is_some_and(|kept| Arc::ptr_eq(kept, schema) || **kept == **schema);
        if !kept {
            self.of = Some(Arc::clone(schema));
            self.parts.clear();
        }
    }

    /// The schema of the part at `path` whose columns are `columns`, each with its path, the
    /// field it came from and its values, and its columns' paths: those kept where the part had
    /// them before, column for column, of the same types.
    ///
    /// A column's type is its values', which normalizing decodes where an object around them
    /// holds nulls, so batches of one schema may differ in it.
    pub(super) fn part(
        &mut self,
        path: &[Arc<str>],
        columns: &[(ColumnPath, FieldRef, ArrayRef)],
    ) -> (SchemaRef, Arc<[ColumnPath]>) {
        if let Some(kept) = self.parts.get(path) {
            let fields = kept.arrow.fields();
            let same = fields.len() == columns.len()
                && fields.iter().zip(kept.paths.iter()).zip(columns).all(
                    |((field, kept), (path, _, values))| {
                        kept == path && field.data_type() == values.data_type()
                    },
                );
            if same {
                return (Arc::clone(&kept.arrow), Arc::clone(&kept.paths));
            }
        }
        #[cfg(test)]
        {
            self.built += 1;
        }
        let fields: Vec<ArrowField> = columns
            .iter()
            .map(|(path, field, values)| {
                // The field keeps its metadata, such as the JSON and UUID extension names.
                field
                    .as_ref()
                    .clone()
                    .with_name(name(path))
                    .with_data_type(values.data_type().clone())
                    .with_nullable(true)
            })
            .collect();
        let arrow = Arc::new(Schema::new(fields));
        let paths: Arc<[ColumnPath]> = columns.iter().map(|(path, _, _)| path.clone()).collect();
        let kept = Kept {
            arrow: Arc::clone(&arrow),
            paths: Arc::clone(&paths),
            table: None,
        };
        self.parts.insert(path.to_vec(), kept);
        (arrow, paths)
    }

    /// The table schema of a part at `path` whose batches are of `arrow`: kept where `arrow` is
    /// the schema kept for the part, and converted otherwise.
    ///
    /// # Errors
    ///
    /// Where a column of `arrow` has no table schema.
    pub(crate) fn table(
        &mut self,
        path: &[Arc<str>],
        arrow: &SchemaRef,
    ) -> Result<Arc<TableSchema>, SchemaError> {
        let kept = self
            .parts
            .get_mut(path)
            .filter(|kept| Arc::ptr_eq(&kept.arrow, arrow));
        let Some(kept) = kept else {
            return Ok(Arc::new(TableSchema::from_arrow(arrow)?));
        };
        if let Some(table) = &kept.table {
            return Ok(Arc::clone(table));
        }
        let table = Arc::new(TableSchema::from_arrow(arrow)?);
        kept.table = Some(Arc::clone(&table));
        Ok(table)
    }
}
