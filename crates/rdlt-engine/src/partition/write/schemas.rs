//! The table schema of the Arrow schema a partition's units had last, kept so units of one
//! schema are converted once, whether they share its `Arc` or each bring their own; and, for a
//! normalized stream, the schemas of the parts its batches split into.

use std::sync::Arc;

use arrow_schema::SchemaRef;
use rdlt_connector::{ColumnPath, SchemaError, TableSchema};

use crate::normalize::PartSchemas;

/// The table schema and column paths of the last Arrow schema a partition's units had, and the
/// schemas of the parts a normalized stream's batches of one schema split into.
#[derive(Debug, Default)]
pub(crate) struct Schemas {
    last: Option<Converted>,
    pub(super) parts: PartSchemas,
    /// How many schemas were converted, not found kept.
    #[cfg(test)]
    misses: usize,
}

/// An Arrow schema, its table schema and its columns' paths.
#[derive(Debug)]
struct Converted {
    arrow: SchemaRef,
    schema: Arc<TableSchema>,
    paths: Arc<[ColumnPath]>,
}

impl Schemas {
    /// The table schema of `arrow` and its columns' paths, in order: those kept, shared, where
    /// `arrow` is the schema converted last, the same `Arc` or equal to it, its metadata
    /// included.
    ///
    /// # Errors
    ///
    /// Where a column of `arrow` has no table schema.
    pub(crate) fn of(
        &mut self,
        arrow: &SchemaRef,
    ) -> Result<(Arc<TableSchema>, Arc<[ColumnPath]>), SchemaError> {
        let kept = self
            .last
            .as_ref()
            .filter(|last| Arc::ptr_eq(&last.arrow, arrow) || *last.arrow == **arrow);
        if let Some(kept) = kept {
            return Ok((Arc::clone(&kept.schema), Arc::clone(&kept.paths)));
        }
        #[cfg(test)]
        {
            self.misses += 1;
        }
        let schema = Arc::new(TableSchema::from_arrow(arrow)?);
        let paths: Arc<[ColumnPath]> = schema
            .fields()
            .iter()
            .map(|field| ColumnPath::from(field.name()))
            .collect();
        self.last = Some(Converted {
            arrow: Arc::clone(arrow),
            schema: Arc::clone(&schema),
            paths: Arc::clone(&paths),
        });
        Ok((schema, paths))
    }
}

#[cfg(test)]
mod tests;
