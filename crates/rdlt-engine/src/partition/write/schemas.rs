//! The table schema of the Arrow schema a partition's units had last, kept so units of one
//! schema are converted once, whether they share its `Arc` or each bring their own.

use std::sync::Arc;

use arrow_schema::SchemaRef;
use rdlt_connector::{ColumnPath, SchemaError, TableSchema};

/// The table schema and column paths of the last Arrow schema a partition's units had.
#[derive(Debug, Default)]
pub(crate) struct Schemas {
    last: Option<Converted>,
    /// How many schemas were converted, not found kept.
    #[cfg(test)]
    misses: usize,
}

/// An Arrow schema, its table schema and its columns' paths.
#[derive(Debug)]
struct Converted {
    arrow: SchemaRef,
    schema: TableSchema,
    paths: Vec<ColumnPath>,
}

impl Schemas {
    /// The table schema of `arrow` and its columns' paths, in order: those kept where `arrow`
    /// is the schema converted last, the same `Arc` or equal to it, its metadata included.
    ///
    /// # Errors
    ///
    /// Where a column of `arrow` has no table schema.
    pub(crate) fn of(
        &mut self,
        arrow: &SchemaRef,
    ) -> Result<(TableSchema, Vec<ColumnPath>), SchemaError> {
        let kept = self
            .last
            .as_ref()
            .filter(|last| Arc::ptr_eq(&last.arrow, arrow) || *last.arrow == **arrow);
        if let Some(kept) = kept {
            return Ok((kept.schema.clone(), kept.paths.clone()));
        }
        #[cfg(test)]
        {
            self.misses += 1;
        }
        let schema = TableSchema::from_arrow(arrow)?;
        let paths: Vec<ColumnPath> = schema
            .fields()
            .iter()
            .map(|field| ColumnPath::from(field.name()))
            .collect();
        self.last = Some(Converted {
            arrow: Arc::clone(arrow),
            schema: schema.clone(),
            paths: paths.clone(),
        });
        Ok((schema, paths))
    }
}

#[cfg(test)]
mod tests;
