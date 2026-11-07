//! A code transform the simulated streams draw: it declares no bound, so the stage measures what
//! it makes, and cuts a batch whose output does not fit in halves.

use std::sync::Arc;

use arrow_array::cast::AsArray as _;
use arrow_array::types::Int64Type;
use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use rdlt_engine::{Transform, TransformError};

use crate::workload::TWICE_COLUMN;

/// Adds [`TWICE_COLUMN`], each row's `offset` doubled.
#[derive(Debug)]
pub(super) struct Twice;

impl Twice {
    /// Where `input` holds the offsets.
    fn offset(input: &Schema) -> Result<usize, TransformError> {
        match input.index_of("offset") {
            Ok(at) if *input.field(at).data_type() == DataType::Int64 => Ok(at),
            _ => {
                Err(TransformError::new("the batch holds no integer offsets").with_column("offset"))
            }
        }
    }
}

impl Transform for Twice {
    fn name(&self) -> &'static str {
        "twice"
    }

    fn output(&self, input: &Schema) -> Result<SchemaRef, TransformError> {
        Self::offset(input)?;
        let mut fields: Vec<FieldRef> = input.fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new(TWICE_COLUMN, DataType::Int64, true)));
        let schema = Schema::new_with_metadata(fields, input.metadata().clone());
        Ok(Arc::new(schema))
    }

    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, TransformError> {
        let offsets = batch.column(Self::offset(batch.schema_ref())?);
        let doubled = offsets
            .as_primitive::<Int64Type>()
            .unary::<_, Int64Type>(|offset| offset.wrapping_mul(2));
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(doubled));
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        let schema = self.output(batch.schema_ref())?;
        RecordBatch::try_new_with_options(schema, columns, &options)
            .map_err(|_| TransformError::new("the doubled offsets do not fit the batch"))
    }

    fn lineage(&self, input: &Schema) -> Option<Vec<Vec<usize>>> {
        let at = Self::offset(input).ok()?;
        let mut lineage: Vec<Vec<usize>> = (0..input.fields().len())
            .map(|column| vec![column])
            .collect();
        lineage.push(vec![at]);
        Some(lineage)
    }
}
