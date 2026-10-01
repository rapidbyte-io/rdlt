//! Batches without dictionaries: each dictionary column as the values it stands for, so batches
//! written apart, each with dictionaries of its own, fit one file.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};

/// `schema` with every dictionary column, and every dictionary within a struct or a list, as
/// its values' type; dictionaries within other types stay.
pub(crate) fn plain(schema: &Schema) -> SchemaRef {
    let fields: Vec<Field> = schema.fields().iter().map(|field| valued(field)).collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// `field` holding values where it held a dictionary of them.
fn valued(field: &Field) -> Field {
    let item = |item: &Arc<Field>| Arc::new(valued(item));
    let data_type = match field.data_type() {
        DataType::Dictionary(_, values) => {
            return valued(&field.clone().with_data_type(values.as_ref().clone()));
        }
        DataType::List(inner) => DataType::List(item(inner)),
        DataType::LargeList(inner) => DataType::LargeList(item(inner)),
        DataType::FixedSizeList(inner, size) => DataType::FixedSizeList(item(inner), *size),
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(item).collect()),
        other => other.clone(),
    };
    field.clone().with_data_type(data_type)
}

/// `batch` in `schema`, the plain form of its own: its dictionaries as their values.
///
/// # Errors
///
/// Arrow cannot cast a column, or the batch is of another schema.
pub(crate) fn unkeyed(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch, ArrowError> {
    if batch.schema_ref() == schema {
        return Ok(batch.clone());
    }
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| arrow_cast::cast(column, field.data_type()))
        .collect::<Result<Vec<ArrayRef>, _>>()?;
    RecordBatch::try_new(SchemaRef::clone(schema), columns)
}
