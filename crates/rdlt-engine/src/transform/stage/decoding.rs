//! A transformed stream's encoded columns decoded before its first transform.
//!
//! What a dictionary's or a run-end encoding's rows hold, decoded, is not linear in the bytes it
//! keeps, so no transform could declare a bound on computing from one. Lowering decodes them
//! anyway, so the stage decodes them first, with lowering's own decoder, asking for the room the
//! decoded rows take as lowering measures it.

use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchOptions, new_empty_array};
use arrow_schema::{ArrowError, DataType, FieldRef, Fields, Schema, SchemaRef};
use rdlt_connector::cost::{Rendering, arrays};

use super::Refusal;
use crate::table::convert::decoded;

/// The places, among a chain's data columns, of those that arrive encoded, and what the arrays
/// decoding them makes take beside their buffers.
#[derive(Debug, Default)]
pub(super) struct Encoded {
    places: Vec<usize>,
    arrays: u64,
}

impl Encoded {
    pub(super) fn is_empty(&self) -> bool {
        self.places.is_empty()
    }
}

/// `arrived`, a chain's data columns as they arrive, with each encoded column as the type it
/// decodes to, and where those columns are.
pub(super) fn decoded_schema(arrived: &SchemaRef) -> Result<(SchemaRef, Encoded), Refusal> {
    let places: Vec<usize> = (0..arrived.fields().len())
        .filter(|place| encoded(arrived.field(*place).data_type()))
        .collect();
    if places.is_empty() {
        return Ok((Arc::clone(arrived), Encoded::default()));
    }
    let mut fields: Vec<FieldRef> = arrived.fields().iter().cloned().collect();
    for place in &places {
        let field = &fields[*place];
        // What lowering's decoder makes of a column of this type, which the type alone fixes.
        let empty = decoded(&new_empty_array(field.data_type())).map_err(|error| failed(&error))?;
        let data_type = empty.data_type().clone();
        let nullable = field.is_nullable() || holds_nulls(field.data_type());
        let decoded = field.as_ref().clone().with_data_type(data_type);
        fields[*place] = Arc::new(decoded.with_nullable(nullable));
    }
    let decoded_fields: Fields = places
        .iter()
        .map(|place| Arc::clone(&fields[*place]))
        .collect();
    let schema = Schema::new_with_metadata(fields, arrived.metadata().clone());
    let arrays = arrays(&decoded_fields);
    Ok((Arc::new(schema), Encoded { places, arrays }))
}

/// Bytes: what decoding `data`'s encoded columns takes, as lowering measures a decode, no
/// further than `limit`.
pub(super) fn room(data: &RecordBatch, encoded: &Encoded, limit: u64) -> Result<u64, Refusal> {
    let columns = data
        .project(&encoded.places)
        .map_err(|error| failed(&error))?;
    let measured = Rendering::native().expanded(&columns, 0..data.num_rows(), limit);
    Ok(measured.saturating_add(encoded.arrays))
}

/// `data` with its encoded columns decoded, as `schema` says they decode.
pub(super) fn decode(
    data: &RecordBatch,
    encoded: &Encoded,
    schema: &SchemaRef,
) -> Result<RecordBatch, Refusal> {
    let mut columns = data.columns().to_vec();
    for place in &encoded.places {
        columns[*place] = decoded(&columns[*place]).map_err(|error| failed(&error))?;
    }
    let options = RecordBatchOptions::new().with_row_count(Some(data.num_rows()));
    RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options)
        .map_err(|error| failed(&error))
}

/// Whether the values an encoding of `data_type` names may hold a null its field does not show,
/// which decoding makes a null of the column: a table holds it as lowering would, nullable
/// unless the column keys it.
fn holds_nulls(data_type: &DataType) -> bool {
    match data_type {
        DataType::RunEndEncoded(_, values) => values.is_nullable(),
        _ => true,
    }
}

/// Whether a column of `data_type` holds a dictionary's keys or runs, naming values beside it.
fn encoded(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Dictionary(..) | DataType::RunEndEncoded(..)
    )
}

fn failed(error: &ArrowError) -> Refusal {
    Refusal::Internal(format!("decoding a batch's encoded columns: {error}"))
}
