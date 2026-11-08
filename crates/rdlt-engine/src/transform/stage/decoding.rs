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
        // The values a dictionary's keys or a run-end encoding's runs name may hold a null
        // whatever their field says, which decoding makes a null of the column. A table holds
        // the column as lowering would: nullable, a key's null refused as lowering refuses it.
        let decoded = field.as_ref().clone().with_data_type(data_type);
        fields[*place] = Arc::new(decoded.with_nullable(true));
    }
    let decoded_fields: Fields = places
        .iter()
        .map(|place| Arc::clone(&fields[*place]))
        .collect();
    let schema = Schema::new_with_metadata(fields, arrived.metadata().clone());
    let arrays = arrays(&decoded_fields);
    Ok((Arc::new(schema), Encoded { places, arrays }))
}

/// Bytes a column decoded may take: lowering's decoder keeps a dictionary's or a run-end
/// encoding's text in an array of its values' own type, whose 32-bit offsets name no more than
/// 2 GiB less a byte.
pub(super) const OFFSETS: u64 = 0x7fff_ffff;

/// Bytes: what decoding `data`'s encoded columns takes, as lowering measures a decode, no
/// further than `limit`, and what the widest of them takes where that is beyond [`OFFSETS`].
pub(super) fn room(
    data: &RecordBatch,
    encoded: &Encoded,
    limit: u64,
) -> Result<(u64, Option<u64>), Refusal> {
    let mut room = encoded.arrays;
    let mut widest = 0;
    for place in &encoded.places {
        let column = data.project(&[*place]).map_err(|error| failed(&error))?;
        let measured = Rendering::native().expanded(&column, 0..data.num_rows(), limit);
        room = room.saturating_add(measured);
        widest = widest.max(measured);
    }
    Ok((room, (widest > OFFSETS).then_some(widest)))
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
