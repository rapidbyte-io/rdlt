//! JSON lines as the destination writes and reads them back: every float exactly, and no line
//! held beyond its limit.

use std::fs::File;
use std::io::BufReader;

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch};
use arrow_json::writer::{Encoder, EncoderFactory, EncoderOptions, NullableEncoder, make_encoder};
use arrow_schema::{ArrowError, DataType, FieldRef, SchemaRef};

use super::lines::Lines;
use crate::limits::{CHUNK_BYTES, LINE_BYTES, READ_BATCH_ROWS};

/// Encodes floats and dictionaries so each row reads back as the value written.
///
/// JSON has no number for a float that is not finite, which arrow-json writes as `null`; here
/// it is the string naming it. A dictionary may hold a null among its values, which arrow-json
/// writes as whatever bytes lie under it; here a row is null where its key is null or the
/// value its key stands for is.
#[derive(Debug)]
pub(super) struct ExactFloats;

impl EncoderFactory for ExactFloats {
    fn make_default_encoder<'a>(
        &self,
        field: &'a FieldRef,
        array: &'a dyn Array,
        options: &'a EncoderOptions,
    ) -> Result<Option<NullableEncoder<'a>>, ArrowError> {
        let encoder: Box<dyn Encoder + 'a> = match array.data_type() {
            DataType::Float32 | DataType::Float64 => Box::new(Floats(array)),
            DataType::Dictionary(..) => {
                let dictionary = array.as_any_dictionary();
                let values = dictionary.values();
                // A dictionary of no values has no key that is not null.
                let keys = match values.len() {
                    0 => Vec::new(),
                    _ => dictionary.normalized_keys(),
                };
                let values = make_encoder(field, values.as_ref(), options)?;
                let keyed: Box<dyn Encoder + 'a> = Box::new(Keyed { keys, values });
                return Ok(Some(NullableEncoder::new(keyed, array.logical_nulls())));
            }
            _ => return Ok(None),
        };
        Ok(Some(NullableEncoder::new(encoder, array.nulls().cloned())))
    }
}

/// Writes each row of a dictionary as the value its key stands for.
struct Keyed<'a> {
    /// Each row's place among the values; that of a row whose key is null is never asked for.
    keys: Vec<usize>,
    values: NullableEncoder<'a>,
}

impl Encoder for Keyed<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        self.values.encode(self.keys[idx], out);
    }
}

/// Writes each float as [`rdlt_connector::json::write_float`] writes it, as the engine writes
/// floats into JSON, so a value read back after its column widened is the value written.
struct Floats<'a>(&'a dyn Array);

impl Encoder for Floats<'_> {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        rdlt_connector::json::write_float(self.0, idx, out);
    }
}

/// The rows of a JSON lines file, read as a table's schema in batches.
pub(super) struct Rows {
    lines: Lines<BufReader<File>>,
    decoder: arrow_json::reader::Decoder,
    line: Vec<u8>,
    /// How much of `line` the decoder took.
    taken: usize,
    /// Bytes given to the decoder since its last batch.
    pending: u64,
    ended: bool,
}

impl std::fmt::Debug for Rows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Rows").finish_non_exhaustive()
    }
}

impl Rows {
    /// The rows of `file` as `schema`.
    pub(super) fn new(file: File, schema: &SchemaRef) -> Result<Self, ArrowError> {
        let decoder = arrow_json::ReaderBuilder::new(SchemaRef::clone(schema))
            .with_batch_size(READ_BATCH_ROWS)
            .build_decoder()?;
        Ok(Self {
            lines: Lines::new(BufReader::new(file), LINE_BYTES),
            decoder,
            line: Vec::new(),
            taken: 0,
            pending: 0,
            ended: false,
        })
    }

    /// The next batch of rows, none once the file is read.
    pub(super) fn next(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        while !self.ended {
            if self.taken == self.line.len() {
                if !self.lines.next(&mut self.line)? {
                    self.ended = true;
                    break;
                }
                self.taken = 0;
            }
            let took = self.decoder.decode(&self.line[self.taken..])?;
            self.taken += took;
            self.pending = self
                .pending
                .saturating_add(u64::try_from(took).unwrap_or(u64::MAX));
            // The decoder stops short of a line only once it holds a batch's rows.
            let full = self.taken < self.line.len();
            if full || self.pending >= CHUNK_BYTES {
                self.pending = 0;
                if let Some(batch) = self.decoder.flush()? {
                    return Ok(Some(batch));
                }
            }
        }
        self.decoder.flush()
    }
}
