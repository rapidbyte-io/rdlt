//! JSON lines as the destination reads them back: no line held beyond its limit.

use std::fs::File;
use std::io::BufReader;

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};

use super::lines::Lines;
use crate::limits::{CHUNK_BYTES, LINE_BYTES, READ_BATCH_ROWS};

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
