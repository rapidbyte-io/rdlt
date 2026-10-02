//! JSON lines read as the columns their lines name, so a column costs only the rows that hold it.
//!
//! A decoder is handed a schema and makes every column of it for every row it reads. Here the
//! lines are read in runs, each decoded under the columns its lines name, and a line starts a
//! new run where joining would leave the run's batch more cells its rows lack than cells they
//! hold, once the run is long enough or lacks enough for that to matter. A row of very many
//! columns among rows of few is so a batch of its own with a few of them, and rows that differ
//! by a few columns share one.

mod keys;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::{ArrowError, Schema, SchemaRef};

use super::lines::{self, Lines};
use crate::limits::{CHUNK_BYTES, LINE_BYTES, READ_BATCH_ROWS, RUN_ABSENT_CELLS, RUN_ROWS};

/// A line and the columns it names, by ascending place in the schema.
type Named = (Vec<u8>, Vec<usize>);

/// The rows of a JSON lines file, read in batches of the columns their lines name.
pub(super) struct Held {
    lines: Lines<BufReader<File>>,
    schema: SchemaRef,
    /// Each column's place in the schema, by name.
    places: HashMap<String, usize>,
    /// The columns every row holds, which a batch has whether or not its lines name them, so
    /// a line that lacks one is refused as it is under the whole schema.
    required: Vec<usize>,
    /// The line read past the last batch's end.
    ahead: Option<Named>,
}

impl Held {
    /// The rows of `file` under the types of `schema`.
    pub(super) fn new(file: File, schema: &SchemaRef) -> Self {
        let fields = schema.fields().iter().enumerate();
        Self {
            lines: Lines::new(BufReader::new(file), LINE_BYTES),
            schema: SchemaRef::clone(schema),
            places: fields
                .clone()
                .map(|(place, field)| (field.name().clone(), place))
                .collect(),
            required: fields
                .filter(|(_, field)| !field.is_nullable())
                .map(|(place, _)| place)
                .collect(),
            ahead: None,
        }
    }

    /// The next batch: the lines that follow, as many as a batch holds, until one would leave
    /// a batch of the columns any of them names lacking more cells than it holds; none once the
    /// file is read.
    ///
    /// A run is left to gather [`RUN_ROWS`] lines before it ends so, unless it would lack more
    /// than [`RUN_ABSENT_CELLS`]: every batch costs its columns something, so lines that take
    /// turns between few columns and many read as batches of all of them, as under the whole
    /// schema, while a line of very many columns still starts a run of its own.
    pub(super) fn next(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        let (mut run, mut rows, mut cells) = (Vec::new(), 0_usize, 0_usize);
        let mut columns: Vec<usize> = Vec::new();
        while let Some((line, named)) = self.line()? {
            let joined = union(&columns, &named);
            let held = cells + named.len();
            let absent = ((rows + 1) * joined.len()).saturating_sub(held);
            let gathered = rows >= RUN_ROWS || absent > RUN_ABSENT_CELLS;
            if rows != 0 && absent > held && gathered {
                self.ahead = Some((line, named));
                break;
            }
            run.extend_from_slice(&line);
            if !line.ends_with(b"\n") {
                run.push(b'\n');
            }
            (rows, cells, columns) = (rows + 1, held, joined);
            let bytes = u64::try_from(run.len()).unwrap_or(u64::MAX);
            if rows >= READ_BATCH_ROWS || bytes >= CHUNK_BYTES {
                break;
            }
        }
        match rows {
            0 => Ok(None),
            _ => decoded(&self.schema, &columns, &run, rows).map(Some),
        }
    }

    /// The next line that holds a record, with the columns it names; none at the file's end.
    fn line(&mut self) -> Result<Option<Named>, ArrowError> {
        if let Some(ahead) = self.ahead.take() {
            return Ok(Some(ahead));
        }
        let mut line = Vec::new();
        while self.lines.next(&mut line)? {
            if lines::holds_a_record(&line) {
                let named = self.named(&line)?;
                return Ok(Some((line, named)));
            }
        }
        Ok(None)
    }

    /// The columns the record `line` holds a value under, with those every row holds.
    fn named(&self, line: &[u8]) -> Result<Vec<usize>, ArrowError> {
        let mut columns = self.required.clone();
        for key in keys::valued(line)? {
            columns.extend(self.places.get(key.as_ref()));
        }
        columns.sort_unstable();
        columns.dedup();
        Ok(columns)
    }
}

/// The columns either of `left` and `right` holds, each ascending as they are.
fn union(left: &[usize], right: &[usize]) -> Vec<usize> {
    let mut joined = Vec::with_capacity(left.len().max(right.len()));
    let (mut left, mut right) = (left.iter().peekable(), right.iter().peekable());
    loop {
        let next = match (left.peek(), right.peek()) {
            (Some(a), Some(b)) if a == b => {
                right.next();
                left.next()
            }
            (Some(a), Some(b)) if a < b => left.next(),
            (Some(_), None) => left.next(),
            (_, Some(_)) => right.next(),
            (None, None) => return joined,
        };
        joined.extend(next);
    }
}

/// The `rows` records of `run`, lines that name `columns` of `schema`, as a batch of them.
fn decoded(
    schema: &SchemaRef,
    columns: &[usize],
    run: &[u8],
    rows: usize,
) -> Result<RecordBatch, ArrowError> {
    let fields: Vec<_> = columns
        .iter()
        .map(|column| Arc::clone(&schema.fields()[*column]))
        .collect();
    let held = Arc::new(Schema::new(fields));
    if columns.is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        return RecordBatch::try_new_with_options(held, Vec::new(), &options);
    }
    let mut decoder = arrow_json::ReaderBuilder::new(held)
        .with_batch_size(rows)
        .build_decoder()?;
    decoder.decode(run)?;
    let batch = decoder.flush()?;
    batch.ok_or_else(|| ArrowError::JsonError("a run of lines held no record".to_owned()))
}
