//! JSON shredding: pushes of JSON records become Arrow batches, on the compute pool.
//!
//! Each push of a coalesced unit is split into its records on the pool, and the records are
//! grouped into chunks. Each chunk is parsed on the pool, its values observed and, speculatively, built into columns typed by what the chunk
//! held. The observations are joined in push order into one shape; a chunk whose own shape is
//! that shape keeps its columns, and any other is parsed again and built against it, on the
//! pool. The join is the same whatever the order chunks finish in, so the batches are too.

mod build;
mod conform;
#[cfg(test)]
mod differential;
mod observe;
#[cfg(test)]
mod reference;
mod render;
#[cfg(test)]
mod tests;
mod visit;

use std::ops::Range;
use std::sync::Arc;

use arrow_array::RecordBatch;
use bytes::Bytes;
use rdlt_connector::TableSchema;
use rdlt_connector::limits::MAX_COLUMNS;
use serde::de::DeserializeSeed;

use crate::compute::{ComputePool, run_all};

use build::Record;
use observe::Shape;
use visit::{Context, Row};

/// Why a JSON push cannot be shredded.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ShredError {
    /// The push is not JSON.
    #[error("the push is not valid JSON: {0}")]
    Invalid(String),
    /// A record is not an object.
    #[error("a record is not a JSON object")]
    NotObject,
    /// A value nests deeper than the limit.
    #[error(
        "a value nests deeper than {} levels",
        rdlt_connector::limits::MAX_NESTING_DEPTH
    )]
    TooDeep,
    /// An object repeats a key.
    #[error("an object repeats the key {0:?}")]
    DuplicateKey(String),
    /// The records have more columns than the limit.
    #[error("the records have {0} columns, over the limit of {MAX_COLUMNS}")]
    TooManyColumns(usize),
    /// The records would shred into more cells than the limit.
    #[error("{0} rows under {1} columns are more cells than one shred builds, {MAX_CELLS}")]
    TooManyCells(u64, u64),
    /// A list holds more items than a column can.
    #[error("a list column holds more items than one batch can")]
    TooLarge,
    /// A bug in the shredder.
    #[error("shredding: {0}")]
    Internal(String),
}

impl ShredError {
    /// The machine code of the error.
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::Invalid(_) => "json_invalid",
            Self::NotObject => "json_not_object",
            Self::TooDeep | Self::TooManyColumns(_) | Self::TooManyCells(..) | Self::TooLarge => {
                "limit_exceeded"
            }
            Self::DuplicateKey(_) => "json_duplicate_key",
            Self::Internal(_) => "shred_internal",
        }
    }
}

/// A JSON push and where each of its records lies in it.
#[derive(Clone, Debug)]
struct Records {
    push: Bytes,
    ranges: Vec<Range<usize>>,
}

impl Records {
    /// The records of `push`: a JSON array of objects, or objects on their own lines, blank lines
    /// skipped.
    ///
    /// Each record is checked when it is parsed.
    fn of(push: Bytes) -> Result<Self, ShredError> {
        let ranges = records(&push)?;
        Ok(Self { push, ranges })
    }
}

/// Whole records of some pushes: where each lies in its push.
struct Chunk {
    parts: Vec<(Bytes, Vec<Range<usize>>)>,
    rows: usize,
    /// How many records of the pushes come before the chunk's first.
    before: usize,
}

impl Chunk {
    /// The records, in order.
    fn records(&self) -> impl Iterator<Item = &[u8]> {
        self.parts
            .iter()
            .flat_map(|(push, records)| records.iter().map(|record| &push[record.clone()]))
    }
}

/// The records of `pushes`, grouped into chunks of about `chunk_bytes` each.
fn chunks(pushes: &[Records], chunk_bytes: usize) -> Vec<Chunk> {
    let mut chunks = Vec::new();
    let mut chunk = Chunk {
        parts: Vec::new(),
        rows: 0,
        before: 0,
    };
    let mut size = 0;
    for push in pushes {
        let mut records = Vec::new();
        for record in &push.ranges {
            size += record.len();
            records.push(record.clone());
            if size >= chunk_bytes {
                chunk.rows += records.len();
                chunk
                    .parts
                    .push((push.push.clone(), std::mem::take(&mut records)));
                let before = chunk.before + chunk.rows;
                chunks.push(std::mem::replace(
                    &mut chunk,
                    Chunk {
                        parts: Vec::new(),
                        rows: 0,
                        before,
                    },
                ));
                size = 0;
            }
        }
        if !records.is_empty() {
            chunk.rows += records.len();
            chunk.parts.push((push.push.clone(), records));
        }
    }
    if chunk.rows > 0 {
        chunks.push(chunk);
    }
    chunks
}

/// Where each record of `push` lies in it.
fn records(push: &[u8]) -> Result<Vec<Range<usize>>, ShredError> {
    let Some(first) = push.iter().position(|byte| !byte.is_ascii_whitespace()) else {
        return Ok(Vec::new());
    };
    if push[first] == b'[' {
        return elements(push, first);
    }
    let base = push.as_ptr() as usize;
    let mut records = Vec::new();
    let mut start = 0;
    // Each line but the first starts with the line end before it, which trimming drops.
    for end in memchr::memchr_iter(b'\n', push).chain(std::iter::once(push.len())) {
        let trimmed = push[start..end].trim_ascii();
        if !trimmed.is_empty() {
            let offset = trimmed.as_ptr() as usize - base;
            records.push(offset..offset + trimmed.len());
        }
        start = end;
    }
    Ok(records)
}

/// Where each element of the JSON array opening at `open` in `push` lies, found without
/// recursing however deep the elements nest; each element is parsed, and so checked, later.
fn elements(push: &[u8], open: usize) -> Result<Vec<Range<usize>>, ShredError> {
    let invalid = |what: &str| ShredError::Invalid(format!("the push is not a JSON array: {what}"));
    let mut elements = Vec::new();
    let mut start = open + 1;
    let mut depth = 0_usize;
    let mut in_string = false;
    let mut escaped = false;
    let mut close = None;
    for (index, &byte) in push.iter().enumerate().skip(open + 1) {
        if in_string {
            match byte {
                _ if escaped => escaped = false,
                b'\\' => escaped = true,
                b'"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'[' | b'{' => depth += 1,
            b']' if depth == 0 => {
                close = Some(index);
                break;
            }
            b']' | b'}' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| invalid("unbalanced brackets"))?;
            }
            b',' if depth == 0 => {
                elements
                    .push(element(push, start..index).ok_or_else(|| invalid("an empty element"))?);
                start = index + 1;
            }
            _ => {}
        }
    }
    let close = close.ok_or_else(|| invalid("it does not end"))?;
    match element(push, start..close) {
        Some(last) => elements.push(last),
        None if !elements.is_empty() => return Err(invalid("an empty element")),
        None => {}
    }
    if !push[close + 1..].trim_ascii().is_empty() {
        return Err(invalid("more follows it"));
    }
    Ok(elements)
}

/// The range `range` of `push` without its surrounding whitespace, unless nothing is left.
fn element(push: &[u8], range: Range<usize>) -> Option<Range<usize>> {
    let bytes = &push[range.clone()];
    let trimmed = bytes.trim_ascii();
    let start = range.start + (trimmed.as_ptr() as usize - bytes.as_ptr() as usize);
    (!trimmed.is_empty()).then(|| start..start + trimmed.len())
}

/// One chunk, parsed, with the columns built from it and the shape its values were observed to
/// have.
struct Parsed {
    chunk: Chunk,
    record: Record,
    shape: Shape,
    /// Whether a column stopped building, so the chunk must be built again.
    spoiled: bool,
}

/// Parses `chunk`, observing its values and building them into columns as they arrive.
fn parse(chunk: Chunk) -> Result<Parsed, ShredError> {
    let mut record = Record::empty(chunk.rows);
    let spoiled = append(&chunk, &mut record)?;
    Ok(Parsed {
        shape: record.shape(),
        chunk,
        record,
        spoiled,
    })
}

/// Appends the records of `chunk` to `record`; returns whether a column stopped building.
fn append(chunk: &Chunk, record: &mut Record) -> Result<bool, ShredError> {
    let context = Context::default();
    for (index, bytes) in chunk.records().enumerate() {
        let mut deserializer = sonic_rs::Deserializer::from_slice(bytes);
        Row {
            record: &mut *record,
            context: &context,
        }
        .deserialize(&mut deserializer)
        .and_then(|()| deserializer.end())
        .map_err(|error| {
            context
                .fault()
                .unwrap_or_else(|| invalid(chunk.before + index, &error))
        })?;
    }
    Ok(context.spoiled())
}

/// The error for record `index` of the pushes, which sonic-rs refused with `error`: what broke and
/// where, without the excerpt of the record sonic-rs quotes after it.
fn invalid(index: usize, error: &sonic_rs::Error) -> ShredError {
    let message = error.to_string();
    let what = message.lines().next().unwrap_or_default();
    ShredError::Invalid(format!("record {}: {what}", index + 1))
}

/// The shape every chunk's records fit: the join of each chunk's, in push order.
fn join(parsed: &[Parsed]) -> Result<Shape, ShredError> {
    let mut joined = Shape::default();
    for chunk in parsed {
        joined.join(&chunk.shape);
    }
    // Each chunk refuses an object over the limit as it is read; objects that each fit may still
    // join into one that does not.
    let widest = joined.widest();
    if !build::within_columns(widest) {
        return Err(ShredError::TooManyColumns(widest));
    }
    let rows = parsed
        .iter()
        .map(|chunk| u64::try_from(chunk.chunk.rows).unwrap_or(u64::MAX))
        .fold(0, u64::saturating_add);
    let leaves = joined.leaves();
    if !within_cells(rows, leaves) {
        return Err(ShredError::TooManyCells(rows, leaves));
    }
    Ok(joined)
}

/// Cells one shred may build: rows times the columns holding values.
///
/// Every row takes a cell in every column, so sparse, wide records would otherwise build gigabytes
/// of nulls from a small push.
const MAX_CELLS: u64 = 1 << 25;

/// Whether `rows` rows under `leaves` columns holding values are within [`MAX_CELLS`].
fn within_cells(rows: u64, leaves: u64) -> bool {
    rows.checked_mul(leaves)
        .is_some_and(|cells| cells <= MAX_CELLS)
}

/// The batch of `parsed`'s records against `shape`: the columns built as it was parsed, fitted to
/// `shape`, when they fit it, else built again.
fn build(parsed: Parsed, shape: &Shape) -> Result<RecordBatch, ShredError> {
    let rows = parsed.chunk.rows;
    let columns = if !parsed.spoiled && conform::shape_fits(&parsed.shape, shape) {
        conform::columns(&parsed.record.finish_columns()?, &parsed.shape, shape, rows)?
    } else {
        // Every value fits the joined shape, so no column stops building this time; a bug that
        // broke that would fail to finish the column or to make the batch.
        let mut record = Record::new(shape, rows);
        append(&parsed.chunk, &mut record)?;
        record.finish_columns()?
    };
    let schema = TableSchema::new(shape.logical_fields())
        .map_err(|error| ShredError::Internal(format!("naming the columns: {error}")))?;
    let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(rows));
    RecordBatch::try_new_with_options(Arc::new(schema.to_arrow()), columns, &options)
        .map_err(|error| ShredError::Internal(format!("building a batch: {error}")))
}

/// Shreds the JSON `pushes`, in order, into one batch per chunk of about `chunk_bytes`, on `pool`.
///
/// A push is a JSON array of objects, or objects on their own lines; blank lines are skipped.
pub(crate) async fn shred(
    pool: &dyn ComputePool,
    pushes: &[Bytes],
    chunk_bytes: usize,
) -> Result<Vec<RecordBatch>, ShredError> {
    let records: Vec<Records> = run_all(
        pool,
        pushes.iter().cloned().map(|push| move || Records::of(push)),
    )
    .await
    .into_iter()
    .collect::<Result<_, _>>()?;
    let chunks = chunks(&records, chunk_bytes);
    let parsed: Vec<Parsed> = run_all(pool, chunks.into_iter().map(|chunk| move || parse(chunk)))
        .await
        .into_iter()
        .collect::<Result<_, _>>()?;
    let shape = Arc::new(join(&parsed)?);
    run_all(
        pool,
        parsed.into_iter().map(|chunk| {
            let shape = Arc::clone(&shape);
            move || build(chunk, &shape)
        }),
    )
    .await
    .into_iter()
    .collect()
}
