//! JSON shredding: pushes of JSON records become Arrow batches, on the compute pool.
//!
//! The pushes of a coalesced unit are scanned on the pool for where their records lie, grouped
//! into chunks. Each chunk is parsed on the pool, its values observed and, speculatively, built into columns typed by what the chunk
//! held. The observations are joined in push order into one shape; a chunk whose own shape is
//! that shape keeps its columns, and any other is parsed again and built against it, on the
//! pool. The join is the same whatever the order chunks finish in, so the batches are too.

mod build;
mod conform;
#[cfg(test)]
mod differential;
mod exact;
mod observe;
mod records;
#[cfg(test)]
mod reference;
mod render;
#[cfg(test)]
mod tests;
mod visit;

use std::sync::Arc;

use arrow_array::RecordBatch;
use bytes::Bytes;
use rdlt_connector::TableSchema;
use rdlt_connector::limits::MAX_COLUMNS;
use serde::de::DeserializeSeed;

use crate::compute::{ComputePool, run_all};

use build::Record;
use observe::Shape;
use records::{Chunk, chunks};
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

/// One chunk, parsed, with the columns built from it and the shape its values were observed to
/// have.
struct Parsed {
    chunk: Chunk,
    record: Record,
    shape: Shape,
    /// Whether a column stopped building, so the chunk must be built again.
    spoiled: bool,
    /// Whether the chunk is parsed with exact numbers, as one holding an integer beyond 64 bits is.
    exact: bool,
}

/// Parses `chunk`, observing its values and building them into columns as they arrive.
///
/// A chunk the fast parse finds a float that may be a rounded integer in is parsed again with its
/// numbers exact.
fn parse(chunk: Chunk) -> Result<Parsed, ShredError> {
    let mut record = Record::empty(chunk.rows);
    let mut parse = append(&chunk, &mut record, false)?;
    if parse.imprecise {
        record = Record::empty(chunk.rows);
        parse = append(&chunk, &mut record, true)?;
    }
    Ok(Parsed {
        shape: record.shape(),
        chunk,
        record,
        spoiled: parse.spoiled,
        exact: parse.imprecise,
    })
}

/// How appending a chunk went.
struct Appended {
    /// Whether a column stopped building.
    spoiled: bool,
    /// Whether the parse stopped at a float that may be a rounded integer, or, parsing exactly,
    /// whether it parsed so.
    imprecise: bool,
}

/// Appends the records of `chunk` to `record`, with their numbers exact where `exact` says.
///
/// The fast parse reads an integer beyond 64 bits as the float nearest it, and refuses one beyond a
/// float's range, so it stops at the first float that may be one, or at the first refusal, leaving
/// the chunk to be parsed again exactly.
fn append(chunk: &Chunk, record: &mut Record, exact: bool) -> Result<Appended, ShredError> {
    let context = Context::default();
    for (index, bytes) in chunk.records().enumerate() {
        let appended = if exact {
            exact::append(bytes, &mut *record, &context)
        } else {
            let mut deserializer = sonic_rs::Deserializer::from_slice(bytes);
            Row {
                record: &mut *record,
                context: &context,
            }
            .deserialize(&mut deserializer)
            .and_then(|()| deserializer.end())
        };
        if let Err(error) = appended {
            if let Some(fault) = context.fault() {
                return Err(fault);
            }
            // The fast parse refuses an integer beyond a float's range as it refuses invalid JSON;
            // the exact parse tells them apart.
            if exact {
                return Err(invalid(chunk.before + index, &error));
            }
            context.reparse();
        }
        if context.imprecise() && !exact {
            break;
        }
    }
    Ok(Appended {
        spoiled: context.spoiled(),
        imprecise: exact || context.imprecise(),
    })
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
        append(&parsed.chunk, &mut record, parsed.exact)?;
        record.finish_columns()?
    };
    let schema = TableSchema::new(shape.logical_fields())
        .map_err(|error| ShredError::Internal(format!("naming the columns: {error}")))?;
    let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(rows));
    RecordBatch::try_new_with_options(Arc::new(schema.to_arrow()), columns, &options)
        .map_err(|error| ShredError::Internal(format!("building a batch: {error}")))
}

/// Stack a shredding job is sure of before it starts, 4 MiB: building and checking the columns of
/// values at the nesting limit walks their types once per level.
const JOB_STACK: usize = 4_194_304;

/// Stack added when a job must grow it, 8 MiB.
const JOB_SEGMENT: usize = 8_388_608;

/// Runs `work`, one shredding job, with at least [`JOB_STACK`] of stack, whatever thread runs it.
fn job<T>(work: impl FnOnce() -> T) -> T {
    stacker::maybe_grow(JOB_STACK, JOB_SEGMENT, work)
}

/// Shreds the JSON `pushes`, in order, into one batch per chunk of about `chunk_bytes`, on `pool`.
///
/// A push is a JSON array of objects, or objects on their own lines; blank lines are skipped.
pub(crate) async fn shred(
    pool: &dyn ComputePool,
    pushes: &[Bytes],
    chunk_bytes: usize,
) -> Result<Vec<RecordBatch>, ShredError> {
    let scanned = pushes.to_vec();
    let chunks = run_all(pool, [move || chunks(&scanned, chunk_bytes)])
        .await
        .pop()
        .ok_or_else(|| {
            ShredError::Internal("the scan of the pushes returned nothing".to_owned())
        })??;
    let parsed: Vec<Parsed> = run_all(
        pool,
        chunks.into_iter().map(|chunk| move || job(|| parse(chunk))),
    )
    .await
    .into_iter()
    .collect::<Result<_, _>>()?;
    let shape = Arc::new(join(&parsed)?);
    run_all(
        pool,
        parsed.into_iter().map(|chunk| {
            let shape = Arc::clone(&shape);
            move || job(|| build(chunk, &shape))
        }),
    )
    .await
    .into_iter()
    .collect()
}
