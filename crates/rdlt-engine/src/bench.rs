//! Internals the benchmarks and fuzz targets drive; not part of the engine's API.

mod corpora;
mod json_passthrough;
mod loading;
mod lowering;
mod matrix;
mod measure;
mod merged;
mod mix;
mod mixed;
mod normalized;
mod passthrough;
mod pools;
mod readings;
mod replay;
mod resident;
mod runner;
mod sink;
mod tables;
mod tally;
#[cfg(test)]
mod tests;
mod wide;
mod zipf;

use std::num::NonZeroUsize;
use std::sync::{Arc, LazyLock};

use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::ArrowError;
use bytes::Bytes;

use crate::compute::{Pool, ready};
use crate::env::Env;
use crate::normalize::{self, Shape};
use crate::shred::{self, ShredError};

use sink::sink;

pub use crate::normalize::reference::compared::{Cut, Disagreement, normalized_against_reference};
pub use corpora::{CHUNK_BYTES, CORPUS_BYTES, Corpus, PUSH_BYTES};
pub use json_passthrough::{JsonCell, JsonPassthrough};
pub use loading::{Load, Loading};
pub use lowering::Lowering;
pub use matrix::{Cadence, Cell};
pub use measure::{Allocations, Paired, counted, logical_bytes, spread};
pub use merged::{Merged, Staging};
pub use mix::Mix;
pub use mixed::Mixed;
pub use normalized::Normalized;
pub use passthrough::Passthrough;
pub use pools::pool_cores;
pub use readings::Readings;
pub use replay::{
    Replayed, register, replay, replay_config, replay_factory, rows_pushed, split_replay_config,
    stream_name,
};
pub use resident::{Resident, reset_peak, resident_kib};
pub use sink::{SinkSession, SinkWriter, Sinking, ipc_sink, null_sink, sink_factory, written};
pub use tables::{Metered, Schedule, Tables, TablesCell, phases};
pub use tally::{Staged, staged};
pub use wide::{Form, Wide};
pub use zipf::Zipf;

/// Why the shredder refused pushes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refused {
    /// The error's machine code.
    pub code: &'static str,
    /// What went wrong.
    pub message: String,
}

impl From<ShredError> for Refused {
    fn from(error: ShredError) -> Self {
        Self {
            code: error.code().as_str(),
            message: error.to_string(),
        }
    }
}

/// A batch of one column of JSON, `rows` rows of the nested corpus's objects, or of the
/// string-heavy corpus's where `escaped` says.
///
/// # Panics
///
/// Panics where `rows` is zero or the batch does not build, which it always does.
pub fn json_column(rows: usize, escaped: bool) -> RecordBatch {
    let corpus = if escaped {
        Corpus::StringHeavy
    } else {
        Corpus::Nested
    };
    let count = u64::try_from(rows).expect("rows fit");
    let per_push = std::num::NonZeroU64::new(count).expect("at least one row");
    let text = corpus.rows(count, per_push).concat();
    let text = String::from_utf8(text).expect("the corpus is UTF-8");
    let values = arrow_array::StringArray::from_iter_values(text.lines().collect::<Vec<_>>());
    let schema = Arc::new(arrow_schema::Schema::new(vec![crate::json::bench_field(
        "payload",
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("the batch builds")
}

/// Checks every value of `batch`'s columns of JSON, as an Arrow push's are checked.
///
/// # Errors
///
/// The first value that is not JSON, with its code.
pub fn check_json(batch: &RecordBatch) -> Result<(), Refused> {
    crate::json::check_batch(batch).map_err(|refused| Refused {
        code: refused.error.code().as_str(),
        message: refused.error.to_string(),
    })
}

/// Bytes the engine admits a JSON push for, for the batches it becomes, for each byte of its
/// records.
pub const ADMITTED: u64 = crate::limits::JSON_ADMITTED;

/// How [`shred_with`] parses each chunk first.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Parse {
    /// Fast, and again exactly where a number may have been misread, as the engine parses.
    Fast,
    /// Exactly, every number read as it is written.
    Exact,
}

/// Shreds the JSON `pushes` on the calling thread, in chunks of `chunk_bytes`, as the engine
/// does.
pub fn shred(pushes: &[Bytes], chunk_bytes: usize) -> Result<Vec<RecordBatch>, Refused> {
    shred_with(pushes, chunk_bytes, ADMITTED, Parse::Fast)
}

/// Shreds the JSON `pushes` on the calling thread, in chunks of `chunk_bytes`, each chunk's
/// builders taking at most `admitted` bytes for each byte of its records before it is observed
/// instead, and each chunk parsed first as `parse` says: none of which may change the batches.
///
/// # Errors
///
/// Why the shredder refused the pushes.
pub fn shred_with(
    pushes: &[Bytes],
    chunk_bytes: usize,
    admitted: u64,
    parse: Parse,
) -> Result<Vec<RecordBatch>, Refused> {
    ready(shredded(
        inline(),
        pushes,
        chunk_bytes,
        limits(admitted, parse),
    ))
}

/// What shredding the JSON `pushes` on the calling thread, in chunks of `chunk_bytes`, took in
/// passes over its chunks.
///
/// # Errors
///
/// Why the shredder refused the pushes.
pub fn shred_counted(pushes: &[Bytes], chunk_bytes: usize) -> Result<crate::ShredCounts, Refused> {
    let limits = limits(ADMITTED, Parse::Fast);
    let shredding = ready(shred::observe(
        inline(),
        pushes.to_vec(),
        chunk_bytes,
        limits,
        rdlt_connector::Exclusion::default(),
    ))?;
    Ok(shredding.counts())
}

/// Shreds the JSON `pushes` on `env`'s compute pool, in chunks of `chunk_bytes`, as a partition
/// does.
pub async fn shred_on(
    env: &Arc<dyn Env>,
    pushes: &[Bytes],
    chunk_bytes: usize,
) -> Result<Vec<RecordBatch>, Refused> {
    let pool = Pool::new(Arc::clone(env), Arc::default());
    shredded(&pool, pushes, chunk_bytes, limits(ADMITTED, Parse::Fast)).await
}

/// The pool every call on the calling thread shares, so a call allocates nothing for it.
fn inline() -> &'static Pool {
    static INLINE: LazyLock<Pool> = LazyLock::new(Pool::inline);
    &INLINE
}

/// The limits of records holding as many columns as the wire's schemas, admitted for `admitted`
/// bytes a byte and parsed first as `parse` says.
fn limits(admitted: u64, parse: Parse) -> shred::ShredLimits {
    shred::ShredLimits {
        admitted,
        exact: parse == Parse::Exact,
        ..shred::ShredLimits::new(rdlt_wire::limits::SCHEMA_COLUMNS)
    }
}

/// Shreds the JSON `pushes` on `pool`, in chunks of `chunk_bytes`, within `limits`.
async fn shredded(
    pool: &Pool,
    pushes: &[Bytes],
    chunk_bytes: usize,
    limits: shred::ShredLimits,
) -> Result<Vec<RecordBatch>, Refused> {
    Ok(shred::shred(pool, pushes, chunk_bytes, limits).await?)
}

/// Normalizing batches as a stream normalized to `max_depth` and merged by `key` does, where it
/// names columns, keeping the schemas of the parts a batch splits into for the batches of its
/// schema that follow, as a partition keeps them.
#[derive(Debug)]
pub struct Normalizing {
    shape: Shape,
    schemas: normalize::PartSchemas,
}

impl Normalizing {
    /// Normalizing to `max_depth`, merged by `key` where it names columns.
    pub fn new(max_depth: u8, key: &[&str]) -> Self {
        Self {
            shape: Shape {
                max_depth,
                whole: std::collections::BTreeSet::new(),
                key: key.iter().map(|column| Arc::from(*column)).collect(),
            },
            schemas: normalize::PartSchemas::default(),
        }
    }

    /// `batch` normalized: each table's path below the stream's table, and its rows' data
    /// columns.
    ///
    /// # Errors
    ///
    /// Where normalizing refuses the batch.
    pub fn normalize(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<Vec<(Vec<String>, RecordBatch)>, ArrowError> {
        let parts = normalize::normalize(batch, &self.shape, &mut self.schemas)?;
        Ok(parts
            .into_iter()
            .map(|part| {
                let path = part.path.iter().map(ToString::to_string).collect();
                (path, part.batch)
            })
            .collect())
    }

    /// `batch` normalized, every `every`th root row dropped, and each child table's rows pruned
    /// of those whose parent went: the rows left in all its tables.
    ///
    /// # Errors
    ///
    /// Where normalizing refuses the batch.
    pub fn pruned(
        &mut self,
        batch: &RecordBatch,
        every: NonZeroUsize,
    ) -> Result<usize, ArrowError> {
        let parts = normalize::normalize(batch, &self.shape, &mut self.schemas)?;
        let mut parts = parts.into_iter();
        let Some(root) = parts.next() else {
            return Ok(0);
        };
        let root = normalize::Pruned::whole(root, normalize::PushKind::Arrow);
        let kept: BooleanArray = (0..root.part.batch.num_rows())
            .map(|row| Some(row % every.get() != 0))
            .collect();
        let mut dropped = normalize::Dropped::new(normalize::PushKind::Arrow);
        dropped.unkept(&root, &kept);
        let mut left = kept.true_count();
        for part in parts {
            left += dropped.prune(part)?.part.batch.num_rows();
        }
        Ok(left)
    }
}
