//! Internals the benchmarks and fuzz targets drive (spec §21.2); not part of the engine's API.

mod connectors;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::ArrowError;
use bytes::Bytes;

use crate::compute::{ComputePool, Inline, ready};
use crate::normalize::{self, Shape};
use crate::shred::{self, ShredError};

pub use connectors::{SinkSession, SinkWriter, ipc_sink, replay};

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
            code: error.code(),
            message: error.to_string(),
        }
    }
}

/// Shreds the JSON `pushes` on the calling thread, in chunks of `chunk_bytes`.
pub fn shred(pushes: &[Bytes], chunk_bytes: usize) -> Result<Vec<RecordBatch>, Refused> {
    ready(shred_on(&Inline, pushes, chunk_bytes))
}

/// Shreds the JSON `pushes` on `pool`, in chunks of `chunk_bytes`, as a partition does.
pub async fn shred_on(
    pool: &dyn ComputePool,
    pushes: &[Bytes],
    chunk_bytes: usize,
) -> Result<Vec<RecordBatch>, Refused> {
    Ok(shred::shred(pool, pushes, chunk_bytes).await?)
}

/// `batch` normalized as a stream normalized to `max_depth` whose rows `key` identifies: each
/// table's path below the stream's table, and its rows' data columns.
pub fn normalize(
    batch: &RecordBatch,
    max_depth: u8,
    key: &[&str],
) -> Result<Vec<(Vec<String>, RecordBatch)>, ArrowError> {
    let shape = Shape {
        max_depth,
        whole: std::collections::BTreeSet::new(),
        key: key.iter().map(|column| Arc::from(*column)).collect(),
    };
    Ok(normalize::normalize(batch, &shape)?
        .into_iter()
        .map(|part| {
            let path = part.path.iter().map(ToString::to_string).collect();
            (path, part.batch)
        })
        .collect())
}
