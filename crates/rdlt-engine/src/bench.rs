//! Internals the benchmarks and fuzz targets drive (spec §21.2); not part of the engine's API.

#[cfg(test)]
mod tests;

use arrow_array::RecordBatch;
use bytes::Bytes;

use crate::compute::{ComputePool, Inline, ready};
use crate::shred::{self, ShredError};

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
