//! How many bytes each chunk of a flush takes: enough records that each batch shares its columns'
//! work among many rows, enough jobs to keep every core busy, and no job past a limit.

#[cfg(test)]
mod tests;

use std::num::NonZeroUsize;

use bytes::Bytes;
use rdlt_connector::json::Structure;

use super::{End, element_end, json_whitespace};

/// Records each chunk of a flush holds at least where its limit allows: enough that the work a
/// batch does for each of its columns is shared among many rows.
const ROWS: usize = 1_024;

/// Bytes at the start of a flush whose records' lengths size its chunks.
const SAMPLE_BYTES: usize = 64 << 10;

/// How large the chunks a flush is cut into are, each shredded in a job of its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Sizing {
    /// Records a chunk holds at least, by the length of the flush's first records.
    pub(crate) rows: usize,
    /// Bytes: the most of the flush a chunk takes before the record end that closes it.
    pub(crate) limit: NonZeroUsize,
    /// The cores the jobs share, each given about two jobs of a flush where its records allow.
    pub(crate) cores: NonZeroUsize,
}

impl Sizing {
    /// Chunks as a partition cuts them: of at least [`ROWS`] records, and at most `limit` bytes,
    /// for `cores` cores.
    pub(crate) fn new(limit: NonZeroUsize, cores: NonZeroUsize) -> Self {
        Self {
            rows: ROWS,
            limit,
            cores,
        }
    }

    /// Chunks of `chunk_bytes` each, whatever their records and the cores.
    #[cfg(any(test, feature = "bench"))]
    pub(crate) fn fixed(chunk_bytes: usize) -> Self {
        Self {
            rows: usize::MAX,
            limit: NonZeroUsize::new(chunk_bytes).unwrap_or(NonZeroUsize::MIN),
            cores: NonZeroUsize::MIN,
        }
    }

    /// Bytes each chunk of the flush `pushes` takes before the record end that closes it.
    ///
    /// They are what [`Sizing::rows`] of its records take, by the records that end in its first
    /// 64 KiB, or an even share of the flush that gives each core two jobs, so a chunk that trips
    /// or is built again leaves no core idle, whichever is more; but never more than the limit.
    /// Where no record ends in those 64 KiB, they are the limit.
    pub(crate) fn chunk_bytes(self, pushes: &[Bytes]) -> usize {
        let limit = self.limit.get();
        let Some(record) = record_bytes(pushes) else {
            return limit;
        };
        let text = pushes
            .iter()
            .map(Bytes::len)
            .fold(0_usize, usize::saturating_add);
        let share = text.div_ceil(self.cores.get().saturating_mul(2));
        limit.min(self.rows.saturating_mul(record).max(share))
    }
}

/// Bytes a record of the flush `pushes` takes, by the records that end in its first
/// [`SAMPLE_BYTES`]: at line ends, or at an array's top-level commas; `None` where none ends
/// there.
fn record_bytes(pushes: &[Bytes]) -> Option<usize> {
    let (mut sampled, mut ended) = (0_usize, 0_usize);
    for push in pushes {
        let sample = &push[..push.len().min(SAMPLE_BYTES - sampled)];
        sampled += sample.len();
        ended += match sample.iter().position(|byte| !json_whitespace(*byte)) {
            Some(open) if sample[open] == b'[' => commas(sample, open),
            Some(_) => memchr::memchr_iter(b'\n', sample).count(),
            None => 0,
        };
    }
    (ended > 0).then(|| sampled.div_ceil(ended))
}

/// The top-level commas of the array `text` opens at `open`, up to where it ends or breaks.
fn commas(text: &[u8], open: usize) -> usize {
    let mut structure = Structure::new(text, open + 1);
    let mut commas = 0;
    while let End::Comma(_) = element_end(&mut structure) {
        commas += 1;
    }
    commas
}
