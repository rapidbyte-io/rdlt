//! Seeded workloads: the streams a simulated source serves and the rows each phase holds.

#[cfg(test)]
mod tests;

use rdlt_connector::{Checkpointing, ReadMode};
use rdlt_engine::WriteMode;

use crate::rng::SplitMix64;

/// How many phases a simulation runs; the source changes between them.
pub const PHASES: usize = 2;

/// Everything a simulated source serves.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Workload {
    /// Mixed into every value, so different seeds produce different rows.
    pub salt: u64,
    /// The streams.
    pub streams: Vec<SimStream>,
}

/// One simulated stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimStream {
    /// The stream's name.
    pub name: String,
    /// How the pipeline reads it.
    pub read: ReadMode,
    /// How the pipeline writes it.
    pub write: WriteMode,
    /// How it checkpoints.
    pub checkpointing: Checkpointing,
    /// Batches between checkpoints of a naturally checkpointing stream.
    pub checkpoint_every: u64,
    /// Whether the stream checkpoints after its last batch.
    ///
    /// Incremental streams always do: a partition that ends with rows after its last checkpoint is
    /// done and never read again.
    pub final_checkpoint: bool,
    /// Rows per pushed batch.
    pub batch_rows: u64,
    /// The rows each partition holds in each phase.
    pub partitions: Vec<[u64; PHASES]>,
}

/// One row as the simulation tracks it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Row {
    /// Unique within the stream.
    pub id: i64,
    /// The partition's index.
    pub partition: i64,
    /// The row's position within its partition.
    pub offset: i64,
    /// A value that changes between phases for full reads.
    pub value: i64,
}

impl Workload {
    /// A workload of one to three streams drawn from `rng`.
    pub fn generate(rng: &mut SplitMix64) -> Self {
        let salt = rng.next_u64();
        let streams = (0..=rng.below(3))
            .map(|index| SimStream::generate(index, rng))
            .collect();
        Self { salt, streams }
    }
}

impl SimStream {
    fn generate(index: u64, rng: &mut SplitMix64) -> Self {
        let (read, write) = match rng.below(3) {
            0 => (ReadMode::Incremental, WriteMode::Append),
            1 => (ReadMode::Full, WriteMode::Append),
            _ => (ReadMode::Full, WriteMode::Replace),
        };
        let partitions = (0..=rng.below(4))
            .map(|_| {
                let first = rng.below(40);
                let second = match read {
                    ReadMode::Incremental => first + rng.below(20),
                    _ => rng.below(40),
                };
                [first, second]
            })
            .collect();
        Self {
            name: format!("s{index}"),
            read,
            write,
            checkpointing: if rng.chance(500) {
                Checkpointing::OnDemand
            } else {
                Checkpointing::Natural
            },
            checkpoint_every: 1 + rng.below(3),
            final_checkpoint: read == ReadMode::Incremental || rng.chance(700),
            batch_rows: 1 + rng.below(8),
            partitions,
        }
    }

    /// The rows `partition` holds in `phase`, in order.
    pub fn rows(&self, salt: u64, partition: usize, phase: usize) -> Vec<Row> {
        let count = self.partitions[partition][phase];
        (0..count)
            .map(|offset| {
                let partition = i64::try_from(partition).unwrap_or(i64::MAX);
                let offset = i64::try_from(offset).unwrap_or(i64::MAX);
                let id = partition * 1_000_000 + offset;
                // Incremental rows never change; a full read sees new values in each phase.
                let version = match self.read {
                    ReadMode::Incremental => 0,
                    _ => phase as u64 + 1,
                };
                let value = mix(salt ^ mix(id.unsigned_abs() ^ (version << 48)));
                Row {
                    id,
                    partition,
                    offset,
                    value: i64::from_ne_bytes(value.to_ne_bytes()),
                }
            })
            .collect()
    }

    /// Every row of the stream in `phase`.
    pub fn all_rows(&self, salt: u64, phase: usize) -> Vec<Row> {
        (0..self.partitions.len())
            .flat_map(|partition| self.rows(salt, partition, phase))
            .collect()
    }
}

/// The `SplitMix64` output function.
fn mix(x: u64) -> u64 {
    SplitMix64::new(x).next_u64()
}
