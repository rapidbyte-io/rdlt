//! Seeded workloads: the streams a simulated source serves and the rows each phase holds.

#[cfg(test)]
mod tests;

use rdlt_connector::{Checkpointing, ReadMode};
use rdlt_engine::{Nested, SchemaPolicy, WriteMode};

use crate::rng::SplitMix64;

/// How many phases a simulation runs; the source changes between them.
pub const PHASES: usize = 2;

/// Names drift columns draw from; some fold to one identifier under case-folding rules.
const DRIFT_NAMES: [&str; 6] = ["d0", "D0", "extra", "Extra", "note", "a__b"];

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
    /// For merge streams, how many keys each partition's rows share; 0 otherwise.
    pub keys: u64,
    /// Whether the plan names the merge key, rather than the catalog's primary key.
    pub plan_key: bool,
    /// Columns whose presence and type change across partitions and phases.
    pub drift: Vec<Drift>,
    /// What the pipeline does with schema changes.
    pub policy: SchemaPolicy,
    /// How the pipeline stores nested values.
    pub nested: Nested,
    /// Whether the source pushes its rows as JSON rather than Arrow.
    pub json: bool,
}

/// A column that comes and goes and changes type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Drift {
    /// The column's name.
    pub name: String,
    /// Its shape in each partition and phase; `None` where batches lack the column.
    pub shapes: Vec<[Option<Shape>; PHASES]>,
}

/// The type of a drift column in one partition and phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Shape {
    /// 32-bit integers.
    Int32,
    /// 64-bit integers beyond 32 bits.
    Int64,
    /// Floats in quarters, which every rendering keeps exactly.
    Float,
    /// Text.
    Text,
    /// A struct with one 64-bit field `n`.
    Object,
    /// A list of 64-bit integers.
    List,
}

impl Shape {
    const ALL: [Self; 6] = [
        Self::Int32,
        Self::Int64,
        Self::Float,
        Self::Text,
        Self::Object,
        Self::List,
    ];
}

/// A drift column's value.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Extra {
    /// An integer of a 32- or 64-bit column.
    Int(i64),
    /// A float, in quarters.
    Quarters(i64),
    /// Text.
    Text(String),
    /// A struct `{ n }`.
    Object(i64),
    /// A list.
    List(Vec<i64>),
}

/// One row as the simulation tracks it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Row {
    /// Unique within the stream.
    pub id: i64,
    /// The partition's index.
    pub partition: i64,
    /// The row's position within its partition.
    pub offset: i64,
    /// A value that changes between phases for full reads.
    pub value: i64,
    /// The merge key, for merge streams.
    pub key: Option<i64>,
    /// Each drift column's value, in the stream's drift order; `None` is null or absent.
    pub extras: Vec<Option<Extra>>,
    /// The phase whose shapes the row's drift values take.
    pub delivered: usize,
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
        let (read, write) = match rng.below(5) {
            0 => (ReadMode::Incremental, WriteMode::Append),
            1 => (ReadMode::Full, WriteMode::Append),
            2 => (ReadMode::Full, WriteMode::Replace),
            3 => (ReadMode::Incremental, WriteMode::Merge),
            _ => (ReadMode::Full, WriteMode::Merge),
        };
        let partitions: Vec<[u64; PHASES]> = (0..=rng.below(4))
            .map(|_| {
                let first = rng.below(40);
                let second = match read {
                    ReadMode::Incremental => first + rng.below(20),
                    _ => rng.below(40),
                };
                [first, second]
            })
            .collect();
        let drift = drift(rng, partitions.len());
        let mut stream = Self {
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
            keys: if write == WriteMode::Merge {
                1 + rng.below(12)
            } else {
                0
            },
            plan_key: rng.chance(500),
            policy: match rng.below(10) {
                0 => SchemaPolicy::DiscardRow,
                1 => SchemaPolicy::DiscardValue,
                _ => SchemaPolicy::Evolve,
            },
            nested: if rng.chance(300) {
                Nested::Json
            } else {
                Nested::Native
            },
            json: rng.chance(333),
            drift,
            partitions,
        };
        if rng.chance(300) {
            stream.nested = Nested::normalize();
        }
        stream
    }

    /// Whether the stream's arrays land in child tables.
    pub fn normalized(&self) -> bool {
        matches!(self.nested, Nested::Normalize { .. })
    }

    /// The phase that first delivers row `offset` of `partition`: incremental rows keep the phase
    /// they first appeared in, full reads deliver every row again in each phase.
    fn delivered(&self, partition: usize, offset: u64, phase: usize) -> usize {
        if self.read != ReadMode::Incremental {
            return phase;
        }
        (0..=phase)
            .find(|earlier| offset < self.partitions[partition][*earlier])
            .unwrap_or(phase)
    }

    /// The rows `partition` holds in `phase`, in order.
    pub fn rows(&self, salt: u64, partition: usize, phase: usize) -> Vec<Row> {
        let count = self.partitions[partition][phase];
        (0..count)
            .map(|offset| {
                let delivered = self.delivered(partition, offset, phase);
                let index = i64::try_from(partition).unwrap_or(i64::MAX);
                let position = i64::try_from(offset).unwrap_or(i64::MAX);
                let id = index * 1_000_000 + position;
                // Incremental rows never change; a full read sees new values in each phase.
                let version = delivered as u64 + 1;
                let value = mix(salt ^ mix(id.unsigned_abs() ^ (version << 48)));
                let key = (self.keys > 0)
                    .then(|| index * 1_000_000 + i64::try_from(offset % self.keys).unwrap_or(0));
                let extras = self
                    .drift
                    .iter()
                    .enumerate()
                    .map(|(column, drift)| {
                        let shape = drift.shapes[partition][delivered]?;
                        extra(shape, mix(value ^ (column as u64 + 1)))
                    })
                    .collect();
                Row {
                    id,
                    partition: index,
                    offset: position,
                    value: i64::from_ne_bytes(value.to_ne_bytes()),
                    key,
                    extras,
                    delivered,
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

/// Zero to three drift columns, each present in some partitions and phases with a drawn shape.
fn drift(rng: &mut SplitMix64, partitions: usize) -> Vec<Drift> {
    let mut names: Vec<&str> = DRIFT_NAMES.to_vec();
    (0..rng.below(4))
        .map(|_| {
            let name = names.remove(usize::try_from(rng.below(names.len() as u64)).unwrap_or(0));
            let first = Shape::ALL[usize::try_from(rng.below(6)).unwrap_or(0)];
            let shapes = (0..partitions)
                .map(|_| {
                    std::array::from_fn(|_| {
                        if rng.chance(250) {
                            None
                        } else if rng.chance(600) {
                            Some(first)
                        } else {
                            Some(Shape::ALL[usize::try_from(rng.below(6)).unwrap_or(0)])
                        }
                    })
                })
                .collect();
            Drift {
                name: name.to_owned(),
                shapes,
            }
        })
        .collect()
}

/// A value of `shape` drawn from `bits`, null one time in five.
fn extra(shape: Shape, bits: u64) -> Option<Extra> {
    if bits.is_multiple_of(5) {
        return None;
    }
    let small = i64::try_from(bits % 1_000).unwrap_or(0) - 500;
    Some(match shape {
        Shape::Int32 => Extra::Int(small),
        Shape::Int64 => Extra::Int((small << 36) + 7),
        Shape::Float => Extra::Quarters(small),
        Shape::Text => Extra::Text(format!("t{small}")),
        Shape::Object => Extra::Object(small),
        Shape::List => Extra::List(vec![small, small + 1]),
    })
}

/// The `SplitMix64` output function.
fn mix(x: u64) -> u64 {
    SplitMix64::new(x).next_u64()
}
