//! Seeded workloads: the streams a simulated source serves and the rows each phase holds.

#[cfg(test)]
mod tests;
mod values;

use rdlt_connector::{Checkpointing, ReadMode};
use rdlt_engine::{Nested, SchemaPolicy, WriteMode};
use rdlt_testkit::draw::{draw, mix};
use rdlt_testkit::drawn::{Scalar, Shape, json, neighbors};

use crate::rng::SplitMix64;
use crate::swarm::Features;

/// How many phases a simulation runs; the source changes between them.
pub const PHASES: usize = 2;

/// Names drift columns draw from; some fold to one identifier under case-folding rules.
const DRIFT_NAMES: [&str; 6] = ["d0", "D0", "extra", "Extra", "note", "a__b"];

/// Everything a simulated source serves.
#[derive(Clone, Debug, PartialEq)]
pub struct Workload {
    /// Mixed into every value, so different seeds produce different rows.
    pub salt: u64,
    /// The features the seed exercises.
    pub features: Features,
    /// The streams.
    pub streams: Vec<SimStream>,
}

/// One simulated stream.
#[derive(Clone, Debug, PartialEq)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "each is a way the stream differs, drawn independently"
)]
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
    /// Whether the source sends each batch as a slice of a larger one.
    pub sliced: bool,
    /// Each partition's rows in each phase.
    rows: Vec<[Vec<Row>; PHASES]>,
}

/// A column that comes and goes and changes type.
#[derive(Clone, Debug, PartialEq)]
pub struct Drift {
    /// The column's name.
    pub name: String,
    /// Its shape in each partition and phase; `None` where batches lack the column.
    pub shapes: Vec<[Option<Shape>; PHASES]>,
}

/// One row as the simulation tracks it.
#[derive(Clone, Debug, PartialEq)]
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
    /// Each drift column's value, in the stream's drift order: `None` where the row's batch lacks
    /// the column.
    pub extras: Vec<Option<Scalar>>,
    /// The phase whose shapes the row's drift values take.
    pub delivered: usize,
}

impl Workload {
    /// A workload of one to three streams drawn from `rng`, exercising `features`.
    pub fn generate(rng: &mut SplitMix64, features: Features) -> Self {
        let salt = rng.next_u64();
        let streams = (0..=rng.below(3))
            .map(|index| SimStream::generate(index, rng, features, salt))
            .collect();
        Self {
            salt,
            features,
            streams,
        }
    }
}

impl SimStream {
    fn generate(index: u64, rng: &mut SplitMix64, features: Features, salt: u64) -> Self {
        let (read, write) = modes(rng);
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
        let json = features.json && rng.chance(500);
        let drift = if features.drift {
            drift(rng, partitions.len(), features, json)
        } else {
            Vec::new()
        };
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
            json,
            sliced: features.sliced && rng.chance(500),
            drift,
            partitions,
            rows: Vec::new(),
        };
        if features.normalize && rng.chance(500) {
            stream.nested = normalized(rng);
        }
        stream.rows = (0..stream.partitions.len())
            .map(|partition| stream.draw_rows(salt, partition))
            .collect();
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

    /// The rows `partition` holds in each phase; a row delivered in an earlier phase is that
    /// phase's row again.
    fn draw_rows(&self, salt: u64, partition: usize) -> [Vec<Row>; PHASES] {
        let mut phases: [Vec<Row>; PHASES] = Default::default();
        for phase in 0..PHASES {
            let count = self.partitions[partition][phase];
            let rows = (0..count)
                .map(|offset| {
                    let delivered = self.delivered(partition, offset, phase);
                    let position = usize::try_from(offset).unwrap_or(usize::MAX);
                    match phases[delivered].get(position) {
                        Some(row) if delivered < phase => row.clone(),
                        _ => self.row(salt, partition, offset, delivered),
                    }
                })
                .collect();
            phases[phase] = rows;
        }
        phases
    }

    /// Row `offset` of `partition` as `delivered` delivers it.
    fn row(&self, salt: u64, partition: usize, offset: u64, delivered: usize) -> Row {
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
                let shape = drift.shapes[partition][delivered].as_ref()?;
                let seed = mix(value ^ (column as u64 + 1));
                Some(values::drawn(shape, !self.json, seed))
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
    }

    /// The rows `partition` holds in `phase`, in order.
    pub fn rows(&self, partition: usize, phase: usize) -> &[Row] {
        &self.rows[partition][phase]
    }

    /// Every row of the stream in `phase`.
    pub fn all_rows(&self, phase: usize) -> Vec<Row> {
        (0..self.partitions.len())
            .flat_map(|partition| self.rows(partition, phase).iter().cloned())
            .collect()
    }
}

/// How a stream is read and written.
fn modes(rng: &mut SplitMix64) -> (ReadMode, WriteMode) {
    match rng.below(5) {
        0 => (ReadMode::Incremental, WriteMode::Append),
        1 => (ReadMode::Full, WriteMode::Append),
        2 => (ReadMode::Full, WriteMode::Replace),
        3 => (ReadMode::Incremental, WriteMode::Merge),
        _ => (ReadMode::Full, WriteMode::Merge),
    }
}

/// Normalizing, to the default depth or a shallow one that stores deeper containers whole, as
/// JSON.
fn normalized(rng: &mut SplitMix64) -> Nested {
    let max_depth = if rng.chance(500) {
        u8::try_from(1 + rng.below(3)).unwrap_or(1)
    } else {
        8
    };
    Nested::Normalize { max_depth }
}

/// Zero to three drift columns, each present in some partitions and phases with a drawn shape:
/// its first shape, a type the lattice joins it with, or another; for a JSON stream, only types
/// JSON holds.
fn drift(rng: &mut SplitMix64, partitions: usize, features: Features, json: bool) -> Vec<Drift> {
    let mut names: Vec<&str> = DRIFT_NAMES.to_vec();
    let fresh = |rng: &mut SplitMix64| -> Shape {
        let seed = rng.next_u64();
        if json {
            draw(&json::shape(features.depth), seed)
        } else {
            let shape = draw(&rdlt_testkit::drawn::values::shape(features.depth), seed);
            values::plain_unless(shape, features.encodings)
        }
    };
    (0..rng.below(4))
        .map(|_| {
            let name = names.remove(usize::try_from(rng.below(names.len() as u64)).unwrap_or(0));
            let first = fresh(rng);
            let shapes = (0..partitions)
                .map(|_| {
                    std::array::from_fn(|_| {
                        if rng.chance(250) {
                            None
                        } else if rng.chance(600) {
                            Some(first.clone())
                        } else if !json && rng.chance(500) {
                            let seed = rng.next_u64();
                            let neighbor = draw(&neighbors::neighbor(&first), seed);
                            Some(values::plain_unless(neighbor, features.encodings))
                        } else {
                            Some(fresh(rng))
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
