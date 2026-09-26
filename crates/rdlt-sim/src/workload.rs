//! Seeded workloads: the streams a simulated source serves and the rows each phase holds.

mod keys;
mod schema;
mod settings;
#[cfg(test)]
mod tests;
mod values;

use std::ops::Range;

use rdlt_connector::{Checkpointing, LogicalType, ReadMode};
use rdlt_engine::{Nested, OnUnsupported, SchemaPolicy, WriteMode};
use rdlt_testkit::draw::{draw, mix};
use rdlt_testkit::drawn::{Scalar, Shape, json, neighbors};

use settings::resolve;
pub use settings::{Level, Relaxed, Resolved};

use crate::rng::SplitMix64;
use crate::swarm::Features;

/// How many phases a simulation runs; the source changes between them.
pub const PHASES: usize = 2;

/// Names drift columns draw from; some fold to one identifier under case-folding rules.
const DRIFT_NAMES: [&str; 6] = ["d0", "D0", "extra", "Extra", "note", "a__b"];

/// More names drift columns draw from where a seed exercises identifiers: characters outside
/// ASCII words, one that grows as it folds to upper case, and one only spaces set apart.
const WIDE_NAMES: [&str; 5] = ["naïve", "日付", "straße", "a b", "Ünï-cöde"];

/// Everything a simulated source serves.
#[derive(Clone, Debug, PartialEq)]
pub struct Workload {
    /// Mixed into every value, so different seeds produce different rows.
    pub salt: u64,
    /// The features the seed exercises.
    pub features: Features,
    /// The pipeline's schema settings.
    pub pipeline: Level,
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
    /// Whether every partition's rows share the same keys, so merges of one key race across
    /// partitions.
    pub shared_keys: bool,
    /// Whether the merge key spans two columns: the key and a text tag.
    pub composite: bool,
    /// The type each partition's batches send the key as in each phase.
    pub key_types: Vec<[LogicalType; PHASES]>,
    /// Columns whose presence and type change across partitions and phases.
    pub drift: Vec<Drift>,
    /// The stream's schema settings.
    pub schema: Level,
    /// The pipeline's schema settings, which the stream's inherit.
    pub pipeline: Level,
    /// Whether the source pushes its rows as JSON rather than Arrow.
    pub json: bool,
    /// Whether the source sends each batch as a slice of a larger one.
    pub sliced: bool,
    /// Whether a JSON stream pushes floats JSON cannot hold, by name; otherwise its floats are
    /// finite, so a column of them keeps its inferred type.
    pub named_floats: bool,
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
    /// The column's own schema settings.
    pub settings: Level,
    /// The type the plan hints for it.
    pub hint: Option<LogicalType>,
    /// The type the source declares it as; undeclared columns are schema changes.
    pub declared: Option<LogicalType>,
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
    /// The merge key's second column, for streams whose key spans two.
    pub tag: Option<String>,
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
        let pipeline = if features.settings {
            Level::draw(rng, false, features.normalize)
        } else {
            Level::default()
        };
        let streams = (0..=rng.below(3))
            .map(|index| SimStream::generate(index, rng, features, salt, pipeline))
            .collect();
        Self {
            salt,
            features,
            pipeline,
            streams,
        }
    }
}

impl SimStream {
    fn generate(
        index: u64,
        rng: &mut SplitMix64,
        features: Features,
        salt: u64,
        pipeline: Level,
    ) -> Self {
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
        let schema = schema::level(rng, features);
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
            shared_keys: false,
            composite: false,
            key_types: Vec::new(),
            schema,
            pipeline,
            json,
            sliced: features.sliced && rng.chance(500),
            named_floats: json && rng.chance(250),
            drift,
            partitions,
            rows: Vec::new(),
        };
        if features.settings {
            stream.draw_columns(rng, features);
        }
        stream.key_types =
            vec![std::array::from_fn(|_| LogicalType::Int64); stream.partitions.len()];
        if write == WriteMode::Merge && features.keys {
            stream.draw_keys(rng);
        }
        stream.rows = (0..stream.partitions.len())
            .map(|partition| stream.draw_rows(salt, partition))
            .collect();
        stream
    }

    /// Whether the stream's arrays land in child tables.
    pub fn normalized(&self) -> bool {
        self.max_depth().is_some()
    }

    /// How deep the stream normalizes, if it does.
    pub fn max_depth(&self) -> Option<u8> {
        match resolve(&[self.schema, self.pipeline]).nested {
            Nested::Normalize { max_depth } => Some(max_depth),
            _ => None,
        }
    }

    /// The settings drift column `column`, or with `None` the stream's other columns, resolve
    /// to, as `relaxed` leaves them.
    pub fn resolved(&self, column: Option<usize>, relaxed: Relaxed) -> Resolved {
        let own = column.map_or_else(Level::default, |column| self.drift[column].settings);
        let mut resolved = resolve(&[own, self.schema, self.pipeline]);
        if relaxed.frozen && resolved.policy == SchemaPolicy::Freeze {
            resolved.policy = SchemaPolicy::Evolve;
        }
        if relaxed.refused && resolved.on_unsupported == OnUnsupported::Refuse {
            resolved.on_unsupported = OnUnsupported::VariantColumn;
        }
        resolved
    }

    /// Whether drift column `column` is stored whole, as one column, in a stream that normalizes:
    /// its settings say how to store nested values, or the plan hints its type.
    pub fn whole(&self, column: usize) -> bool {
        let drift = &self.drift[column];
        matches!(drift.settings.nested, Some(Nested::Native | Nested::Json)) || drift.hint.is_some()
    }

    /// The rows of `partition` the source reads in `phase`: every row of a full read, and an
    /// incremental read's rows new in `phase`.
    pub fn read(&self, partition: usize, phase: usize) -> &[Row] {
        let rows = self.rows(partition, phase);
        &rows[self.start(partition, phase).min(rows.len())..]
    }

    /// The offset `partition`'s read starts from in `phase`: where the last phase's ended, for an
    /// incremental read, and else 0.
    fn start(&self, partition: usize, phase: usize) -> usize {
        match (self.read, phase.checked_sub(1)) {
            (ReadMode::Incremental, Some(last)) => {
                usize::try_from(self.partitions[partition][last]).unwrap_or(usize::MAX)
            }
            _ => 0,
        }
    }

    /// The offsets of the rows each batch of `partition` in `phase` holds, in order: a read
    /// resumes only at a checkpoint, and checkpoints fall between batches, so every run sends the
    /// same batches.
    pub fn batches(&self, partition: usize, phase: usize) -> Vec<Range<usize>> {
        let end = self.rows(partition, phase).len();
        let size = usize::try_from(self.batch_rows).unwrap_or(1);
        (self.start(partition, phase)..end)
            .step_by(size)
            .map(|start| start..(start + size).min(end))
            .collect()
    }

    /// The offsets of the rows whose pushes the engine may shred together with the push holding
    /// row `offset` of `partition` in `phase`: those between the checkpoints around it, as the
    /// engine gathers pushes only up to a checkpoint, and where the stream checkpoints on demand,
    /// wherever the engine asks, every row the phase reads.
    pub fn span(&self, partition: usize, phase: usize, offset: usize) -> Range<usize> {
        let batches = self.batches(partition, phase);
        let every = match self.checkpointing {
            Checkpointing::Natural => usize::try_from(self.checkpoint_every).unwrap_or(1),
            Checkpointing::OnDemand => batches.len().max(1),
        };
        let at = batches
            .iter()
            .position(|batch| batch.contains(&offset))
            .unwrap_or(0);
        let group = &batches[at / every * every..batches.len().min((at / every + 1) * every)];
        match (group.first(), group.last()) {
            (Some(first), Some(last)) => first.start..last.end,
            _ => offset..offset,
        }
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
        let first = if self.shared_keys { 0 } else { index * 16 };
        let key = (self.keys > 0).then(|| first + i64::try_from(offset % self.keys).unwrap_or(0));
        let tag = (self.keys > 0 && self.composite).then(|| format!("t{}", offset % 3));
        let extras = self
            .drift
            .iter()
            .enumerate()
            .map(|(column, drift)| {
                let shape = drift.shapes[partition][delivered].as_ref()?;
                let seed = mix(value ^ (column as u64 + 1));
                let keep = |value: &Scalar| match (self.json, self.named_floats) {
                    (false, _) => values::convergent(value, &shape.logical),
                    (true, named) => named || values::finite(value),
                };
                Some(values::drawn(shape, seed, keep))
            })
            .collect();
        Row {
            id,
            partition: index,
            offset: position,
            value: i64::from_ne_bytes(value.to_ne_bytes()),
            key,
            tag,
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
    if features.identifiers {
        names.extend(WIDE_NAMES);
    }
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
                settings: Level::default(),
                hint: None,
                declared: None,
            }
        })
        .collect()
}
