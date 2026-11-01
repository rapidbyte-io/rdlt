//! Seeded workloads: the streams a simulated source serves and the rows each phase holds.

mod keys;
mod rows;
mod schema;
mod settings;
#[cfg(test)]
mod tests;
mod values;

use rdlt_connector::{Checkpointing, LogicalType, ReadMode};
use rdlt_engine::{Nested, OnUnsupported, SchemaPolicy, WriteMode};
use rdlt_testkit::draw::{draw, mix};
use rdlt_testkit::drawn::{Scalar, Shape, json, neighbors};

use settings::resolve;
pub use settings::{Level, Relaxed, Resolved};

use crate::rng::SplitMix64;
use crate::swarm::{EXCLUDE, Features};

/// How many phases a simulation runs; the source changes between them.
pub const PHASES: usize = 2;

/// Names drift columns draw from; some fold to one identifier under case-folding rules.
const DRIFT_NAMES: [&str; 6] = ["d0", "D0", "extra", "Extra", "note", "a__b"];

/// More names drift columns draw from where a seed exercises identifiers: characters outside
/// ASCII words, one that grows as it folds to upper case, and one only spaces set apart.
const WIDE_NAMES: [&str; 5] = ["naïve", "日付", "straße", "a b", "Ünï-cöde"];

/// More names drift columns draw from where a seed exercises identifiers: names beginning as the
/// engine's metadata columns' do, whose own no source column may ask for, a name its SQL uses to
/// rank merged rows, and one its staging tables use.
const ENGINE_NAMES: [&str; 4] = [
    "_rdlt_seq_",
    "_rdlt_load_epochs",
    "_rdlt_rank",
    "_rdlt_staging__x",
];

/// Names excluded columns draw from: one a metadata column has, which no source column may ask
/// for unless it is excluded.
const EXCLUDED_NAMES: [&str; 3] = ["secret", "blob", "_rdlt_loaded_at"];

/// Everything a simulated source serves.
#[derive(Clone, Debug, PartialEq)]
pub struct Workload {
    /// Mixed into every value, so different seeds produce different rows.
    pub salt: u64,
    /// The features the seed exercises.
    pub features: Features,
    /// The pipeline's schema settings, which every pipeline sharing the destination has.
    pub pipeline: Level,
    /// How many pipelines load the streams into one destination: stream `i` belongs to
    /// pipeline `i % pipelines`.
    pub pipelines: usize,
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
    /// Keys of the wide object of JSON each eighth record carries in its `fat` column, in a
    /// world of fat values and a stream that pushes JSON and does not normalize; 0 otherwise.
    pub fat: u64,
    /// Whether the source sends each batch as a slice of a larger one.
    pub sliced: bool,
    /// Whether a JSON stream pushes floats JSON cannot hold, by name; otherwise its floats are
    /// finite, so a column of them keeps its inferred type.
    pub named_floats: bool,
    /// Whether the source can read again what it acknowledged; one that cannot forgets it.
    ///
    /// Chosen by the stream's place, not drawn, so forgetting leaves a seed's workload as it was.
    pub replayable: bool,
    /// Whether the stream's rows arrive as simulated time passes, so runs follow it for a while.
    pub follows: bool,
    /// Whether its partitions never end, as a log's; a followed stream's others are read again as
    /// they grow.
    ///
    /// Both chosen by the stream's read and place, not drawn, so streaming leaves a seed's
    /// workload as it was.
    pub unbounded: bool,
    /// The columns the plan excludes, none of which lands.
    pub excluded: Excluding,
    /// Each partition's rows in each phase.
    rows: Vec<[Vec<Row>; PHASES]>,
}

/// The columns a stream excludes, which its source sends unless it honours the projection.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Excluding {
    /// The columns; none where the stream excludes none.
    pub columns: Vec<Excluded>,
    /// Whether the source leaves them out, as a source honouring its read's projection does.
    pub honours: bool,
}

/// A column the source sends that the plan excludes.
#[derive(Clone, Debug, PartialEq)]
pub struct Excluded {
    /// The column's name.
    pub name: String,
    /// Its shape, in every batch.
    pub shape: Shape,
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
    /// A workload of no streams, for a world whose source serves changes.
    pub(crate) fn empty(features: Features) -> Self {
        Self {
            salt: 0,
            features,
            pipeline: Level::default(),
            pipelines: 1,
            streams: Vec::new(),
        }
    }

    /// A workload of one to three streams drawn from `rng`, exercising `features`.
    pub fn generate(rng: &mut SplitMix64, features: Features) -> Self {
        let salt = rng.next_u64();
        let pipeline = if features.settings {
            Level::draw(rng, false, features.normalize)
        } else {
            Level::default()
        };
        let streams: Vec<SimStream> = (0..=rng.below(3))
            .map(|index| SimStream::generate(index, rng, features, salt, pipeline))
            .collect();
        let pipelines = if features.shared && streams.len() > 1 {
            2
        } else {
            1
        };
        Self {
            salt,
            features,
            pipeline,
            pipelines,
            streams,
        }
    }
}

/// How many rows each of a stream's partitions holds in each phase: an incremental stream's grow
/// from one phase to the next, a full read's are drawn anew.
fn partition_rows(rng: &mut SplitMix64, read: ReadMode) -> Vec<[u64; PHASES]> {
    (0..=rng.below(4))
        .map(|_| {
            let first = rng.below(40);
            let second = match read {
                ReadMode::Incremental => first + rng.below(20),
                _ => rng.below(40),
            };
            [first, second]
        })
        .collect()
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
        let partitions = partition_rows(rng, read);
        let json = features.json && rng.chance(500);
        let schema = schema::level(rng, features);
        let drift = drift(rng, partitions.len(), features, json);
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
            key_types: vec![std::array::from_fn(|_| LogicalType::Int64); partitions.len()],
            schema,
            pipeline,
            json,
            fat: 0,
            sliced: features.sliced && rng.chance(500),
            named_floats: json && rng.chance(250),
            replayable: !(features.forgetting
                && read == ReadMode::Incremental
                && index.is_multiple_of(2)),
            follows: features.streaming && read == ReadMode::Incremental,
            unbounded: features.streaming
                && read == ReadMode::Incremental
                && index.is_multiple_of(2),
            drift,
            partitions,
            excluded: Excluding::drawn(features, json, mix(salt ^ EXCLUDE ^ index)),
            rows: Vec::new(),
        };
        if features.settings {
            stream.draw_columns(rng, features);
        }
        if write == WriteMode::Merge && features.keys {
            stream.draw_keys(rng);
        }
        if features.fat && stream.json && !stream.normalized() {
            stream.fat = crate::fat::keys(salt, index);
        }
        stream.rows = (0..stream.partitions.len())
            .map(|partition| stream.draw_rows(salt, partition))
            .collect();
        stream
    }

    /// The value excluded column `column` holds in `row`: any its shape holds, and for a JSON
    /// stream one JSON holds as a number.
    pub fn excluded_value(&self, column: usize, row: &Row) -> Scalar {
        let excluded = &self.excluded.columns[column];
        let seed = mix(u64::from_ne_bytes(row.value.to_ne_bytes()) ^ EXCLUDE ^ column as u64);
        values::drawn(&excluded.shape, seed, |value| {
            !self.json || values::finite(value)
        })
    }

    /// The columns the source sends that the plan excludes: none where it honours the
    /// projection.
    pub fn sent_excluded(&self) -> &[Excluded] {
        if self.excluded.honours {
            &[]
        } else {
            &self.excluded.columns
        }
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
}

impl Excluding {
    /// The columns a stream of JSON where `json` says excludes, where `features` exclude any,
    /// drawn from `seed` apart from every other draw: none, or up to two, each of a shape its
    /// pushes hold, and whether its source leaves them out.
    fn drawn(features: Features, json: bool, seed: u64) -> Self {
        let mut rng = SplitMix64::new(seed);
        if !features.exclude || !rng.chance(700) {
            return Self::default();
        }
        let mut names = EXCLUDED_NAMES.to_vec();
        let columns = (0..=rng.below(2))
            .map(|_| {
                let name =
                    names.remove(usize::try_from(rng.below(names.len() as u64)).unwrap_or(0));
                let shape = fresh(&mut rng, features, json);
                Excluded {
                    name: name.to_owned(),
                    shape,
                }
            })
            .collect();
        Self {
            columns,
            honours: rng.chance(500),
        }
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

/// Zero to three drift columns where `features` drift, none otherwise, each present in some
/// partitions and phases with a drawn shape: its first shape, a type the lattice joins it with,
/// or another; for a JSON stream, only types JSON holds.
fn drift(rng: &mut SplitMix64, partitions: usize, features: Features, json: bool) -> Vec<Drift> {
    if !features.drift {
        return Vec::new();
    }
    let mut names: Vec<&str> = DRIFT_NAMES.to_vec();
    if features.identifiers {
        names.extend(WIDE_NAMES);
        names.extend(ENGINE_NAMES);
    }
    (0..rng.below(4))
        .map(|_| {
            let name = names.remove(usize::try_from(rng.below(names.len() as u64)).unwrap_or(0));
            let first = fresh(rng, features, json);
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
                            Some(fresh(rng, features, json))
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

/// A column's shape drawn from `rng`, nested as deep as `features` let it; for a JSON stream,
/// only types JSON holds.
fn fresh(rng: &mut SplitMix64, features: Features, json: bool) -> Shape {
    let seed = rng.next_u64();
    if json {
        draw(&json::shape(features.depth), seed)
    } else {
        let shape = draw(&rdlt_testkit::drawn::values::shape(features.depth), seed);
        values::plain_unless(shape, features.encodings)
    }
}
