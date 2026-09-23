//! A source that generates seeded, partitioned Arrow data.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use rdlt_connector::prelude::*;
use rdlt_connector::{Field, Partitioning};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Configuration of [`GeneratorSource`].
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GeneratorConfig {
    /// Every value derives from this seed.
    pub seed: u64,
    /// The streams to generate.
    pub streams: Vec<GeneratedStream>,
}

/// One generated stream.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GeneratedStream {
    /// The stream's name.
    pub name: String,
    /// Rows in the stream.
    pub rows: u64,
    /// Partitions the rows are split across; row `i` belongs to partition `i % partitions`.
    #[serde(default = "one")]
    pub partitions: u64,
    /// Rows per pushed batch; a checkpoint follows each batch.
    #[serde(default = "hundred")]
    pub batch_rows: u64,
}

fn one() -> u64 {
    1
}

fn hundred() -> u64 {
    100
}

/// Generates rows `(id, value, name)` where `value` is a seeded hash of `id`.
///
/// Partitioned, with on-demand checkpoints after every batch; the same seed always yields the
/// same data.
#[derive(Debug)]
pub struct GeneratorSource {
    seed: u64,
    streams: Vec<GeneratedStream>,
}

#[source(id = "io.rapidbyte.generator")]
impl SourceConnector for GeneratorSource {
    type Config = GeneratorConfig;

    async fn connect(config: GeneratorConfig, _context: &ConnectContext) -> Result<Self> {
        for stream in &config.streams {
            StreamName::new(&stream.name).config(format!("stream name {:?}", stream.name))?;
            if stream.partitions == 0 || stream.batch_rows == 0 {
                return Err(ConnectorError::config(format!(
                    "stream {}: partitions and batch_rows must be at least 1",
                    stream.name
                )));
            }
        }
        Ok(Self {
            seed: config.seed,
            streams: config.streams,
        })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        self.streams.iter().fold(Streams::new(), |streams, stream| {
            streams.with(Generated(stream.clone()))
        })
    }
}

struct Generated(GeneratedStream);

/// The next row id this partition emits; `None` before the first batch.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
struct NextRow {
    next: Option<u64>,
}

impl Generated {
    fn partition_index(&self, partition: &Partition) -> Result<u64> {
        partition
            .id()
            .as_str()
            .parse::<u64>()
            .ok()
            .filter(|index| *index < self.0.partitions)
            .ok_or_else(|| {
                ConnectorError::data(format!(
                    "stream {} has no partition {}",
                    self.0.name,
                    partition.id()
                ))
            })
    }
}

impl ReadStream<GeneratorSource> for Generated {
    type Cursor = NextRow;

    fn spec(&self) -> StreamSpec {
        let schema = TableSchema::new(vec![
            Field::new("id", LogicalType::Int64, false),
            Field::new("value", LogicalType::Int64, false),
            Field::new("name", LogicalType::Utf8, false),
        ])
        .expect("the generated schema has distinct field names");
        StreamSpec::new(StreamName::new(&self.0.name).expect("connect validated stream names"))
            .with_schema(schema)
            .with_primary_key(["id"])
            .with_partitioning(Partitioning::Planned)
            .with_checkpointing(Checkpointing::OnDemand)
    }

    async fn partitions(
        &self,
        _source: &GeneratorSource,
        _state: &StreamState,
    ) -> Result<Vec<Partition>> {
        (0..self.0.partitions)
            .map(|index| {
                PartitionId::parse(index.to_string())
                    .map(Partition::new)
                    .internal("partition id")
            })
            .collect()
    }

    async fn read(
        &self,
        source: &GeneratorSource,
        partition: &Partition,
        cursor: NextRow,
        out: &mut Emitter<NextRow>,
    ) -> Result<()> {
        let index = self.partition_index(partition)?;
        let stride = self.0.partitions;
        let mut next = cursor.next.unwrap_or(index);
        while next < self.0.rows {
            let ids: Vec<u64> = (next..self.0.rows)
                .step_by(usize::try_from(stride).unwrap_or(usize::MAX))
                .take(usize::try_from(self.0.batch_rows).unwrap_or(usize::MAX))
                .collect();
            next = ids.last().map_or(self.0.rows, |last| last + stride);
            out.batch(batch(source.seed, &ids)?).await?;
            out.checkpoint(&NextRow { next: Some(next) }).await?;
        }
        Ok(())
    }
}

fn batch(seed: u64, ids: &[u64]) -> Result<RecordBatch> {
    let as_i64 = |value: u64| i64::from_ne_bytes(value.to_ne_bytes());
    let id = Int64Array::from_iter_values(ids.iter().map(|id| as_i64(*id)));
    let value = Int64Array::from_iter_values(ids.iter().map(|id| as_i64(mix(seed ^ id))));
    let name = StringArray::from_iter_values(ids.iter().map(|id| format!("row-{id}")));
    RecordBatch::try_from_iter([
        ("id", Arc::new(id) as _),
        ("value", Arc::new(value) as _),
        ("name", Arc::new(name) as _),
    ])
    .internal("building a generated batch")
}

/// The `SplitMix64` output function: a fast, well-distributed hash of `x`.
fn mix(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}
