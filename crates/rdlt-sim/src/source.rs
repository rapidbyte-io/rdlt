//! The simulated source: serves the world's workload, with faults.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use rdlt_connector::{
    Checkpointing, ConnectContext, ConnectorError, Emitter, Field, LogicalType, Partition,
    PartitionId, Partitioning, ReadMode, ReadStream, Result, SourceConnector, StreamName,
    StreamSpec, StreamState, Streams, TableSchema,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::destination::committed_next;
use crate::workload::{Row, SimStream};
use crate::world::{FaultPoint, World};

/// Configuration of [`SimSource`]: the world to serve.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct SimSourceConfig {
    /// The world's registered name.
    pub world: String,
}

/// Serves every stream of a world's workload.
#[derive(Debug)]
pub struct SimSource {
    world: Arc<World>,
}

/// Where a simulated partition resumes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimCursor {
    /// The offset of the next row to read.
    pub next: u64,
}

impl SourceConnector for SimSource {
    const ID: &'static str = "io.rapidbyte.sim";
    const VERSION: &'static str = "0.0.0";
    type Config = SimSourceConfig;

    async fn connect(config: SimSourceConfig, _context: &ConnectContext) -> Result<Self> {
        let world = World::named(&config.world)
            .ok_or_else(|| ConnectorError::config(format!("no world named {}", config.world)))?;
        Ok(Self { world })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        let streams = self.world.workload.streams.iter().enumerate();
        streams.fold(Streams::new(), |streams, (index, stream)| {
            streams.with(SimStreamReader {
                index,
                checkpointing: stream.checkpointing,
            })
        })
    }
}

/// The schema every simulated stream declares.
#[expect(
    clippy::missing_panics_doc,
    reason = "four distinct column names always make a schema"
)]
pub fn schema() -> TableSchema {
    let column = |name| Field::new(name, LogicalType::Int64, false);
    TableSchema::new(vec![
        column("id"),
        column("partition"),
        column("offset"),
        column("value"),
    ])
    .expect("the simulated schema has distinct names")
}

struct SimStreamReader {
    index: usize,
    checkpointing: Checkpointing,
}

impl SimStreamReader {
    fn stream<'a>(&self, source: &'a SimSource) -> &'a SimStream {
        &source.world.workload.streams[self.index]
    }
}

impl ReadStream<SimSource> for SimStreamReader {
    type Cursor = SimCursor;

    fn spec(&self) -> StreamSpec {
        let name = format!("s{}", self.index);
        StreamSpec::new(StreamName::new(name).expect("simulated stream names are valid"))
            .with_schema(schema())
            .with_read_modes([ReadMode::Full, ReadMode::Incremental])
            .with_partitioning(Partitioning::Planned)
            .with_checkpointing(self.checkpointing)
    }

    async fn partitions(&self, source: &SimSource, _state: &StreamState) -> Result<Vec<Partition>> {
        let count = self.stream(source).partitions.len();
        Ok((0..count)
            .map(|index| {
                Partition::new(PartitionId::parse(format!("p{index}")).expect("valid partition id"))
            })
            .collect())
    }

    async fn read(
        &self,
        source: &SimSource,
        partition: &Partition,
        cursor: SimCursor,
        out: &mut Emitter<SimCursor>,
    ) -> Result<()> {
        let world = &source.world;
        let stream = self.stream(source);
        let index = partition_index(partition)?;
        let rows = stream.rows(world.workload.salt, index, world.phase());
        let mut next = usize::try_from(cursor.next).unwrap_or(usize::MAX);
        let mut batches = 0;
        while next < rows.len() {
            world.latency().await;
            if let Some(fault) = world.fault(FaultPoint::Read) {
                return Err(fault);
            }
            let end = (next + usize::try_from(stream.batch_rows).unwrap_or(1)).min(rows.len());
            out.batch(batch(&rows[next..end])).await?;
            next = end;
            batches += 1;
            let due = match stream.checkpointing {
                Checkpointing::OnDemand => out.checkpoint_due(),
                Checkpointing::Natural => batches % stream.checkpoint_every == 0,
            };
            if due {
                out.checkpoint(&SimCursor { next: next as u64 }).await?;
            }
        }
        if stream.final_checkpoint {
            out.checkpoint(&SimCursor { next: next as u64 }).await?;
        }
        Ok(())
    }

    async fn committed(
        &self,
        source: &SimSource,
        cursors: &[(PartitionId, SimCursor)],
    ) -> Result<()> {
        let world = &source.world;
        let name = StreamName::new(&self.stream(source).name).expect("valid stream name");
        for (partition, cursor) in cursors {
            let committed = committed_next(world, &name, partition);
            if committed.is_none_or(|committed| cursor.next > committed) {
                world.violation(format!(
                    "stream {name} partition {partition}: acknowledged offset {} beyond the \
                     committed {committed:?}",
                    cursor.next
                ));
            }
        }
        match world.fault(FaultPoint::Acknowledge) {
            Some(fault) => Err(fault),
            None => Ok(()),
        }
    }
}

fn partition_index(partition: &Partition) -> Result<usize> {
    partition
        .id()
        .as_str()
        .strip_prefix('p')
        .and_then(|index| index.parse().ok())
        .ok_or_else(|| ConnectorError::data(format!("no partition {}", partition.id())))
}

fn batch(rows: &[Row]) -> RecordBatch {
    let column =
        |value: fn(&Row) -> i64| Arc::new(Int64Array::from_iter_values(rows.iter().map(value)));
    RecordBatch::try_from_iter([
        ("id", column(|row| row.id) as _),
        ("partition", column(|row| row.partition) as _),
        ("offset", column(|row| row.offset) as _),
        ("value", column(|row| row.value) as _),
    ])
    .expect("four equal-length columns make a batch")
}
