//! The simulated source: serves the world's workload, with faults.

mod push;

use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{
    Checkpointing, ConnectContext, ConnectorError, ConnectorErrorKind, Emitter, Field, LogicalType,
    Partition, PartitionId, Partitioning, ReadMode, ReadStream, Result, SourceConnector,
    StreamName, StreamSpec, StreamState, Streams, TableSchema,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use push::{batch, json_push};

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
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimCursor {
    /// The offset of the next row to read.
    pub next: u64,
    /// What the cursor carries beside its offset, so cursors press on the engine's budget.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub pad: String,
}

/// The cursor at `next` of a source pressing as `world` says.
fn at(world: &World, next: usize) -> SimCursor {
    SimCursor {
        next: next as u64,
        pad: "p".repeat(world.pressure().pad),
    }
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
                schema: schema(stream),
                key: (stream.keys > 0 && !stream.plan_key).then(|| stream.key_columns()),
                replayable: stream.replayable,
            })
        })
    }
}

/// The schema a simulated stream declares: its base columns, the key of a merge stream, and the
/// drift columns it declares.
///
/// Other drift columns are left out, so they are schema changes the pipeline's policy handles.
#[expect(
    clippy::missing_panics_doc,
    reason = "distinct column names always make a schema"
)]
pub fn schema(stream: &SimStream) -> TableSchema {
    let column = |name| Field::new(name, LogicalType::Int64, false);
    let mut columns = vec![
        column("id"),
        column("partition"),
        column("offset"),
        column("value"),
    ];
    if stream.keys > 0 {
        columns.push(column("key"));
    }
    if stream.keys > 0 && stream.composite {
        columns.push(Field::new("tag", LogicalType::Utf8, false));
    }
    for drift in &stream.drift {
        if let Some(declared) = &drift.declared {
            columns.push(Field::new(drift.name.as_str(), declared.clone(), true));
        }
    }
    TableSchema::new(columns).expect("the simulated schema has distinct names")
}

struct SimStreamReader {
    index: usize,
    checkpointing: Checkpointing,
    schema: TableSchema,
    /// The primary key the catalog names.
    key: Option<Vec<&'static str>>,
    replayable: bool,
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
        let spec =
            StreamSpec::new(StreamName::new(name).expect("simulated stream names are valid"))
                .with_schema(self.schema.clone())
                .with_read_modes([ReadMode::Full, ReadMode::Incremental])
                .with_partitioning(Partitioning::Planned)
                .with_checkpointing(self.checkpointing)
                .with_replayable(self.replayable);
        match &self.key {
            Some(key) => spec.with_primary_key(key.iter().copied()),
            None => spec,
        }
    }

    async fn partitions(&self, source: &SimSource, _state: &StreamState) -> Result<Vec<Partition>> {
        if let Some(fault) = source.world.fault(FaultPoint::Partitions) {
            return Err(fault);
        }
        let stream = self.stream(source);
        Ok((0..stream.partitions.len())
            .map(|index| {
                let id = PartitionId::parse(format!("p{index}")).expect("valid partition id");
                if stream.unbounded {
                    Partition::new(id).unbounded()
                } else {
                    Partition::new(id)
                }
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
        if !stream.replayable {
            let key = (stream.name.clone(), partition.id().to_string());
            let forgotten = world.acknowledged.lock().get(&key).copied();
            // Rows it acknowledged are gone; a read from before them waits for them to land.
            if forgotten.is_some_and(|forgotten| cursor.next < forgotten) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Transient,
                    "the rows before the acknowledged offset are gone",
                ));
            }
        }
        let given = out.resumes().then_some(&cursor);
        world.reports.started(&stream.name, partition.id(), given);
        let rows = stream.rows(index, world.phase());
        let mut next = usize::try_from(cursor.next).unwrap_or(usize::MAX);
        let started = next;
        let mut batches = 0;
        loop {
            let arrived = world.available(self.index, index, rows.len());
            let available = stream.servable(index, world.phase(), arrived);
            let serving = Serving {
                partition: index,
                available,
            };
            self.serve(source, out, rows, &mut next, serving, &mut batches)
                .await?;
            // A read that follows a partition that never ends waits for its next rows.
            if !(out.follows() && partition.is_unbounded()) {
                break;
            }
            self.checkpoint(source, out, index, next).await?;
            // As a log does, it says how far behind its newest rows it is; and it asks for the
            // stream to be planned again, which, its partitions unchanged, must change nothing.
            out.behind(arrived.saturating_sub(next) as u64).await?;
            out.replan().await?;
            if !self.arrival(source, out, index, rows.len(), next).await? {
                return Ok(());
            }
        }
        // A read that sent nothing says nothing of where it is, at every other position: its
        // host then reports the cursor it read from, which the source never sent.
        let silent = next == started && (index + started).is_multiple_of(2);
        if stream.final_checkpoint && !silent {
            self.checkpoint(source, out, index, next).await?;
        }
        Ok(())
    }

    async fn committed(
        &self,
        source: &SimSource,
        cursors: &[(PartitionId, SimCursor)],
    ) -> Result<()> {
        committed(self.stream(source), &source.world, cursors)
    }
}

/// What a read serves next: of which partition, and up to which of its rows.
#[derive(Clone, Copy)]
struct Serving {
    partition: usize,
    available: usize,
}

impl SimStreamReader {
    /// Sends the checkpoint at row `next` of partition `partition`, which the source remembers
    /// it sent.
    async fn checkpoint(
        &self,
        source: &SimSource,
        out: &mut Emitter<SimCursor>,
        partition: usize,
        next: usize,
    ) -> Result<()> {
        let cursor = at(&source.world, next);
        let id = PartitionId::parse(format!("p{partition}")).expect("valid partition id");
        let stream = self.stream(source);
        source.world.reports.note(&stream.name, &id, &cursor);
        out.checkpoint(&cursor).await
    }

    /// Serves `rows` from `next` up to what `serving` says, counting `batches`.
    async fn serve(
        &self,
        source: &SimSource,
        out: &mut Emitter<SimCursor>,
        rows: &[Row],
        next: &mut usize,
        serving: Serving,
        batches: &mut u64,
    ) -> Result<()> {
        let available = serving.available;
        let world = &source.world;
        let stream = self.stream(source);
        while *next < available {
            world.latency().await;
            if let Some(fault) = world.fault(FaultPoint::Read) {
                return Err(fault);
            }
            let end = (*next + usize::try_from(stream.batch_rows).unwrap_or(1)).min(available);
            if stream.json {
                out.json(json_push(stream, &rows[*next..end], *batches % 2 == 1))
                    .await?;
            } else {
                let ballast = world.pressure().ballast;
                out.batch(batch(stream, &rows[*next..end], ballast)).await?;
            }
            *next = end;
            *batches += 1;
            let due = match stream.checkpointing {
                Checkpointing::OnDemand => out.checkpoint_due(),
                Checkpointing::Natural => (*batches).is_multiple_of(stream.checkpoint_every),
            };
            if due {
                self.checkpoint(source, out, serving.partition, *next)
                    .await?;
            }
        }
        Ok(())
    }

    /// Waits until more of partition `partition`'s `rows` rows than `next` have arrived: false
    /// once the read is asked to stop instead.
    ///
    /// It wakes now and then to answer a barrier, as a source waiting for data does.
    async fn arrival(
        &self,
        source: &SimSource,
        out: &mut Emitter<SimCursor>,
        partition: usize,
        rows: usize,
        next: usize,
    ) -> Result<bool> {
        let world = &source.world;
        loop {
            let arrived = world.arrived.notified();
            tokio::pin!(arrived);
            arrived.as_mut().enable();
            let count = world.available(self.index, partition, rows);
            let stream = self.stream(source);
            if stream.servable(partition, world.phase(), count) > next {
                return Ok(true);
            }
            tokio::select! {
                biased;
                () = out.stopped() => return Ok(false),
                () = &mut arrived => {}
                () = tokio::time::sleep(WAKE) => {
                    if out.checkpoint_due() {
                        self.checkpoint(source, out, partition, next).await?;
                    }
                }
            }
        }
    }
}

/// How long a read waiting for rows sleeps before it looks for a barrier to answer.
const WAKE: Duration = Duration::from_millis(50);

/// Hears `stream`'s `cursors` are committed, in `world`.
fn committed(
    stream: &SimStream,
    world: &World,
    cursors: &[(PartitionId, SimCursor)],
) -> Result<()> {
    let name = StreamName::new(&stream.name).expect("valid stream name");
    for (partition, cursor) in cursors {
        world
            .reports
            .hear(&stream.name, partition, cursor, cursor.next)?;
        if !stream.replayable {
            // It hears once the engine's log holds the rows, before they land; the oracle
            // checks they did once the phase is over.
            let key = (stream.name.clone(), partition.to_string());
            let mut acknowledged = world.acknowledged.lock();
            let furthest = acknowledged.entry(key).or_default();
            *furthest = (*furthest).max(cursor.next);
            continue;
        }
        let committed = committed_next(world, &name, partition);
        let reset = world.reset.lock().contains(&stream.name);
        if !reset && committed.is_none_or(|committed| cursor.next > committed) {
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

fn partition_index(partition: &Partition) -> Result<usize> {
    partition
        .id()
        .as_str()
        .strip_prefix('p')
        .and_then(|index| index.parse().ok())
        .ok_or_else(|| ConnectorError::data(format!("no partition {}", partition.id())))
}
