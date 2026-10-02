//! The simulated change source: serves a world's change workload in phases, a snapshot then its
//! changes, with faults.

use std::sync::Arc;

use arrow_array::builder::BinaryBuilder;
use arrow_array::{
    ArrayRef, FixedSizeBinaryArray, Int8Array, Int64Array, RecordBatch, StringArray,
};
use rdlt_connector::{
    ChangeOp, Checkpointing, ConnectContext, ConnectorError, ConnectorErrorKind, Cursor, Emitter,
    Field, LogicalType, OP_COLUMN, Partition, PartitionId, PartitionPlan, PartitionState,
    Partitioning, ReadMode, ReadStream, Result, SEQ_COLUMN, SourceConnector, StateEntry,
    StreamName, StreamSpec, StreamState, Streams, TableSchema, UNCHANGED_COLUMN,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::history;
use super::workload::{ChangeStream, Event, Merged};
use crate::world::{FaultPoint, World};

/// The phase a stream reads its changes in.
pub(crate) const CHANGES: u16 = 1;

/// The partition a stream's changes are read in.
pub(crate) const CHANGES_PARTITION: &str = "changes";

/// Configuration of [`SimChangeSource`]: the world to serve.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct SimChangeSourceConfig {
    /// The world's registered name.
    pub world: String,
}

/// Serves every stream of a world's change workload.
#[derive(Debug)]
pub struct SimChangeSource {
    world: Arc<World>,
}

/// Where a partition resumes: a snapshot partition's next key, or the next change's index;
/// `done` once a snapshot partition has read its last key.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    /// The next key or change.
    pub next: u64,
    /// Whether the snapshot partition is read.
    pub done: bool,
}

impl SourceConnector for SimChangeSource {
    const ID: &'static str = "io.rapidbyte.sim.changes";
    const VERSION: &'static str = "0.0.0";
    type Config = SimChangeSourceConfig;

    async fn connect(config: SimChangeSourceConfig, _context: &ConnectContext) -> Result<Self> {
        let world = World::named(&config.world)
            .ok_or_else(|| ConnectorError::config(format!("no world named {}", config.world)))?;
        Ok(Self { world })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        let streams = self.world.changes.streams.iter().enumerate();
        streams.fold(Streams::new(), |streams, (index, stream)| {
            streams.with(Reader {
                index,
                replayable: stream.replayable,
                history: stream.history,
            })
        })
    }
}

/// Reads the change stream at `index`.
struct Reader {
    index: usize,
    /// Whether its source can read again what it acknowledged, which its spec says.
    replayable: bool,
    /// Whether it keeps history, so its rows carry their change time.
    history: bool,
}

impl Reader {
    fn stream<'a>(&self, source: &'a SimChangeSource) -> &'a ChangeStream {
        &source.world.changes.streams[self.index]
    }

    fn name(&self) -> StreamName {
        StreamName::new(format!("c{}", self.index)).expect("a valid stream name")
    }
}

/// The schema every change stream declares.
fn schema() -> TableSchema {
    TableSchema::new(vec![
        Field::new("id", LogicalType::Int64, true),
        Field::new("value", LogicalType::Utf8, true),
        Field::new("n", LogicalType::Int64, true),
    ])
    .expect("the change schema has distinct names")
}

fn partitions(ids: impl IntoIterator<Item = String>) -> Vec<Partition> {
    ids.into_iter()
        .map(|id| Partition::new(PartitionId::parse(id).expect("a valid partition id")))
        .collect()
}

impl ReadStream<SimChangeSource> for Reader {
    type Cursor = Position;

    fn spec(&self) -> StreamSpec {
        // The spec names the stream without the workload, so it stays what the catalog says.
        let spec = StreamSpec::new(self.name());
        let spec = if self.history {
            history::timed_spec(spec, &schema())
        } else {
            spec.with_schema(schema())
        };
        spec.with_primary_key(["id"])
            .with_read_modes([ReadMode::Cdc])
            .with_partitioning(Partitioning::Planned)
            .with_checkpointing(Checkpointing::Natural)
            .with_replayable(self.replayable)
    }

    async fn plan(&self, source: &SimChangeSource, state: &StreamState) -> Result<PartitionPlan> {
        if let Some(fault) = source.world.fault(FaultPoint::Partitions) {
            return Err(fault);
        }
        if state.phase == CHANGES {
            return Ok(PartitionPlan::new(changes()));
        }
        let stream = self.stream(source);
        let snapshot: Vec<String> = (0..stream.partitions)
            .map(|index| format!("s{index}"))
            .collect();
        let read = snapshot.iter().all(|id| {
            let id = PartitionId::parse(id).expect("a valid partition id");
            match state.partitions.get(&id) {
                Some(PartitionState::Done) => true,
                Some(PartitionState::Cursor(cursor)) => cursor
                    .decode::<Position>(1)
                    .is_ok_and(|position| position.done),
                None => false,
            }
        });
        if !read {
            return Ok(PartitionPlan::new(partitions(snapshot)));
        }
        // The changes start after the position the snapshot captured.
        let next = u64::try_from(self.stream(source).captured).unwrap_or(u64::MAX);
        let start = Cursor::encode(1, &Position { next, done: false })?;
        let id = PartitionId::parse(CHANGES_PARTITION).expect("a valid partition id");
        Ok(PartitionPlan::new(changes())
            .phase(CHANGES)
            .start(id, start))
    }

    async fn read(
        &self,
        source: &SimChangeSource,
        partition: &Partition,
        cursor: Position,
        out: &mut Emitter<Position>,
    ) -> Result<()> {
        let (world, stream) = (&source.world, self.stream(source));
        if !stream.replayable {
            let key = (stream.name.clone(), partition.id().to_string());
            let forgotten = world.acknowledged.lock().get(&key).copied();
            // What it acknowledged is gone; a read from before it waits for it to land.
            if forgotten.is_some_and(|forgotten| cursor.next < forgotten) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Transient,
                    "the changes before the acknowledged position are gone",
                ));
            }
        }
        let given = out.resumes().then_some(&cursor);
        world.reports.started(&stream.name, partition.id(), given);
        let reading = Reading {
            world,
            stream,
            partition: partition.id(),
        };
        if partition.id().as_str() == CHANGES_PARTITION {
            return read_changes(reading, cursor, out).await;
        }
        let index = partition
            .id()
            .as_str()
            .strip_prefix('s')
            .and_then(|index| index.parse::<u64>().ok())
            .ok_or_else(|| ConnectorError::data(format!("no partition {}", partition.id())))?;
        read_snapshot(reading, index, cursor, out).await
    }

    async fn committed(
        &self,
        source: &SimChangeSource,
        cursors: &[(PartitionId, Position)],
    ) -> Result<()> {
        let (world, stream) = (&source.world, self.stream(source));
        let name = self.name();
        for (partition, cursor) in cursors {
            world
                .reports
                .hear(&stream.name, partition, cursor, cursor.next)?;
            if !stream.replayable {
                // It hears once the engine's log holds the changes, before they land; the oracle
                // checks they did once the round is over.
                let key = (stream.name.clone(), partition.to_string());
                let mut acknowledged = world.acknowledged.lock();
                let furthest = acknowledged.entry(key).or_default();
                *furthest = (*furthest).max(cursor.next);
                continue;
            }
            let committed = committed_position(world, &name, partition);
            if committed.is_none_or(|committed| cursor.next > committed.next) {
                world.violation(format!(
                    "stream {name} partition {partition}: acknowledged position {} beyond the \
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

/// A read of one partition of a stream, in its world.
#[derive(Clone, Copy)]
struct Reading<'a> {
    world: &'a World,
    stream: &'a ChangeStream,
    partition: &'a PartitionId,
}

impl Reading<'_> {
    /// Sends the checkpoint `position`, which the source remembers it sent.
    async fn checkpoint(self, out: &mut Emitter<Position>, position: &Position) -> Result<()> {
        let reports = &self.world.reports;
        reports.note(&self.stream.name, self.partition, position);
        out.checkpoint(position).await
    }
}

/// Reads the changes the source holds this round, from `cursor`.
async fn read_changes(
    reading: Reading<'_>,
    cursor: Position,
    out: &mut Emitter<Position>,
) -> Result<()> {
    let (world, stream) = (reading.world, reading.stream);
    let batch_rows = usize::try_from(stream.batch_rows).unwrap_or(1);
    let visible = stream.rounds[world.phase().min(stream.rounds.len() - 1)];
    let mut next = usize::try_from(cursor.next).unwrap_or(usize::MAX);
    // Resuming past changes it sends again, the source sends them before any other, and
    // checkpoints where it resumed, so they commit.
    if let Some(replay) = stream.replay.clone().filter(|replay| replay.end <= next) {
        let rows: Vec<Change> = replay
            .map(|index| Change::of(&stream.events[index], index as u64 + 1))
            .collect();
        out.changes(batch(stream, &rows)).await?;
        reading.checkpoint(out, &cursor).await?;
    }
    while next < visible {
        world.latency().await;
        if let Some(fault) = world.fault(FaultPoint::Read) {
            return Err(fault);
        }
        let end = (next + batch_rows).min(visible);
        let rows: Vec<Change> = (next..end)
            .map(|index| Change::of(&stream.events[index], index as u64 + 1))
            .collect();
        out.changes(batch(stream, &rows)).await?;
        next = end;
        let position = Position {
            next: next as u64,
            done: false,
        };
        reading.checkpoint(out, &position).await?;
    }
    // A source reading ahead pushes changes of the next round with no checkpoint after them.
    let ahead = (next + batch_rows).min(stream.events.len());
    if stream.reads_ahead && next < ahead {
        let rows: Vec<Change> = (next..ahead)
            .map(|index| Change::of(&stream.events[index], index as u64 + 1))
            .collect();
        out.changes(batch(stream, &rows)).await?;
    }
    Ok(())
}

/// The changes partition, which never ends.
fn changes() -> Vec<Partition> {
    let id = PartitionId::parse(CHANGES_PARTITION).expect("a valid partition id");
    vec![Partition::new(id).unbounded()]
}

/// Reads snapshot partition `index` from `cursor`: its keys' rows as inserts at the position the
/// snapshot captured.
async fn read_snapshot(
    reading: Reading<'_>,
    index: u64,
    cursor: Position,
    out: &mut Emitter<Position>,
) -> Result<()> {
    let (world, stream) = (reading.world, reading.stream);
    let batch_rows = usize::try_from(stream.batch_rows).unwrap_or(1);
    let keys: Vec<(i64, Merged)> = stream
        .snapshot()
        .into_iter()
        .filter(|(key, _)| key.unsigned_abs() % stream.partitions == index)
        .collect();
    let captured = u64::try_from(stream.captured).unwrap_or(u64::MAX);
    let mut next = usize::try_from(cursor.next).unwrap_or(usize::MAX);
    while next < keys.len() {
        world.latency().await;
        if let Some(fault) = world.fault(FaultPoint::Read) {
            return Err(fault);
        }
        let end = (next + batch_rows).min(keys.len());
        let rows: Vec<Change> = keys[next..end]
            .iter()
            .map(|(key, row)| Change {
                op: ChangeOp::Insert,
                seq: captured,
                key: Some(*key),
                value: row.value.clone(),
                n: Some(row.n),
                partial: false,
            })
            .collect();
        out.changes(batch(stream, &rows)).await?;
        next = end;
        let done = next >= keys.len();
        let position = Position {
            next: next as u64,
            done,
        };
        reading.checkpoint(out, &position).await?;
    }
    if cursor == Position::default() && keys.is_empty() {
        let position = Position {
            next: 0,
            done: true,
        };
        reading.checkpoint(out, &position).await?;
    }
    Ok(())
}

/// The committed position of `partition` of `stream`, in any pipeline's state.
fn committed_position(
    world: &World,
    stream: &StreamName,
    partition: &PartitionId,
) -> Option<Position> {
    let store = world.store.lock();
    store
        .states()
        .flat_map(|records| records.values())
        .filter_map(|record| StateEntry::from_record(record).ok())
        .filter_map(|entry| match entry {
            StateEntry::Partition {
                stream: named,
                partition: id,
                state: PartitionState::Cursor(cursor),
            } if &named == stream && &id == partition => cursor.decode::<Position>(1).ok(),
            _ => None,
        })
        .max_by_key(|position| position.next)
}

/// One change row as the source pushes it.
struct Change {
    op: ChangeOp,
    seq: u64,
    key: Option<i64>,
    value: Option<String>,
    n: Option<i64>,
    partial: bool,
}

impl Change {
    fn of(event: &Event, seq: u64) -> Self {
        Self {
            op: event.op,
            seq,
            key: event.key,
            value: event.value.clone(),
            n: event.n,
            partial: event.partial,
        }
    }
}

/// `rows` of `stream` as a change batch: `id`, `value` and `n`, with a history stream's change
/// time, then the op, sequence and unchanged columns, a partial row flagging `value`, field 1.
fn batch(stream: &ChangeStream, rows: &[Change]) -> RecordBatch {
    let ids: Int64Array = rows.iter().map(|row| row.key).collect();
    let values: StringArray = rows.iter().map(|row| row.value.clone()).collect();
    let n: Int64Array = rows.iter().map(|row| row.n).collect();
    let ops = Int8Array::from_iter_values(rows.iter().map(|row| row.op.code()));
    let seqs = FixedSizeBinaryArray::try_from_iter(rows.iter().map(|row| {
        let mut bytes = [0_u8; 16];
        bytes[8..].copy_from_slice(&row.seq.to_be_bytes());
        bytes
    }))
    .expect("sequences are 16 bytes");
    let mut unchanged = BinaryBuilder::new();
    for row in rows {
        if row.partial {
            unchanged.append_value([0b10]);
        } else {
            unchanged.append_null();
        }
    }
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(ids) as ArrayRef),
        ("value", Arc::new(values) as ArrayRef),
        ("n", Arc::new(n) as ArrayRef),
        (OP_COLUMN, Arc::new(ops) as ArrayRef),
        (SEQ_COLUMN, Arc::new(seqs) as ArrayRef),
        (UNCHANGED_COLUMN, Arc::new(unchanged.finish()) as ArrayRef),
    ])
    .expect("a valid change batch");
    if stream.history {
        history::timed(&batch)
    } else {
        batch
    }
}
