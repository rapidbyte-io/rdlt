//! A source of seeded change streams: a snapshot of a keyed table, then the changes made to it.

mod bounds;
mod history;
mod model;
mod slot;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::builder::BinaryBuilder;
use arrow_array::{
    ArrayRef, FixedSizeBinaryArray, Int8Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use rdlt_connector::prelude::*;
use rdlt_connector::{
    ChangeOp, Field, OP_COLUMN, PartitionPlan, PartitionState, Partitioning, SEQ_COLUMN,
    UNCHANGED_COLUMN,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub use history::{Version, history};
pub use model::{Change, Row, change, expected, snapshot};
use slot::Slot;

use crate::positions::{keeper_name, keeper_path, unnamed};

/// Configuration of [`ChangesSource`].
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ChangesConfig {
    /// Every change derives from this seed.
    pub seed: u64,
    /// The streams.
    pub streams: Vec<ChangedStream>,
    /// The slot the source keeps its acknowledged positions in, by name, which every source of
    /// this process naming it shares, as a replication slot outlives a connection; the default
    /// slot where none is named.
    #[serde(default)]
    pub slot: Option<String>,
    /// The file the slot is kept in instead, which outlives the process as a replication slot
    /// outlives its clients; `slot` names none then.
    ///
    /// A path to a file named `*.slot`.
    #[serde(default)]
    pub slot_path: Option<std::path::PathBuf>,
}

/// One change stream: a table of `keys` rows, snapshotted, then changed `changes` times.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ChangedStream {
    /// The stream's name.
    pub name: String,
    /// The keys the table holds when the snapshot is taken: `0..keys`, at most a million.
    pub keys: u64,
    /// Partitions the snapshot is read in, at most 1024; key `k` belongs to partition
    /// `k % snapshot_partitions`.
    #[serde(default = "one")]
    pub snapshot_partitions: u64,
    /// Changes after the snapshot, at positions `1..=changes`.
    pub changes: u64,
    /// Rows per pushed batch; a checkpoint follows each batch.
    #[serde(default = "ten")]
    pub batch_rows: u64,
    /// The positions of changes that truncate the table, at most 1024.
    #[serde(default)]
    pub truncates: Vec<u64>,
    /// How many changes the snapshot holds: it is taken at position `captured`, its rows carry
    /// that position, and the changes after it are read; at most a million.
    #[serde(default)]
    pub captured: u64,
    /// Whether the source serves again the changes its slot acknowledged; one that does not
    /// forgets them, as a replication slot does, and a read from before them waits; its source
    /// names its `slot` or `slot_path`.
    #[serde(default = "yes")]
    pub replayable: bool,
    /// Whether each row carries when its change happened in `changed_at`, a timestamp of its
    /// position in microseconds, which the stream names its change time.
    #[serde(default)]
    pub changed_at: bool,
    /// Whether an update may leave `value` unchanged; without, every update sets it, as a stream
    /// kept as history needs.
    #[serde(default = "yes")]
    pub partial: bool,
}

fn yes() -> bool {
    true
}

fn one() -> u64 {
    1
}

fn ten() -> u64 {
    10
}

/// The phase a stream reads its snapshot in.
pub const SNAPSHOT: u16 = 0;

/// The phase a stream reads its changes in.
pub const CHANGES: u16 = 1;

/// The column a timed stream's rows say when their change happened in.
const CHANGED_AT: &str = "changed_at";

/// The id of the partition changes are read in.
const CHANGES_PARTITION: &str = "changes";

/// Reads each stream as a CDC source does: a snapshot of rows `(id, value, n)` at position 0, in
/// partitions, then its changes, one partition, each at its own position.
///
/// Change `i` upserts, partly updates (leaving `value` unchanged), or deletes a key the seed
/// picks, or truncates the table when `i` is among the stream's truncates. The same seed always
/// yields the same changes; [`expected`] is the table they leave.
#[derive(Debug)]
pub struct ChangesSource {
    seed: u64,
    streams: Vec<ChangedStream>,
    slot: Arc<Slot>,
}

#[source(id = "io.rapidbyte.changes", acknowledged)]
impl SourceConnector for ChangesSource {
    type Config = ChangesConfig;

    async fn connect(config: ChangesConfig, _context: &ConnectContext) -> Result<Self> {
        for stream in &config.streams {
            StreamName::new(&stream.name).config(format!("stream name {:?}", stream.name))?;
            stream.bounded()?;
        }
        keeper_name(config.slot.as_deref(), "slot")?;
        if let Some(path) = &config.slot_path {
            keeper_path(path, "slot")?;
        }
        let shared = config.slot.is_none() && config.slot_path.is_none();
        if let Some(forgets) = config.streams.iter().find(|stream| !stream.replayable)
            && shared
        {
            return Err(unnamed(&forgets.name, "slot"));
        }
        Ok(Self {
            seed: config.seed,
            streams: config.streams,
            slot: match &config.slot_path {
                Some(path) => slot::at(path).config(format!("slot {}", path.display()))?,
                None => slot::named(config.slot.as_deref()),
            },
        })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        self.streams.iter().fold(Streams::new(), |streams, stream| {
            streams.with(Changed(stream.clone()))
        })
    }
}

struct Changed(ChangedStream);

/// Where a partition resumes: the next key of a snapshot partition, or the next change; `done`
/// once a snapshot partition has read its last key.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct Position {
    next: u64,
    done: bool,
}

impl Changed {
    fn snapshot_ids(&self) -> Vec<String> {
        (0..self.0.snapshot_partitions)
            .map(|index| format!("snapshot-{index}"))
            .collect()
    }

    /// The changes partition, which never ends.
    fn changes() -> Result<Vec<Partition>> {
        let id = PartitionId::parse(CHANGES_PARTITION).internal("partition id")?;
        Ok(vec![Partition::new(id).unbounded()])
    }

    fn partitions(ids: Vec<String>) -> Result<Vec<Partition>> {
        ids.into_iter()
            .map(|id| {
                PartitionId::parse(id)
                    .map(Partition::new)
                    .internal("partition id")
            })
            .collect()
    }
}

impl ReadStream<ChangesSource> for Changed {
    type Cursor = Position;

    fn spec(&self) -> StreamSpec {
        let mut fields = vec![
            Field::new("id", LogicalType::Int64, true),
            Field::new("value", LogicalType::Utf8, true),
            Field::new("n", LogicalType::Int64, true),
        ];
        if self.0.changed_at {
            let micros =
                LogicalType::Timestamp(rdlt_connector::TimeUnit::Microsecond, Some("UTC".into()));
            fields.push(Field::new(CHANGED_AT, micros, true));
        }
        let schema = TableSchema::new(fields).expect("the change schema has distinct field names");
        let spec =
            StreamSpec::new(StreamName::new(&self.0.name).expect("connect validated stream names"));
        let spec = if self.0.changed_at {
            spec.with_change_time(CHANGED_AT)
        } else {
            spec
        };
        spec.with_schema(schema)
            .with_primary_key(["id"])
            .with_read_modes([ReadMode::Cdc])
            .with_partitioning(Partitioning::Planned)
            .with_checkpointing(Checkpointing::Natural)
            .with_replayable(self.0.replayable)
    }

    async fn plan(&self, _source: &ChangesSource, state: &StreamState) -> Result<PartitionPlan> {
        if state.phase == CHANGES {
            return Ok(PartitionPlan::new(Self::changes()?));
        }
        let snapshot = self.snapshot_ids();
        let finished = snapshot.iter().all(|id| {
            let position = PartitionId::parse(id)
                .ok()
                .and_then(|id| state.partitions.get(&id).cloned());
            matches!(position, Some(PartitionState::Done))
                || matches!(
                    position,
                    Some(PartitionState::Cursor(cursor))
                        if cursor.decode::<Position>(1).is_ok_and(|position| position.done)
                )
        });
        if !finished {
            return Ok(PartitionPlan::new(Self::partitions(snapshot)?));
        }
        // The changes start after the position the snapshot captured.
        let start = Cursor::encode(
            1,
            &Position {
                next: self.0.captured.saturating_add(1),
                done: false,
            },
        )?;
        let id = PartitionId::parse(CHANGES_PARTITION).internal("partition id")?;
        Ok(PartitionPlan::new(Self::changes()?)
            .phase(CHANGES)
            .start(id, start))
    }

    /// Acknowledges each partition's position in the source's slot.
    async fn committed(
        &self,
        source: &ChangesSource,
        cursors: &[(PartitionId, Position)],
    ) -> Result<()> {
        for (partition, _) in cursors {
            self.0.member(partition)?;
        }
        for (partition, position) in cursors {
            let kept = source.slot.advance(&self.0.name, partition, *position);
            kept.transient("keeping the slot")?;
        }
        Ok(())
    }

    async fn acknowledged(
        &self,
        source: &ChangesSource,
        partition: &PartitionId,
    ) -> Result<Option<Position>> {
        Ok(source.slot.position(&self.0.name, partition))
    }

    async fn read(
        &self,
        source: &ChangesSource,
        partition: &Partition,
        cursor: Position,
        out: &mut Emitter<Position>,
    ) -> Result<()> {
        let id = partition.id().as_str();
        if id == CHANGES_PARTITION {
            let forgotten = source.slot.position(&self.0.name, partition.id());
            // The changes it acknowledged are gone; a read from before them waits for them to land.
            if !self.0.replayable && forgotten.is_some_and(|forgotten| cursor.next < forgotten.next)
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Transient,
                    "the changes before the acknowledged position are gone",
                ));
            }
            // The changes' positions are counted from one, and none of them is done.
            if cursor.done || cursor.next > self.0.changes.saturating_add(1) {
                return Err(unissued(&self.0.name, id, cursor));
            }
            return self.read_changes(source.seed, cursor, out).await;
        }
        let index = self
            .0
            .snapshot_index(id)
            .ok_or_else(|| self.0.no_partition(id))?;
        self.read_snapshot(source.seed, id, index, cursor, out)
            .await
    }
}

impl Changed {
    /// Reads the keys of snapshot partition `id`, the `index`th, from `cursor`.
    async fn read_snapshot(
        &self,
        seed: u64,
        id: &str,
        index: u64,
        cursor: Position,
        out: &mut Emitter<Position>,
    ) -> Result<()> {
        let stride = self.0.snapshot_partitions;
        // The partition's rows, by key; the cursor counts those read.
        let rows: Vec<(i64, Row)> = snapshot(seed, &self.0)
            .into_iter()
            .filter(|(id, _)| id.unsigned_abs() % stride == index)
            .collect();
        let total = u64::try_from(rows.len()).unwrap_or(u64::MAX);
        // A snapshot's cursor counts the rows read, and is done exactly at the last of them.
        let done = cursor.next >= total && (total > 0 || cursor.done);
        if cursor.next > total || cursor.done != done {
            return Err(unissued(&self.0.name, id, cursor));
        }
        let mut next = cursor.next;
        while next < total {
            let last = next.saturating_add(self.0.batch_rows).min(total);
            let batch = rows[to_index(next)..to_index(last)]
                .iter()
                .map(|(id, row)| {
                    let (value, n) = (row.value.clone(), Some(row.n));
                    (
                        ChangeOp::Insert,
                        self.0.captured,
                        Some(*id),
                        value,
                        n,
                        false,
                    )
                })
                .collect::<Vec<_>>();
            out.changes(changes_batch(&batch, self.0.changed_at)?)
                .await?;
            next = last;
            let done = next >= total;
            out.checkpoint(&Position { next, done }).await?;
        }
        if cursor == Position::default() && total == 0 {
            out.checkpoint(&Position {
                next: 0,
                done: true,
            })
            .await?;
        }
        Ok(())
    }

    async fn read_changes(
        &self,
        seed: u64,
        cursor: Position,
        out: &mut Emitter<Position>,
    ) -> Result<()> {
        let mut next = cursor.next.max(1);
        while next <= self.0.changes {
            let last = next
                .saturating_add(self.0.batch_rows - 1)
                .min(self.0.changes);
            let rows = (next..=last)
                .map(|position| {
                    let seq = position;
                    match change(seed, &self.0, position) {
                        Change::Upsert { id, value, n } => {
                            let partial = value.is_none();
                            (ChangeOp::Update, seq, Some(id), value, Some(n), partial)
                        }
                        Change::Delete { id } => {
                            (ChangeOp::Delete, seq, Some(id), None, None, false)
                        }
                        Change::Truncate => (ChangeOp::Truncate, seq, None, None, None, false),
                    }
                })
                .collect::<Vec<_>>();
            out.changes(changes_batch(&rows, self.0.changed_at)?)
                .await?;
            // A stream's changes end below the last position a number holds.
            next = last.saturating_add(1);
            out.checkpoint(&Position { next, done: false }).await?;
        }
        Ok(())
    }
}

/// The error of a read asked to start from `cursor`, which no read of `partition` of `stream`
/// was sent: a host that read from it could then report it committed.
fn unissued(stream: &str, partition: &str, cursor: Position) -> ConnectorError {
    ConnectorError::cursor_unissued(format!(
        "stream {stream} partition {partition} has no position {} (done: {})",
        cursor.next, cursor.done
    ))
}

/// One change row: its op, position, key, value, counter, and whether it leaves `value`
/// unchanged.
type ChangeRow = (
    ChangeOp,
    u64,
    Option<i64>,
    Option<String>,
    Option<i64>,
    bool,
);

/// `rows` as a change batch: `id`, `value` and `n`, where `timed` when each change happened, then
/// the op, sequence and unchanged columns, the unchanged bitmap flagging `value`, field 1.
fn changes_batch(rows: &[ChangeRow], timed: bool) -> Result<RecordBatch> {
    let ids: Int64Array = rows.iter().map(|row| row.2).collect();
    let values: StringArray = rows.iter().map(|row| row.3.clone()).collect();
    let n: Int64Array = rows.iter().map(|row| row.4).collect();
    let ops = Int8Array::from_iter_values(rows.iter().map(|row| row.0.code()));
    let seqs = FixedSizeBinaryArray::try_from_iter(rows.iter().map(|row| {
        let mut bytes = [0_u8; 16];
        bytes[8..].copy_from_slice(&row.1.to_be_bytes());
        bytes
    }))
    .internal("building sequences")?;
    let mut unchanged = BinaryBuilder::new();
    for row in rows {
        if row.5 {
            unchanged.append_value([0b10]);
        } else {
            unchanged.append_null();
        }
    }
    let mut columns = vec![
        ("id", Arc::new(ids) as ArrayRef),
        ("value", Arc::new(values) as ArrayRef),
        ("n", Arc::new(n) as ArrayRef),
    ];
    if timed {
        let at = rows
            .iter()
            .map(|row| i64::try_from(row.1).unwrap_or(i64::MAX));
        let at = TimestampMicrosecondArray::from_iter_values(at).with_timezone("UTC");
        columns.push((CHANGED_AT, Arc::new(at)));
    }
    columns.extend([
        (OP_COLUMN, Arc::new(ops) as ArrayRef),
        (SEQ_COLUMN, Arc::new(seqs) as ArrayRef),
        (UNCHANGED_COLUMN, Arc::new(unchanged.finish()) as ArrayRef),
    ]);
    RecordBatch::try_from_iter(columns).internal("building a change batch")
}

fn to_index(position: u64) -> usize {
    usize::try_from(position).unwrap_or(usize::MAX)
}
