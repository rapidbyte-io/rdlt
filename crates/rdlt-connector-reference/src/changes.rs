//! A source of seeded change streams: a snapshot of a keyed table, then the changes made to it.

mod slot;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::builder::BinaryBuilder;
use arrow_array::{
    ArrayRef, FixedSizeBinaryArray, Int8Array, Int64Array, RecordBatch, StringArray,
};
use rdlt_connector::prelude::*;
use rdlt_connector::{
    ChangeOp, Field, OP_COLUMN, PartitionPlan, PartitionState, Partitioning, SEQ_COLUMN,
    UNCHANGED_COLUMN,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use slot::Slot;

use crate::generator::mix;

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
}

/// One change stream: a table of `keys` rows, snapshotted, then changed `changes` times.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ChangedStream {
    /// The stream's name.
    pub name: String,
    /// The keys the table holds when the snapshot is taken: `0..keys`.
    pub keys: u64,
    /// Partitions the snapshot is read in; key `k` belongs to partition `k % snapshot_partitions`.
    #[serde(default = "one")]
    pub snapshot_partitions: u64,
    /// Changes after the snapshot, at positions `1..=changes`.
    pub changes: u64,
    /// Rows per pushed batch; a checkpoint follows each batch.
    #[serde(default = "ten")]
    pub batch_rows: u64,
    /// The positions of changes that truncate the table.
    #[serde(default)]
    pub truncates: Vec<u64>,
    /// How many changes the snapshot holds: it is taken at position `captured`, its rows carry
    /// that position, and the changes after it are read.
    #[serde(default)]
    pub captured: u64,
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
            if stream.snapshot_partitions == 0 || stream.batch_rows == 0 {
                return Err(ConnectorError::config(format!(
                    "stream {}: snapshot_partitions and batch_rows must be at least 1",
                    stream.name
                )));
            }
        }
        Ok(Self {
            seed: config.seed,
            streams: config.streams,
            slot: Slot::named(config.slot.as_deref()),
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

/// What change `position` of a stream does.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Change {
    /// Sets the key's row.
    Upsert {
        /// The key.
        id: i64,
        /// Its value; `None` leaves the value unchanged.
        value: Option<String>,
        /// Its counter: the change's position.
        n: i64,
    },
    /// Removes the key's row.
    Delete {
        /// The key.
        id: i64,
    },
    /// Removes every row.
    Truncate,
}

/// Change `position` (from 1) of `stream` under `seed`.
pub fn change(seed: u64, stream: &ChangedStream, position: u64) -> Change {
    if stream.truncates.contains(&position) {
        return Change::Truncate;
    }
    let draw = mix(seed ^ position.wrapping_mul(0x9E37_79B9));
    // Half again as many keys as the snapshot holds, so changes insert keys too.
    let span = stream.keys + stream.keys / 2 + 1;
    let id = i64::try_from(draw % span).unwrap_or(i64::MAX);
    let n = i64::try_from(position).unwrap_or(i64::MAX);
    match (draw >> 32) % 10 {
        0 | 1 => Change::Delete { id },
        2 => Change::Upsert { id, value: None, n },
        _ => Change::Upsert {
            id,
            value: Some(format!("v{position}")),
            n,
        },
    }
}

/// One row of the table a stream's changes leave.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    /// Its value.
    pub value: Option<String>,
    /// Its counter.
    pub n: i64,
}

/// The table `stream` holds, by key, once its snapshot and every change apply with deletes
/// removing rows.
pub fn expected(seed: u64, stream: &ChangedStream) -> BTreeMap<i64, Row> {
    let mut table = snapshot(seed, stream);
    let captured = usize::try_from(stream.captured).unwrap_or(usize::MAX);
    for position in (1..=stream.changes).skip(captured) {
        apply(&mut table, change(seed, stream, position));
    }
    table
}

/// The table `stream`'s snapshot holds, by key: its `keys` rows once the first `captured`
/// changes applied.
pub fn snapshot(seed: u64, stream: &ChangedStream) -> BTreeMap<i64, Row> {
    let mut table: BTreeMap<i64, Row> = (0..stream.keys)
        .map(|key| {
            let id = i64::try_from(key).unwrap_or(i64::MAX);
            (id, snapshot_row(id))
        })
        .collect();
    for position in 1..=stream.captured.min(stream.changes) {
        apply(&mut table, change(seed, stream, position));
    }
    table
}

/// Applies `change` to `table`, deletes removing rows.
fn apply(table: &mut BTreeMap<i64, Row>, change: Change) {
    match change {
        Change::Upsert { id, value, n } => {
            let value = value.or_else(|| table.get(&id).and_then(|row| row.value.clone()));
            table.insert(id, Row { value, n });
        }
        Change::Delete { id } => {
            table.remove(&id);
        }
        Change::Truncate => table.clear(),
    }
}

fn snapshot_row(id: i64) -> Row {
    Row {
        value: Some(format!("s{id}")),
        n: 0,
    }
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
        let schema = TableSchema::new(vec![
            Field::new("id", LogicalType::Int64, true),
            Field::new("value", LogicalType::Utf8, true),
            Field::new("n", LogicalType::Int64, true),
        ])
        .expect("the change schema has distinct field names");
        StreamSpec::new(StreamName::new(&self.0.name).expect("connect validated stream names"))
            .with_schema(schema)
            .with_primary_key(["id"])
            .with_read_modes([ReadMode::Cdc])
            .with_partitioning(Partitioning::Planned)
            .with_checkpointing(Checkpointing::Natural)
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
                next: self.0.captured + 1,
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
        for (partition, position) in cursors {
            source.slot.advance(&self.0.name, partition, *position);
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
            return self.read_changes(source.seed, cursor, out).await;
        }
        let index = id
            .strip_prefix("snapshot-")
            .and_then(|index| index.parse::<u64>().ok())
            .filter(|index| *index < self.0.snapshot_partitions)
            .ok_or_else(|| {
                ConnectorError::data(format!("stream {} has no partition {id}", self.0.name))
            })?;
        let stride = self.0.snapshot_partitions;
        // The partition's rows, by key; the cursor counts those read.
        let rows: Vec<(i64, Row)> = snapshot(source.seed, &self.0)
            .into_iter()
            .filter(|(id, _)| id.unsigned_abs() % stride == index)
            .collect();
        let total = u64::try_from(rows.len()).unwrap_or(u64::MAX);
        let mut next = cursor.next;
        while next < total {
            let last = (next + self.0.batch_rows).min(total);
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
            out.changes(changes_batch(&batch)?).await?;
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
}

impl Changed {
    async fn read_changes(
        &self,
        seed: u64,
        cursor: Position,
        out: &mut Emitter<Position>,
    ) -> Result<()> {
        let mut next = cursor.next.max(1);
        while next <= self.0.changes {
            let last = (next + self.0.batch_rows - 1).min(self.0.changes);
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
            out.changes(changes_batch(&rows)?).await?;
            next = last + 1;
            out.checkpoint(&Position { next, done: false }).await?;
        }
        Ok(())
    }
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

/// `rows` as a change batch: `id`, `value` and `n`, then the op, sequence and unchanged columns,
/// the unchanged bitmap flagging `value`, field 1.
fn changes_batch(rows: &[ChangeRow]) -> Result<RecordBatch> {
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
    RecordBatch::try_from_iter([
        ("id", Arc::new(ids) as ArrayRef),
        ("value", Arc::new(values) as ArrayRef),
        ("n", Arc::new(n) as ArrayRef),
        (OP_COLUMN, Arc::new(ops) as ArrayRef),
        (SEQ_COLUMN, Arc::new(seqs) as ArrayRef),
        (UNCHANGED_COLUMN, Arc::new(unchanged.finish()) as ArrayRef),
    ])
    .internal("building a change batch")
}

fn to_index(position: u64) -> usize {
    usize::try_from(position).unwrap_or(usize::MAX)
}
