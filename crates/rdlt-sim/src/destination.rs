//! The simulated destination: transactional commits, receipts, fencing and staging, checking the
//! engine's invariants as it commits.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::{Int64Array, RecordBatch};
use rdlt_connector::{
    Capabilities, CommitMeta, CommitSeq, ConnectContext, ConnectorError, DestinationConnector,
    Epoch, GenerationId, LoadId, OpenContext, Opened, PartitionId, PartitionState, Receipt, Result,
    SegmentId, Session, StateChange, StateEntry, StateKey, StateRecord, StreamName, TableChange,
    TablePath, TableRef, TableWriter, WriteStats,
};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::source::SimCursor;
use crate::workload::Row;
use crate::world::{FaultPoint, World};

/// The destination's contents, kept in its world.
#[derive(Debug, Default)]
pub(crate) struct Store {
    epoch: Epoch,
    state: BTreeMap<String, StateRecord>,
    receipts: BTreeMap<(LoadId, CommitSeq), Receipt>,
    staged: BTreeMap<SegmentId, Vec<Staged>>,
    names: BTreeMap<TablePath, String>,
    tables: BTreeMap<String, Table>,
    /// The generations of full reads completed, by stream and the phase they completed in.
    ///
    /// A run that read a stream twice would complete the same generation twice, and count once.
    completions: BTreeMap<(String, usize), BTreeSet<GenerationId>>,
}

#[derive(Debug)]
struct Staged {
    table: String,
    generation: Option<GenerationId>,
    rows: Vec<Row>,
}

#[derive(Debug, Default)]
struct Table {
    published: Vec<Row>,
    generations: BTreeMap<GenerationId, Vec<Row>>,
}

/// Every row published to `table`.
pub fn published(world: &World, table: &str) -> Vec<Row> {
    let store = world.store.lock();
    store
        .tables
        .get(table)
        .map(|table| table.published.clone())
        .unwrap_or_default()
}

/// Whether state records a full read in progress.
pub(crate) fn reads_in_progress(world: &World) -> bool {
    let store = world.store.lock();
    store
        .state
        .keys()
        .any(|key| matches!(StateKey::parse(key), Ok(StateKey::Generation(_))))
}

/// Distinct full reads of `stream` completed in `phase`.
pub fn completions(world: &World, stream: &str, phase: usize) -> usize {
    let store = world.store.lock();
    store
        .completions
        .get(&(stream.to_owned(), phase))
        .map_or(0, BTreeSet::len)
}

/// The committed resume offset of a partition: `u64::MAX` once it is done.
pub(crate) fn committed_next(
    world: &World,
    stream: &StreamName,
    partition: &PartitionId,
) -> Option<u64> {
    let store = world.store.lock();
    next_offset(&store.state, stream, partition)
}

fn next_offset(
    state: &BTreeMap<String, StateRecord>,
    stream: &StreamName,
    partition: &PartitionId,
) -> Option<u64> {
    let key = StateKey::Partition(stream.clone(), partition.clone()).encode();
    let entry = StateEntry::from_record(state.get(&key)?).ok()?;
    match entry {
        StateEntry::Partition {
            state: PartitionState::Cursor(cursor),
            ..
        } => cursor.decode::<SimCursor>(1).ok().map(|cursor| cursor.next),
        StateEntry::Partition {
            state: PartitionState::Done,
            ..
        } => Some(u64::MAX),
        _ => None,
    }
}

/// Configuration of [`SimDestination`]: the world to write to.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct SimDestinationConfig {
    /// The world's registered name.
    pub world: String,
}

/// Writes to a world's store.
#[derive(Debug)]
pub struct SimDestination {
    world: Arc<World>,
}

impl DestinationConnector for SimDestination {
    const ID: &'static str = "io.rapidbyte.sim";
    const VERSION: &'static str = "0.0.0";
    type Config = SimDestinationConfig;
    type Session = SimSession;

    fn capabilities(&self) -> Capabilities {
        let mut capabilities = Capabilities::minimal();
        capabilities.write_modes.replace = true;
        capabilities
    }

    async fn connect(config: SimDestinationConfig, _context: &ConnectContext) -> Result<Self> {
        let world = World::named(&config.world)
            .ok_or_else(|| ConnectorError::config(format!("no world named {}", config.world)))?;
        Ok(Self { world })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    async fn open(&self, _context: &OpenContext) -> Result<Opened<SimSession>> {
        self.world.latency().await;
        if let Some(fault) = self.world.fault(FaultPoint::Open) {
            return Err(fault);
        }
        let mut store = self.world.store.lock();
        store.epoch = store.epoch.next();
        Ok(Opened {
            session: SimSession {
                world: Arc::clone(&self.world),
                epoch: store.epoch,
            },
            epoch: store.epoch,
            state: store.state.values().cloned().collect(),
        })
    }
}

/// A session of [`SimDestination`].
#[derive(Debug)]
pub struct SimSession {
    world: Arc<World>,
    epoch: Epoch,
}

impl Session for SimSession {
    type Writer = SimWriter;

    async fn apply_schema(&mut self, change: &TableChange) -> Result<()> {
        if let TableChange::Create { table, .. } = change {
            let mut store = self.world.store.lock();
            store
                .names
                .insert(table.path.clone(), table.name.to_string());
            store.tables.entry(table.name.to_string()).or_default();
        }
        Ok(())
    }

    async fn writer(&mut self, table: &TableRef) -> Result<SimWriter> {
        let mut store = self.world.store.lock();
        store
            .names
            .insert(table.path.clone(), table.name.to_string());
        Ok(SimWriter {
            world: Arc::clone(&self.world),
            epoch: self.epoch,
            table: table.name.to_string(),
            generation: table.generation,
            buffered: Vec::new(),
        })
    }

    async fn discard_staged(&mut self) -> Result<()> {
        self.world.store.lock().staged.clear();
        Ok(())
    }

    async fn commit(&mut self, meta: &CommitMeta) -> Result<Receipt> {
        self.world.latency().await;
        if let Some(fault) = self.world.fault(FaultPoint::CommitBefore) {
            return Err(fault);
        }
        let receipt = {
            let mut store = self.world.store.lock();
            if store.epoch != self.epoch || meta.epoch != self.epoch {
                return Err(ConnectorError::fenced(format!(
                    "the store is at epoch {}; this session opened at {}",
                    store.epoch, self.epoch
                )));
            }
            let key = (meta.load_id, meta.commit_seq);
            if let Some(receipt) = store.receipts.get(&key) {
                return Ok(receipt.clone());
            }
            let published = store.publish(meta);
            store.apply(&self.world, meta);
            store.check_cursors(&self.world, &published);
            let receipt = Receipt {
                load_id: meta.load_id,
                commit_seq: meta.commit_seq,
                committed_at: UNIX_EPOCH,
                rows: published.iter().map(|(_, rows)| rows.len() as u64).sum(),
                bytes: 0,
            };
            store.receipts.insert(key, receipt.clone());
            receipt
        };
        match self.world.fault(FaultPoint::CommitAfter) {
            Some(fault) => Err(fault),
            None => Ok(receipt),
        }
    }

    async fn close(self) -> Result<()> {
        Ok(())
    }
}

impl Store {
    /// Publishes the staged segments of `meta` and swaps in the generations it finishes; returns
    /// the rows published, by table.
    fn publish(&mut self, meta: &CommitMeta) -> Vec<(String, Vec<Row>)> {
        let mut published = Vec::new();
        for segment in meta.segments.iter() {
            for staged in self.staged.remove(&segment).unwrap_or_default() {
                let table = self.tables.entry(staged.table.clone()).or_default();
                match staged.generation {
                    Some(generation) => table
                        .generations
                        .entry(generation)
                        .or_default()
                        .extend_from_slice(&staged.rows),
                    None => table.published.extend_from_slice(&staged.rows),
                }
                published.push((staged.table, staged.rows));
            }
        }
        for (path, generation) in &meta.finish_generations {
            let Some(name) = self.names.get(path).cloned() else {
                continue;
            };
            let table = self.tables.entry(name).or_default();
            table.published = table.generations.remove(generation).unwrap_or_default();
            table.generations.clear();
        }
        published
    }

    /// Applies the state changes of `meta`, checking that no partition's cursor moves backwards
    /// and counting completed full reads.
    fn apply(&mut self, world: &World, meta: &CommitMeta) {
        for change in &meta.state_delta {
            match change {
                StateChange::Put(record) => {
                    match StateEntry::from_record(record) {
                        Ok(StateEntry::Partition {
                            stream, partition, ..
                        }) => {
                            let before = next_offset(&self.state, &stream, &partition);
                            self.state.insert(record.key.clone(), record.clone());
                            let after = next_offset(&self.state, &stream, &partition);
                            if before.is_some_and(|before| after < Some(before)) {
                                world.violation(format!(
                                    "stream {stream} partition {partition}: cursor moved back \
                                     from {before:?} to {after:?}"
                                ));
                            }
                            continue;
                        }
                        Ok(StateEntry::Completed {
                            stream,
                            generations,
                        }) => {
                            // The read that just completed is the newest in the list.
                            let key = (stream.to_string(), world.phase());
                            let newest = generations.last().copied();
                            self.completions.entry(key).or_default().extend(newest);
                        }
                        Ok(_) => {}
                        Err(error) => world.violation(format!("unreadable state record: {error}")),
                    }
                    self.state.insert(record.key.clone(), record.clone());
                }
                StateChange::Delete(key) => {
                    self.state.remove(key);
                }
            }
        }
    }

    /// Checks that every row just published lies before its partition's committed cursor.
    fn check_cursors(&self, world: &World, published: &[(String, Vec<Row>)]) {
        for (table, rows) in published {
            let Ok(stream) = StreamName::new(table) else {
                continue;
            };
            for row in rows {
                let partition =
                    PartitionId::parse(format!("p{}", row.partition)).expect("valid partition id");
                let next = next_offset(&self.state, &stream, &partition);
                let offset = u64::try_from(row.offset).unwrap_or(u64::MAX);
                if next.is_none_or(|next| offset >= next) {
                    world.violation(format!(
                        "stream {stream} partition {partition}: row {offset} published past the \
                         committed cursor {next:?}"
                    ));
                }
            }
        }
    }
}

/// A writer of [`SimDestination`]: buffers rows and stages them on flush.
#[derive(Debug)]
pub struct SimWriter {
    world: Arc<World>,
    epoch: Epoch,
    table: String,
    generation: Option<GenerationId>,
    buffered: Vec<(SegmentId, Vec<Row>)>,
}

impl TableWriter for SimWriter {
    async fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> Result<()> {
        if let Some(fault) = self.world.fault(FaultPoint::Write) {
            return Err(fault);
        }
        self.buffered.push((segment, rows(&batch)?));
        Ok(())
    }

    async fn flush(&mut self) -> Result<WriteStats> {
        self.world.latency().await;
        if let Some(fault) = self.world.fault(FaultPoint::Flush) {
            return Err(fault);
        }
        let mut store = self.world.store.lock();
        if store.epoch != self.epoch {
            return Err(ConnectorError::fenced("a newer session holds the store"));
        }
        let mut stats = WriteStats::default();
        for (segment, rows) in self.buffered.drain(..) {
            stats.rows += rows.len() as u64;
            store.staged.entry(segment).or_default().push(Staged {
                table: self.table.clone(),
                generation: self.generation,
                rows,
            });
        }
        Ok(stats)
    }
}

fn rows(batch: &RecordBatch) -> Result<Vec<Row>> {
    let column = |name: &str| {
        batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| ConnectorError::data(format!("the batch has no Int64 column {name}")))
    };
    let (id, partition, offset, value) = (
        column("id")?,
        column("partition")?,
        column("offset")?,
        column("value")?,
    );
    Ok((0..batch.num_rows())
        .map(|row| Row {
            id: id.value(row),
            partition: partition.value(row),
            offset: offset.value(row),
            value: value.value(row),
        })
        .collect())
}
