//! The simulated destination: transactional commits, receipts, fencing, staging and merges,
//! checking the engine's invariants as it commits.

mod cells;
mod columns;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::RecordBatch;
use rdlt_connector::{
    Capabilities, CommitMeta, CommitSeq, ConnectContext, ConnectorError, DestinationConnector,
    Epoch, GenerationId, LoadId, MergeKey, NameMap, OpenContext, Opened, PartitionId,
    PartitionState, Receipt, Result, SegmentId, Session, StateChange, StateEntry, StateKey,
    StateRecord, StreamName, TableChange, TablePath, TableRef, TableWriter, WriteStats,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::source::SimCursor;
use crate::world::{FaultPoint, World};

pub use cells::Cells;
pub(crate) use cells::canonical;

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
    merge: Option<MergeKey>,
    rows: Vec<Cells>,
}

#[derive(Debug, Default)]
struct Table {
    columns: columns::Columns,
    published: Vec<Cells>,
    generations: BTreeMap<GenerationId, Vec<Cells>>,
}

/// Every row published for `stream`, with each source column's value gathered from its column
/// and variant columns through the committed name map: the rows as the source sent them.
///
/// A value found in two columns of one source column is a violation.
pub fn published(world: &World, stream: &str) -> Vec<Map<String, Value>> {
    let store = world.store.lock();
    let Ok(path) = TablePath::new([stream]) else {
        return Vec::new();
    };
    let Some((physical, names)) = names(&store.state, &path) else {
        return Vec::new();
    };
    let Some(table) = store.tables.get(physical.as_str()) else {
        return Vec::new();
    };
    table
        .published
        .iter()
        .map(|row| {
            cells::source_row(row, &names).unwrap_or_else(|finding| {
                world.violation(format!("stream {stream}: {finding}"));
                Map::new()
            })
        })
        .collect()
}

/// The identifier and name map state records for the table at `path`.
fn names(state: &BTreeMap<String, StateRecord>, path: &TablePath) -> Option<(String, NameMap)> {
    let record = state.get(&StateKey::Names(path.clone()).encode())?;
    match StateEntry::from_record(record).ok()? {
        StateEntry::Names {
            physical, names, ..
        } => Some((physical.to_string(), names)),
        _ => None,
    }
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

/// Writes to a world's store, with the capabilities the world drew.
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
        self.world.capabilities.clone()
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
        let table = change.table();
        let mut store = self.world.store.lock();
        store
            .names
            .insert(table.path.clone(), table.name.to_string());
        let entry = store.tables.entry(table.name.to_string()).or_default();
        columns::apply(&mut entry.columns, change)
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
            merge: table.merge.clone(),
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
    fn publish(&mut self, meta: &CommitMeta) -> Vec<(String, Vec<Cells>)> {
        let mut published = Vec::new();
        let mut merging: BTreeMap<String, (MergeKey, Vec<Cells>)> = BTreeMap::new();
        for segment in meta.segments.iter() {
            for staged in self.staged.remove(&segment).unwrap_or_default() {
                published.push((staged.table.clone(), staged.rows.clone()));
                if let Some(key) = staged.merge {
                    merging
                        .entry(staged.table)
                        .or_insert_with(|| (key, Vec::new()))
                        .1
                        .extend(staged.rows);
                    continue;
                }
                let table = self.tables.entry(staged.table).or_default();
                match staged.generation {
                    Some(generation) => table
                        .generations
                        .entry(generation)
                        .or_default()
                        .extend(staged.rows),
                    None => table.published.extend(staged.rows),
                }
            }
        }
        for (name, (key, rows)) in merging {
            cells::merge(
                &mut self.tables.entry(name).or_default().published,
                rows,
                &key,
            );
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
    fn check_cursors(&self, world: &World, published: &[(String, Vec<Cells>)]) {
        for (table, rows) in published {
            let Some((path, _)) = self.names.iter().find(|(_, name)| *name == table) else {
                continue;
            };
            let Some(stream) = path
                .segments()
                .next()
                .and_then(|name| StreamName::new(name).ok())
            else {
                continue;
            };
            let Some((_, names)) = names(&self.state, path) else {
                world.violation(format!("stream {stream}: rows published without names"));
                continue;
            };
            for row in rows {
                let Ok(row) = cells::source_row(row, &names) else {
                    continue;
                };
                let number = |column: &str| row.get(column).and_then(Value::as_u64);
                let (Some(partition), Some(offset)) = (number("partition"), number("offset"))
                else {
                    world.violation(format!("stream {stream}: a row lacks its position"));
                    continue;
                };
                let partition =
                    PartitionId::parse(format!("p{partition}")).expect("valid partition id");
                let next = next_offset(&self.state, &stream, &partition);
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
    merge: Option<MergeKey>,
    buffered: Vec<(SegmentId, Vec<Cells>)>,
}

impl TableWriter for SimWriter {
    async fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> Result<()> {
        if let Some(fault) = self.world.fault(FaultPoint::Write) {
            return Err(fault);
        }
        let unfit = {
            let store = self.world.store.lock();
            let held = store.tables.get(&self.table).map(|table| &table.columns);
            columns::unfit(held.unwrap_or(&columns::Columns::new()), &batch)
        };
        for finding in unfit {
            self.world
                .violation(format!("table {}: {finding}", self.table));
        }
        self.buffered.push((segment, cells::rows(&batch)?));
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
                merge: self.merge.clone(),
                rows,
            });
        }
        Ok(stats)
    }
}
