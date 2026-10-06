//! A load's write-ahead log as its attempt writes it: the tables it describes, the batches its
//! partitions write, and its commits, each made durable before the destination sees it.

mod commit;
mod room;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::future::Future;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use arrow_array::RecordBatch;
use rdlt_connector::{
    CommitSeq, GenerationId, PartitionId, PartitionState, Permit, Receipt, SchemaVersion,
    SegmentId, StreamName,
};
use tokio::sync::{Mutex, oneshot};

use super::frame::{self, Frame};
use super::store::WalStore;
pub(crate) use super::writer::Owner;
use super::writer::{Command, WalWriter};
use crate::budget::{Denied, MemoryBudget, Reservation};
use crate::compute::{ComputePool, run_all};
use crate::error::Error;
use crate::limits::{LOG_FRAME_EXCEEDS_BUDGET, LOG_PARTS, RECORDED};
use crate::table::TableView;

/// A table as a load's log tells its versions apart: its index in the attempt, its schema version
/// and its generation.
type TableKey = (usize, SchemaVersion, Option<GenerationId>);

/// A sealed segment, as its commit's frame is preceded by it.
pub(crate) struct Sealed {
    pub(crate) segment: SegmentId,
    pub(crate) stream: StreamName,
    pub(crate) partition: PartitionId,
    /// Whether the stream's source can read the segment again.
    pub(crate) replayable: bool,
    /// The phase of the stream the segment belongs to.
    pub(crate) phase: u16,
    /// Where the destination held the partition just before the segment's commit, once that
    /// commit began the stream's phase if it did.
    pub(crate) from: Option<PartitionState>,
    pub(crate) state: PartitionState,
}

/// The sending end of a load's log; clones share it.
#[derive(Clone)]
pub(crate) struct LoadLog {
    writer: WalWriter,
    /// The table versions the log describes, held while a new one is sent, so no batch frame of
    /// a table precedes its schema frame.
    tables: Arc<Mutex<Described>>,
    /// The ordinal the next batch frame takes.
    batches: Arc<AtomicU64>,
    /// The batch frames and rows logged of each segment not yet sealed or abandoned.
    counts: Arc<parking_lot::Mutex<BTreeMap<SegmentId, Counted>>>,
    /// What the log may hold on disk, and what makes a commit due.
    disk: Arc<Disk>,
    /// What decides whether a batch that finds the log full waits for room or is refused.
    pressure: Arc<room::Pressure>,
    /// What the store's stagings hold in memory, charged to the budget while the log is written.
    _staging: Arc<Option<Reservation>>,
}

/// What a load's log may hold on disk, and when it makes a commit due.
struct Disk {
    /// Bytes: what the log may hold.
    limit: u64,
    /// Bytes: what the log holds when a commit is due next.
    due: AtomicU64,
}

/// The batch frames and the rows logged of a segment.
#[derive(Clone, Copy, Debug, Default)]
struct Counted {
    batches: u64,
    rows: u64,
}

/// The index each table version's schema frame gave it, with every view of the version its
/// batches were logged for, while any of them may still log one, and the index the next version
/// takes.
#[derive(Default)]
struct Described {
    indexes: BTreeMap<TableKey, Version>,
    next: u32,
}

/// A table version the log describes: its index, the bytes of its schema frame, and the views
/// of it that may still log a batch.
struct Version {
    index: u32,
    schema: u64,
    views: Vec<Weak<TableView>>,
}

impl LoadLog {
    /// The log of `owner`'s load in `store`, holding at most `limit` bytes on disk, and the task
    /// writing it, for the attempt's scope to run until every clone is dropped or the log is
    /// closed; `staging` holds what the store stages in memory, and what a carry reads at once.
    pub(crate) fn start(
        store: Arc<dyn WalStore>,
        owner: Owner,
        limit: NonZeroU64,
        staging: Option<Reservation>,
    ) -> (
        Self,
        impl Future<Output = Result<(), Error>> + Send + 'static,
    ) {
        let (writer, task) = WalWriter::start(store, owner, limit.get());
        writer
            .shared()
            .committed
            .store(limit.get() / LOG_PARTS, Ordering::SeqCst);
        let disk = Disk {
            limit: limit.get(),
            due: AtomicU64::new(limit.get() / 2),
        };
        let log = Self {
            writer,
            tables: Arc::default(),
            batches: Arc::default(),
            counts: Arc::default(),
            disk: Arc::new(disk),
            pressure: Arc::default(),
            _staging: Arc::new(staging),
        };
        (log, task)
    }

    /// Whether the log holds enough on disk that a commit is due: half of what it may hold, and
    /// an eighth more after each commit made while it was due.
    pub(crate) fn due(&self) -> bool {
        self.held() >= self.disk.due.load(Ordering::Relaxed)
    }

    /// Notes a commit was made: the next is due once the log holds an eighth more than now, or
    /// half of what it may.
    pub(crate) fn passed(&self) {
        let next = self
            .held()
            .saturating_add(self.disk.limit / 8)
            .max(self.disk.limit / 2);
        self.disk.due.store(next, Ordering::Relaxed);
    }

    /// The oldest commit of the load a replay of its log may repeat: the oldest whose frame a
    /// chunk not deleted holds; none where no chunk holds one.
    pub(crate) fn oldest(&self) -> Option<CommitSeq> {
        *self.writer.shared().oldest.lock()
    }

    /// Bytes: what the log holds on disk.
    pub(crate) fn held(&self) -> u64 {
        self.writer.shared().held.load(Ordering::Relaxed)
    }

    /// Logs `batch` of `segment`, lowered for `view` of the attempt's table `table`, encoded on
    /// `compute`; `held` holds the frame's bytes until it is appended, and `budget` its table's
    /// schema frame where it is the first.
    pub(crate) async fn batch(
        &self,
        compute: &dyn ComputePool,
        budget: &MemoryBudget,
        mut held: Permit,
        (table, view): (usize, &Arc<TableView>),
        segment: SegmentId,
        batch: &RecordBatch,
    ) -> Result<(), Error> {
        // A failed write fails the batches after it at once, not only the next commit.
        self.writer.shared().failure()?;
        let (index, schema) = self.describe(budget, table, view).await?;
        {
            let mut counts = self.counts.lock();
            let counted = counts.entry(segment).or_default();
            counted.batches += 1;
            counted.rows += u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
        }
        // A partition logs a segment's batches one after another, so their ordinals are their order.
        let batch = frame::Batch {
            segment,
            table: index,
            ordinal: self.batches.fetch_add(1, Ordering::Relaxed),
            batch: batch.clone(),
        };
        let mut encoded = run_all(compute, [move || Frame::Batch(batch).encode()]).await;
        let frame = encoded
            .pop()
            .ok_or_else(|| Error::internal("a batch frame's job returned nothing"))??;
        // The frame takes what it takes of what its piece reserved for it; the rest is released.
        if let Some(reserved) = held.downcast_mut::<Reservation>() {
            reserved.shrink(count(frame.len()));
        }
        // Counted with its table's schema frame, which the chunk it lands in may lack, and what
        // ends that chunk, where the frame would take it past what a carry may copy.
        let closing = self
            .admit(count(frame.len()).saturating_add(schema))
            .await?;
        self.writer
            .send(Command::Batch {
                segment,
                table: index,
                frame,
                schema,
                closing,
                held,
            })
            .await
    }

    /// The index of `view` of the attempt's table `table` in the log, its schema frame sent first
    /// where it has none, and the bytes of that frame.
    async fn describe(
        &self,
        budget: &MemoryBudget,
        table: usize,
        view: &Arc<TableView>,
    ) -> Result<(u32, u64), Error> {
        let key = (table, view.table.version, view.table.generation);
        let mut tables = self.tables.lock().await;
        if let Some(version) = tables.indexes.get_mut(&key) {
            // Each view of the version keeps it described: one that only rounds a column has
            // the version of the view before it.
            let known = |known: &Weak<TableView>| {
                known
                    .upgrade()
                    .is_some_and(|alive| Arc::ptr_eq(&alive, view))
            };
            if !version.views.iter().any(known) {
                version.views.push(Arc::downgrade(view));
            }
            return Ok((version.index, version.schema));
        }
        let index = tables.next;
        tables.next = index
            .checked_add(1)
            .ok_or_else(|| Error::internal("a load writes more table versions than a log names"))?;
        // Reserved before it is encoded for what it takes at most, until it is first appended.
        let schema = view.physical_schema();
        let held = reserved(budget, described(&schema)).await?;
        let frame = Frame::Schema(frame::Table {
            index,
            table: view.table.clone(),
            schema,
        })
        .encode()?;
        let schema = count(frame.len());
        let held = Box::new(settled(budget, held, frame.len()).await?);
        self.writer
            .send(Command::Table { index, frame, held })
            .await?;
        let views = vec![Arc::downgrade(view)];
        let version = Version {
            index,
            schema,
            views,
        };
        tables.indexes.insert(key, version);
        Ok((index, schema))
    }

    /// Tells the writer to forget the schema frames of table versions all of whose views are gone.
    ///
    /// A batch is logged while its view is held, and its view is noted before its frame is
    /// sent, so every batch frame of such a version was sent before this: none follows its
    /// retirement. A batch of the version lowered by a view of its
    /// own after it is described again under a new index.
    async fn retire(&self) -> Result<(), Error> {
        let mut described = self.tables.lock().await;
        let gone: Vec<TableKey> = described
            .indexes
            .iter()
            .filter(|(_, version)| version.views.iter().all(|view| view.strong_count() == 0))
            .map(|(key, _)| *key)
            .collect();
        let tables: Vec<u32> = gone
            .iter()
            .filter_map(|key| described.indexes.remove(key))
            .map(|version| version.index)
            .collect();
        if tables.is_empty() {
            return Ok(());
        }
        self.writer.send(Command::Retire { tables }).await
    }

    /// Notes `receipt`, the commit's the destination answered with, which the next chunk the log
    /// publishes records.
    pub(crate) async fn committed(&self, receipt: &Receipt) -> Result<(), Error> {
        self.writer
            .send(Command::Committed {
                seq: receipt.commit_seq,
            })
            .await
    }

    /// Tells the log `segment`'s partition ended without sealing it: no commit takes it, so it
    /// holds no chunk back.
    pub(crate) async fn abandon(&self, segment: SegmentId) -> Result<(), Error> {
        self.counts.lock().remove(&segment);
        self.writer.send(Command::Abandon { segment }).await
    }

    /// Closes the log: no frame follows, and the log is removed where every commit in it has its
    /// receipt.
    pub(crate) async fn close(&self) -> Result<(), Error> {
        let (done, answer) = oneshot::channel();
        self.writer.send(Command::Close { done }).await?;
        answer
            .await
            .map_err(|_| Error::wal("the write-ahead log's writer stopped"))?
    }
}

/// Reserves `bytes` of the log's share of `budget` for a frame about to be encoded, as much as
/// the frame takes at most.
///
/// # Errors
///
/// A frame beyond the log's share is refused with [`LOG_FRAME_EXCEEDS_BUDGET`]: no wait could
/// admit it.
async fn reserved(budget: &MemoryBudget, bytes: u64) -> Result<Reservation, Error> {
    budget
        .acquire_log(bytes)
        .await
        .map_err(|denied| match denied {
            Denied::Exhausted(exhausted) => Error::memory(exhausted),
            Denied::TooLarge(large) => Error::wal(format!(
                "a frame of the write-ahead log takes up to {bytes} bytes in memory, more than \
                 the memory budget's share for the log holds"
            ))
            .with_code(LOG_FRAME_EXCEEDS_BUDGET)
            .with_source(large),
        })
}

/// Bytes: what a frame takes beside the records it holds, at most: its head, its kind and the
/// names and numbers it carries.
const FRAMED: u64 = 4 << 10;

/// Bytes: the most a table's schema frame takes: its schema written twice over, as its fields
/// with their names take, and a frame beside.
fn described(schema: &rdlt_connector::TableSchema) -> u64 {
    let fields = rdlt_connector::cost::schema_bytes(&schema.to_arrow());
    RECORDED.saturating_mul(fields).saturating_add(FRAMED)
}

/// What holds a frame of `frame` bytes that `held` was reserved for before it was encoded: what
/// the frame takes, the rest released.
///
/// A frame larger than was reserved for it keeps what it holds and waits for the rest: the
/// writer releases what the log's share holds without asking for more.
async fn settled(
    budget: &MemoryBudget,
    mut held: Reservation,
    frame: usize,
) -> Result<(Reservation, Option<Reservation>), Error> {
    let frame = count(frame);
    if frame > held.bytes() {
        let more = reserved(budget, frame - held.bytes()).await?;
        return Ok((held, Some(more)));
    }
    held.shrink(frame);
    Ok((held, None))
}

fn count(bytes: usize) -> u64 {
    u64::try_from(bytes).unwrap_or(u64::MAX)
}
