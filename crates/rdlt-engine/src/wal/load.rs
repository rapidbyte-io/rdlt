//! A load's write-ahead log as its attempt writes it: the tables it describes, the batches its
//! partitions write, and its commits, each made durable before the destination sees it.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use arrow_array::RecordBatch;
use bytes::Bytes;
use rdlt_connector::{
    CommitMeta, CommitSeq, GenerationId, LoadId, PartitionId, PartitionState, Permit, PipelineId,
    Receipt, SchemaVersion, SegmentId, StreamName,
};
use tokio::sync::{Mutex, oneshot};

use super::frame::{self, Frame, Header, VERSION};
use super::store::WalStore;
use super::writer::{Command, WalWriter};
use crate::budget::{Denied, MemoryBudget, Reservation};
use crate::compute::{ComputePool, run_all};
use crate::error::Error;
use crate::limits::{LOG_FRAME_EXCEEDS_BUDGET, RECORDED};
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
}

/// The index each table version's schema frame gave it, with every view of the version its
/// batches were logged for, while any of them may still log one, and the index the next version
/// takes.
#[derive(Default)]
struct Described {
    indexes: BTreeMap<TableKey, (u32, Vec<Weak<TableView>>)>,
    next: u32,
}

impl LoadLog {
    /// The log of `load` of `pipeline` in `store`, claimed before anything of it exists, and the
    /// task writing it, for the attempt's scope to run until every clone is dropped or the log is
    /// closed; `opened` is the last commit the destination had received when the load opened.
    pub(crate) async fn start(
        store: Arc<dyn WalStore>,
        pipeline: PipelineId,
        load: LoadId,
        opened: Option<(LoadId, CommitSeq)>,
    ) -> Result<
        (
            Self,
            impl Future<Output = Result<(), Error>> + Send + 'static,
        ),
        Error,
    > {
        let claim = store
            .claim(&pipeline, load)
            .await
            .map_err(Error::from_wal)?
            .ok_or_else(|| Error::internal(format!("the log of load {load} is claimed already")))?;
        let header = Frame::Header(Header {
            version: VERSION,
            pipeline: pipeline.clone(),
            load,
            opened,
        })
        .encode()?;
        let (writer, task) = WalWriter::start(store, pipeline, load, header, claim);
        let log = Self {
            writer,
            tables: Arc::default(),
            batches: Arc::default(),
        };
        Ok((log, task))
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
        let index = self.describe(budget, table, view).await?;
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
        self.writer
            .send(Command::Batch {
                segment,
                table: index,
                frame,
                held,
            })
            .await
    }

    /// The index of `view` of the attempt's table `table` in the log, its schema frame sent first
    /// where it has none.
    async fn describe(
        &self,
        budget: &MemoryBudget,
        table: usize,
        view: &Arc<TableView>,
    ) -> Result<u32, Error> {
        let key = (table, view.table.version, view.table.generation);
        let mut tables = self.tables.lock().await;
        if let Some((index, views)) = tables.indexes.get_mut(&key) {
            // Each view of the version keeps it described: one that only rounds a column has
            // the version of the view before it.
            let known = |known: &Weak<TableView>| {
                known
                    .upgrade()
                    .is_some_and(|alive| Arc::ptr_eq(&alive, view))
            };
            if !views.iter().any(known) {
                views.push(Arc::downgrade(view));
            }
            return Ok(*index);
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
        let held = Box::new(settled(budget, held, frame.len()).await?);
        self.writer
            .send(Command::Table { index, frame, held })
            .await?;
        tables
            .indexes
            .insert(key, (index, vec![Arc::downgrade(view)]));
        Ok(index)
    }

    /// Logs `sealed`, then the phases `begun` that `meta`'s commit begins with it and the commit,
    /// and returns once the commit's frame is durable; `budget` holds each frame's bytes until
    /// it is appended.
    ///
    /// The phase frames go in one append with the commit's, so a crash tears them with it.
    pub(crate) async fn commit(
        &self,
        budget: &MemoryBudget,
        sealed: Vec<Sealed>,
        begun: Vec<frame::BegunPhase>,
        meta: &CommitMeta,
        prepaid: u64,
    ) -> Result<(), Error> {
        self.retire().await?;
        // The commit settles every segment it sealed, those it publishes nothing of included, so
        // its receipt lets their chunks go.
        let mut segments = meta.segments.clone();
        for seal in &sealed {
            segments.insert(seal.segment);
        }
        for seal in sealed {
            let segment = seal.segment;
            // Reserved before it is encoded for the cursors it records, each written twice over
            // in base64, and for the frame as it is once it exists.
            let cursors = [seal.from.as_ref(), Some(&seal.state)];
            let cursors = cursors.into_iter().flatten().map(recorded);
            let bytes = cursors.fold(FRAMED, u64::saturating_add);
            let held = reserved(budget, bytes).await?;
            let frame = Frame::Seal(frame::Seal {
                segment,
                stream: seal.stream,
                partition: seal.partition,
                replayable: seal.replayable,
                phase: seal.phase,
                from: seal.from,
                state: seal.state,
            })
            .encode()?;
            let held = settled(budget, held, frame.len()).await?;
            let seal = Command::Seal {
                segment,
                frame,
                held: Box::new(held),
            };
            self.writer.send(seal).await?;
        }
        // Reserved before they are encoded for what they take at most, but what they record of
        // tables, which each table's change reserved.
        let held = reserved(budget, commit_bytes(&begun, meta).saturating_sub(prepaid)).await?;
        let mut frames = Vec::new();
        for begun in begun {
            frames.extend_from_slice(&Frame::Begun(begun).encode()?);
        }
        frames.extend_from_slice(&frame::commit(meta)?);
        let frame = frames
            .len()
            .saturating_sub(usize::try_from(prepaid).unwrap_or(usize::MAX));
        let held = settled(budget, held, frame).await?;
        let held = Box::new(held);
        let (durable, answer) = oneshot::channel();
        self.writer
            .send(Command::Commit {
                seq: meta.commit_seq,
                segments,
                frame: Bytes::from(frames),
                held,
                durable,
            })
            .await?;
        answer
            .await
            .map_err(|_| Error::wal("the write-ahead log's writer stopped"))?
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
            .filter(|(_, (_, views))| views.iter().all(|view| view.strong_count() == 0))
            .map(|(key, _)| *key)
            .collect();
        let tables: Vec<u32> = gone
            .iter()
            .filter_map(|key| described.indexes.remove(key))
            .map(|(index, _)| index)
            .collect();
        if tables.is_empty() {
            return Ok(());
        }
        self.writer.send(Command::Retire { tables }).await
    }

    /// Logs `receipt`, the commit's the destination answered with.
    pub(crate) async fn committed(&self, receipt: &Receipt) -> Result<(), Error> {
        let frame = Frame::Committed(receipt.clone()).encode()?;
        self.writer
            .send(Command::Committed {
                seq: receipt.commit_seq,
                frame,
            })
            .await
    }

    /// Tells the log `segment`'s partition ended without sealing it: no commit takes it, so it
    /// holds no chunk back.
    pub(crate) async fn abandon(&self, segment: SegmentId) -> Result<(), Error> {
        self.writer.send(Command::Abandon { segment }).await
    }

    /// Closes the log: no frame follows, and the log is removed where every commit in it has its
    /// receipt.
    pub(crate) async fn close(&self) -> Result<(), Error> {
        let (done, answer) = oneshot::channel();
        let frame = Frame::Closed.encode()?;
        self.writer.send(Command::Close { frame, done }).await?;
        answer
            .await
            .map_err(|_| Error::wal("the write-ahead log's writer stopped"))?
    }
}

/// Bytes: about what `state` takes in a frame that records it.
fn recorded(state: &PartitionState) -> u64 {
    match state {
        PartitionState::Cursor(cursor) => RECORDED.saturating_mul(count(cursor.bytes().len())),
        PartitionState::Done => 0,
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
            Denied::TooLarge(large) => {
                Error::wal(large.to_string()).with_code(LOG_FRAME_EXCEEDS_BUDGET)
            }
        })
}

/// Bytes: what a frame takes beside the records it holds, at most: its head, its kind and the
/// names and numbers it carries.
const FRAMED: u64 = 4 << 10;

/// Bytes: what a record of state takes in a frame beside its key and value, at most.
const PER_RECORD: u64 = 64;

/// Bytes: the most the frames of the commit `meta` and of the phases `begun` with it take: each
/// record's key and value written twice over, what a record and a frame take beside, and the
/// segments, generations and tables the commit names.
fn commit_bytes(begun: &[frame::BegunPhase], meta: &CommitMeta) -> u64 {
    let changes = begun.iter().flat_map(|begun| &begun.changes);
    let record = |change: &rdlt_connector::StateChange| match change {
        rdlt_connector::StateChange::Put(record) => {
            count(record.key.len().saturating_add(record.value.len()))
        }
        rdlt_connector::StateChange::Delete(key) => count(key.len()),
    };
    let records = changes.chain(&meta.state_delta).map(|change| {
        RECORDED
            .saturating_mul(record(change))
            .saturating_add(PER_RECORD)
    });
    let named = [
        meta.finish_generations.len(),
        meta.child_tables.len(),
        meta.drop_tables.len(),
    ];
    let named = named.into_iter().map(count);
    let named = named.fold(meta.segments.len(), u64::saturating_add);
    let named = named.saturating_mul(NAMED);
    let frames = count(begun.len()).saturating_add(1).saturating_mul(FRAMED);
    records.fold(named.saturating_add(frames), u64::saturating_add)
}

/// Bytes: the most a segment, a generation or a table a commit names takes in its frame.
const NAMED: u64 = 1 << 10;

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
