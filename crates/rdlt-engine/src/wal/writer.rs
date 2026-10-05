//! The task that owns a load's write-ahead log: it stages frames in the order they are sent,
//! publishes the chunk they make at each commit before answering, and deletes the chunks nothing
//! needs any more.

mod carry;
mod publish;
mod relief;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use rdlt_connector::{CommitSeq, Epoch, LoadId, Permit, PipelineId, SegmentId, SegmentSet};
use tokio::sync::{mpsc, oneshot};

use super::frame::{self, Frame, Header};
use super::store::{Chunk, StagedChunk, WalStore};
use crate::budget::MemoryBudget;
use crate::crash::crash_point;
use crate::error::Error;
use crate::limits::LOG_PARTS;

/// Frames queued for the writer before a sender waits: a batch's frame holds its whole batch.
const QUEUED: usize = 16;

/// Whose log the writer writes, and what its load opened on.
#[derive(Clone, Debug)]
pub(crate) struct Owner {
    pub(crate) pipeline: PipelineId,
    pub(crate) load: LoadId,
    /// The epoch of the load's session.
    pub(crate) epoch: Epoch,
    /// The last commit the destination had received when the load opened.
    pub(crate) opened: Option<(LoadId, CommitSeq)>,
    /// The destination the log is written for, as its header names it.
    pub(crate) origin: LoadId,
}

/// A frame for the writer, encoded, with what it says about the log.
pub(crate) enum Command {
    /// A table's schema frame, written to each chunk before the first batch of the table in it,
    /// and the memory it holds until it is first written.
    Table {
        index: u32,
        frame: Bytes,
        held: Permit,
    },
    /// A batch frame of `segment`, for the table at `table`, and the memory it holds until it
    /// is written; `schema` bytes were counted beside it for the table's schema frame, which the
    /// chunk staged may hold already.
    Batch {
        segment: SegmentId,
        table: u32,
        frame: Bytes,
        schema: u64,
        held: Permit,
    },
    /// A seal frame of `segment`, and the memory it holds until it is written.
    Seal {
        segment: SegmentId,
        frame: Bytes,
        held: Permit,
    },
    /// The frame of commit `seq` of `segments`, and the memory it holds until it is written:
    /// its chunk is published, then the commit answered; the next frame starts a new chunk.
    Commit {
        seq: CommitSeq,
        segments: SegmentSet,
        frame: Bytes,
        held: Permit,
        durable: oneshot::Sender<Result<(), Error>>,
    },
    /// Commit `seq` has its receipt, which the next chunk published records.
    Committed { seq: CommitSeq },
    /// A segment its partition ended without sealing, which no commit takes: settled as a
    /// committed one is, so it holds no chunk back.
    Abandon { segment: SegmentId },
    /// Tables whose schema frames no later batch frame names: the writer keeps them no more.
    Retire { tables: Vec<u32> },
    /// A batch finds the log full: chunks are published between commits while each lets the
    /// log hold less, and `done` answered.
    Relieve {
        done: oneshot::Sender<Result<(), Error>>,
    },
    /// The load stopped: a closing chunk is published, then the log deleted where every commit
    /// in it has a receipt.
    Close {
        done: oneshot::Sender<Result<(), Error>>,
    },
}

/// What a load's writer and its senders share: what the log holds on disk, and its first
/// failure.
pub(crate) struct Shared {
    /// Bytes: what the log holds on disk, its chunks published and the chunk staged, and what was
    /// counted for frames not yet written; it never passes `limit`.
    pub(crate) held: AtomicU64,
    /// Bytes: what the log may hold on disk.
    pub(crate) limit: AtomicU64,
    /// Bytes: what the writer needs to end the chunk staged, or a chunk it stages to free room:
    /// a header, an end naming every chunk and commit the log holds, and a closing frame.
    pub(crate) closing: AtomicU64,
    /// Bytes: the most a commit of the load wrote, its seal, phase and commit frames, and at
    /// least an eighth of `limit`, which a batch keeps room for, so the commit its checkpoint
    /// brings can be written.
    pub(crate) committed: AtomicU64,
    /// The first failure, which every later batch and command is answered with.
    pub(crate) failed: parking_lot::Mutex<Option<Error>>,
    /// The oldest commit a replay of the log may repeat: one waiting for its receipt, or whose
    /// receipt no published chunk records yet; none where there is none.
    pub(crate) oldest: parking_lot::Mutex<Option<CommitSeq>>,
    /// Rung once the log holds less, or has failed: a batch waiting for room looks again.
    pub(crate) room: tokio::sync::Notify,
}

impl Default for Shared {
    fn default() -> Self {
        Self {
            held: AtomicU64::new(0),
            limit: AtomicU64::new(u64::MAX),
            closing: AtomicU64::new(0),
            committed: AtomicU64::new(0),
            failed: parking_lot::Mutex::default(),
            oldest: parking_lot::Mutex::default(),
            room: tokio::sync::Notify::new(),
        }
    }
}

impl Shared {
    /// The failure every command answers with once one failed.
    pub(crate) fn failure(&self) -> Result<(), Error> {
        match &*self.failed.lock() {
            Some(failed) => Err(Error::wal_failed_before(failed)),
            None => Ok(()),
        }
    }

    /// Counts `bytes` against what the log may hold where it holds them with `beside` bytes to
    /// spare: whether it did.
    pub(crate) fn reserve(&self, bytes: u64, beside: u64) -> bool {
        let limit = self.limit.load(Ordering::SeqCst);
        self.held
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                let held = held.checked_add(bytes)?;
                (held.saturating_add(beside) <= limit).then_some(held)
            })
            .is_ok()
    }

    /// Gives back `bytes` counted for frames never written.
    pub(crate) fn release(&self, bytes: u64) {
        // The update only lowers the count, so it never fails.
        self.held
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                Some(held.saturating_sub(bytes))
            })
            .ok();
    }

    /// Bytes: the room every reservation of the log's frames keeps: what ends the chunk staged,
    /// and beside it what each of `kept` names.
    pub(crate) fn kept(&self, kept: Kept) -> u64 {
        let closing = self.closing.load(Ordering::SeqCst);
        // A carry may copy an eighth of the log, so it can gather many small chunks into one.
        let carry = self.limit.load(Ordering::SeqCst) / LOG_PARTS;
        let committed = self.committed.load(Ordering::SeqCst);
        match kept {
            Kept::Closing => closing,
            Kept::Commit => closing.saturating_add(committed),
            Kept::Carry => closing.saturating_add(carry),
            Kept::All => closing.saturating_add(carry).saturating_add(committed),
        }
    }
}

/// What a reservation keeps room for beside what ends the chunk staged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kept {
    /// Nothing more.
    Closing,
    /// The frames of a commit as large as the largest yet.
    Commit,
    /// A carry of one chunk.
    Carry,
    /// Both.
    All,
}

/// The sending end of a load's writer.
#[derive(Clone)]
pub(crate) struct WalWriter {
    commands: mpsc::Sender<Command>,
    shared: Arc<Shared>,
}

impl WalWriter {
    /// The writer of `owner`'s log in `store`, and the task that writes it, for the caller's
    /// scope to run; the task ends once every sender is dropped or the log is closed.
    ///
    /// What it reads back of the log to carry is held in `budget`'s share for logs.
    pub(crate) fn start(
        store: Arc<dyn WalStore>,
        owner: Owner,
        budget: MemoryBudget,
    ) -> (
        Self,
        impl Future<Output = Result<(), Error>> + Send + 'static,
    ) {
        let (commands, receiver) = mpsc::channel(QUEUED);
        let shared = Arc::<Shared>::default();
        let log = Log {
            shared: Arc::clone(&shared),
            store,
            owner,
            budget,
            chunk: 0,
            staged: None,
            tables: BTreeMap::new(),
            describing: BTreeMap::new(),
            written: BTreeMap::new(),
            pending: BTreeMap::new(),
            unrecorded: BTreeSet::new(),
            settled: Settled::default(),
            sealing: false,
            last: None,
        };
        let mut log = log;
        log.note_room();
        (Self { commands, shared }, log.run(receiver))
    }

    /// What the writer and its senders share.
    pub(crate) fn shared(&self) -> &Arc<Shared> {
        &self.shared
    }

    /// Sends `command`, waiting while the writer is behind.
    pub(crate) async fn send(&self, command: Command) -> Result<(), Error> {
        self.commands
            .send(command)
            .await
            .map_err(|_| Error::wal("the write-ahead log's writer stopped"))
    }
}

/// The segments whose frames no replay needs: committed, or abandoned by their partition.
///
/// A settled segment is forgotten once no chunk holds its frames, so a load that commits for ever
/// keeps only those of the chunks it has not deleted.
#[derive(Default)]
struct Settled(BTreeSet<SegmentId>);

impl Settled {
    fn settle(&mut self, segments: impl IntoIterator<Item = SegmentId>) {
        self.0.extend(segments);
    }

    fn contains(&self, segment: SegmentId) -> bool {
        self.0.contains(&segment)
    }

    /// Forgets each segment no chunk in `written` holds: sealed, it never gains a frame again.
    fn forget_unwritten(&mut self, written: &BTreeMap<u64, Written>) {
        self.0.retain(|segment| {
            written
                .values()
                .any(|chunk| chunk.segments.contains(segment))
        });
    }
}

/// What one chunk of the log holds.
#[derive(Default)]
struct Written {
    /// Bytes: what was written to it.
    len: u64,
    /// Bytes: what its batch frames of segments not settled take.
    open: u64,
    /// Where each table's schema frame lies in it.
    schemas: BTreeMap<u32, Span>,
    /// The segments with frames in it.
    segments: BTreeSet<SegmentId>,
    /// Its batch frames, in order.
    batches: Vec<Logged>,
    /// The commits whose frames it holds.
    commits: BTreeSet<CommitSeq>,
    /// Whether the frames of its open segments were carried to a later chunk: it is needed no
    /// more once that chunk is published.
    carried: bool,
}

/// Where a frame lies in its chunk: its offset and its length.
#[derive(Clone, Copy, Debug)]
struct Span {
    offset: u64,
    len: u64,
}

/// A batch frame in a chunk: its segment, its table, and where it lies.
#[derive(Clone, Copy, Debug)]
struct Logged {
    segment: SegmentId,
    table: u32,
    span: Span,
}

/// The writer's state: the chunk it stages, and what every chunk it has not deleted holds.
struct Log {
    store: Arc<dyn WalStore>,
    owner: Owner,
    /// What frames read back to carry are held in while they are copied.
    budget: MemoryBudget,
    /// The number of the chunk staged, or staged next.
    chunk: u64,
    staged: Option<Box<dyn StagedChunk>>,
    tables: BTreeMap<u32, Bytes>,
    /// What holds each table's schema frame until it is first written.
    describing: BTreeMap<u32, Permit>,
    written: BTreeMap<u64, Written>,
    /// The segments of each commit without a receipt.
    pending: BTreeMap<CommitSeq, SegmentSet>,
    /// The commits with receipts that no chunk published since records.
    unrecorded: BTreeSet<CommitSeq>,
    /// The segments of commits with receipts, and those abandoned, still in a chunk.
    settled: Settled,
    /// Whether the chunk staged holds seals, which go with the commit that follows them: it is
    /// published by that commit alone.
    sealing: bool,
    /// The last commit written, which the next follows.
    last: Option<CommitSeq>,
    /// What the log holds on disk, and its first failure: after a failed write, what the log
    /// holds is unknown, and no later frame may be trusted to follow it.
    shared: Arc<Shared>,
}

impl Log {
    async fn run(mut self, mut receiver: mpsc::Receiver<Command>) -> Result<(), Error> {
        while let Some(command) = receiver.recv().await {
            let close = matches!(command, Command::Close { .. });
            self.handle(command).await;
            if close {
                break;
            }
        }
        // What was staged and never published goes, giving back the room it took.
        self.discard().await;
        Ok(())
    }

    /// Deletes the chunk staged, where there is one: after a failed write nothing of it is
    /// published, and a full disk is given back what it held.
    async fn discard(&mut self) {
        if let Some(staged) = self.staged.take() {
            drop(staged.discard().await);
        }
    }

    async fn handle(&mut self, command: Command) {
        match command {
            Command::Table { index, frame, held } => {
                self.tables.insert(index, frame);
                self.describing.insert(index, held);
            }
            Command::Batch {
                segment,
                table,
                frame,
                schema,
                held,
            } => {
                let result = self.batch(segment, table, frame, schema).await;
                drop(held);
                self.note(&result);
            }
            Command::Seal {
                segment,
                frame,
                held,
            } => {
                self.seal(segment, frame).await;
                drop(held);
            }
            Command::Commit {
                seq,
                segments,
                frame,
                held,
                durable,
            } => {
                let result = self.commit(seq, segments, frame).await;
                drop(held);
                self.answer(result, durable);
            }
            Command::Committed { seq } => {
                let result = self.committed(seq).await;
                self.note(&result);
            }
            Command::Abandon { segment } => {
                self.settled.settle([segment]);
                self.note_room();
            }
            Command::Retire { tables } => {
                for table in tables {
                    self.tables.remove(&table);
                    drop(self.describing.remove(&table));
                }
            }
            Command::Relieve { done } => {
                let result = self.relieve().await;
                self.answer(result, done);
            }
            Command::Close { done } => {
                let result = self.close().await;
                self.answer(result, done);
            }
        }
    }

    /// Writes `frame`, the seal frame of `segment`, counted with its commit, which takes it.
    async fn seal(&mut self, segment: SegmentId, frame: Bytes) {
        let result = self.written_out(frame).await.map(drop);
        self.note(&result);
        self.sealing = true;
        self.current().segments.insert(segment);
        self.note_room();
    }

    /// Keeps the first failure, and answers with `result`.
    fn answer<T>(&mut self, result: Result<T, Error>, to: oneshot::Sender<Result<T, Error>>) {
        self.note(&result);
        drop(to.send(result));
    }

    /// Keeps the first failure.
    fn note<T>(&mut self, result: &Result<T, Error>) {
        let mut failed = self.shared.failed.lock();
        if let (Err(error), None) = (result, &*failed) {
            *failed = Some(Error::wal_failed_before(error));
            self.shared.room.notify_waiters();
        }
    }

    /// The failure every command answers with once one failed.
    fn failure(&self) -> Result<(), Error> {
        self.shared.failure()
    }

    fn current(&mut self) -> &mut Written {
        self.written.entry(self.chunk).or_default()
    }

    /// Writes `frame`, one of the writer's own, to the chunk staged, as [`Log::written_out`]
    /// does, counting it first: it comes out of the room every other frame keeps for it.
    async fn append(&mut self, frame: Bytes) -> Result<Span, Error> {
        self.take(count(frame.len()));
        self.written_out(frame).await
    }

    /// Counts `bytes` of the writer's own frames, out of the room every other frame keeps.
    fn take(&self, bytes: u64) {
        self.shared.held.fetch_add(bytes, Ordering::SeqCst);
    }

    /// Writes `frame`, whose bytes were counted already, to the chunk staged, staging it with
    /// its preamble and header where it is the first: where it lies there.
    async fn written_out(&mut self, frame: Bytes) -> Result<Span, Error> {
        self.failure()?;
        let chunk = Chunk {
            load: self.owner.load,
            number: self.chunk,
        };
        if self.staged.is_none() {
            let mut staged = self
                .store
                .stage(&self.owner.pipeline, chunk)
                .await
                .map_err(|error| self.lost(error))?;
            let mut head = frame::preamble().to_vec();
            head.extend_from_slice(&self.header(self.chunk)?);
            let len = count(head.len());
            self.take(len);
            if let Err(error) = staged.append(Bytes::from(head)).await {
                drop(staged.discard().await);
                return Err(Error::from_wal(error));
            }
            self.written.entry(self.chunk).or_default().len = len;
            self.staged = Some(staged);
        }
        let staged = self
            .staged
            .as_mut()
            .ok_or_else(|| Error::internal("a chunk staged is gone"))?;
        crash_point!("engine.wal.append");
        if let Err(error) = staged.append(frame.clone()).await {
            self.discard().await;
            return Err(Error::from_wal(error));
        }
        let current = self.current();
        let span = Span {
            offset: current.len,
            len: count(frame.len()),
        };
        current.len += span.len;
        Ok(span)
    }

    /// The error for `error` of the store: a log found removed, or a chunk name found taken, was
    /// taken over by a replay.
    fn lost(&self, error: std::io::Error) -> Error {
        match error.kind() {
            std::io::ErrorKind::NotFound | std::io::ErrorKind::AlreadyExists => {
                Error::wal_fenced(self.owner.load)
            }
            _ => Error::from_wal(error),
        }
    }

    /// The header frame of chunk `chunk`.
    fn header(&self, chunk: u64) -> Result<Bytes, Error> {
        Frame::Header(Header {
            pipeline: self.owner.pipeline.clone(),
            load: self.owner.load,
            chunk,
            epoch: self.owner.epoch,
            opened: self.owner.opened,
            origin: self.owner.origin,
        })
        .encode()
    }

    /// Writes `frame`, a batch frame of `segment` for the table at `table`, counted with
    /// `schema` bytes beside it for the table's schema frame, written first where the chunk
    /// staged lacks it and given back otherwise.
    async fn batch(
        &mut self,
        segment: SegmentId,
        table: u32,
        frame: Bytes,
        schema: u64,
    ) -> Result<(), Error> {
        if self.current().schemas.contains_key(&table) {
            self.shared.release(schema);
        } else {
            let frame = self.tables.get(&table).cloned().ok_or_else(|| {
                Error::internal(format!(
                    "a batch of table {table}, whose schema was never sent"
                ))
            })?;
            self.describe(table, frame).await?;
            // Written once, the frame is the writer's to keep for the chunks after.
            drop(self.describing.remove(&table));
        }
        let span = self.written_out(frame).await?;
        let current = self.current();
        current.open += span.len;
        current.segments.insert(segment);
        current.batches.push(Logged {
            segment,
            table,
            span,
        });
        self.note_room();
        Ok(())
    }

    /// Writes `frame`, the schema frame of `table`, counted already, to the chunk staged.
    async fn describe(&mut self, table: u32, frame: Bytes) -> Result<(), Error> {
        let span = self.written_out(frame).await?;
        self.current().schemas.insert(table, span);
        Ok(())
    }

    /// Writes the commit's `frame` and the chunk's end, publishes the chunk, then deletes the
    /// chunks it leaves unneeded.
    async fn commit(
        &mut self,
        seq: CommitSeq,
        segments: SegmentSet,
        frame: Bytes,
    ) -> Result<(), Error> {
        self.written_out(frame).await?;
        self.last = Some(seq);
        self.current().commits.insert(seq);
        self.publish().await?;
        self.sealing = false;
        self.pending.insert(seq, segments);
        self.note_oldest();
        Ok(())
    }

    /// Notes the oldest commit a replay of the log may repeat.
    fn note_oldest(&self) {
        let oldest = self.pending.keys().chain(&self.unrecorded).min().copied();
        *self.shared.oldest.lock() = oldest;
    }

    /// Notes commit `seq`'s receipt, and carries open segments out of chunks it leaves holding
    /// little else.
    async fn committed(&mut self, seq: CommitSeq) -> Result<(), Error> {
        self.failure()?;
        crash_point!("engine.receipt.after");
        if let Some(segments) = self.pending.remove(&seq) {
            self.settled.settle(segments.iter());
            self.unrecorded.insert(seq);
            self.note_oldest();
        }
        self.note_room();
        self.carry().await?;
        self.note_room();
        Ok(())
    }
}

fn count(bytes: usize) -> u64 {
    u64::try_from(bytes).unwrap_or(u64::MAX)
}
