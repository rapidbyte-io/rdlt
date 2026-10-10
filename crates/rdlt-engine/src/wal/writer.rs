//! The task that owns a load's write-ahead log: it stages frames in the order they are sent,
//! publishes the chunk they make at each commit before answering, and deletes the chunks nothing
//! needs any more.

mod carry;
mod chunk;
mod publish;
mod relief;
mod shared;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use rdlt_connector::{CommitSeq, Epoch, LoadId, Permit, PipelineId, SegmentId, SegmentSet};
use tokio::sync::{mpsc, oneshot};

use self::chunk::{Settled, Written};
pub(crate) use self::shared::{Kept, Shared};
use super::store::{StagedChunk, WalStore};
use crate::crash::crash_point;
use crate::error::Error;
use crate::report::Tally;

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
    /// chunk staged may hold already, and `closing` bytes for ending the chunk staged where the
    /// frame would take it past what a carry may copy.
    Batch {
        segment: SegmentId,
        table: u32,
        frame: Bytes,
        schema: u64,
        closing: u64,
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

/// The sending end of a load's writer.
#[derive(Clone)]
pub(crate) struct WalWriter {
    commands: mpsc::Sender<Command>,
    shared: Arc<Shared>,
}

impl WalWriter {
    /// The writer of `owner`'s log in `store`, which may hold `limit` bytes, and the task that
    /// writes it, for the caller's scope to run, counting what it carries and relieves into
    /// `tally`; the task ends once every sender is dropped or the log is closed.
    pub(crate) fn start(
        store: Arc<dyn WalStore>,
        owner: Owner,
        limit: u64,
        tally: Arc<Tally>,
    ) -> (
        Self,
        impl Future<Output = Result<(), Error>> + Send + 'static,
    ) {
        let (commands, receiver) = mpsc::channel(QUEUED);
        let shared = Arc::new(Shared::new(limit));
        let log = Log::new(store, owner, Arc::clone(&shared), tally);
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

impl Log {
    /// The writer of `owner`'s log in `store`, sharing `shared` and counting into `tally`,
    /// before anything is written.
    fn new(store: Arc<dyn WalStore>, owner: Owner, shared: Arc<Shared>, tally: Arc<Tally>) -> Self {
        let mut log = Self {
            shared,
            tally,
            store,
            owner,
            chunk: 0,
            staged: None,
            tables: BTreeMap::new(),
            describing: BTreeMap::new(),
            written: BTreeMap::new(),
            holders: BTreeMap::new(),
            pending: BTreeMap::new(),
            unrecorded: BTreeSet::new(),
            settled: Settled::default(),
            sealing: false,
            held_back: VecDeque::new(),
            last: None,
            spare: 0,
        };
        log.note_room();
        log
    }
}

/// The writer's state: the chunk it stages, and what every chunk it has not deleted holds.
struct Log {
    store: Arc<dyn WalStore>,
    owner: Owner,
    /// The number of the chunk staged, or staged next.
    chunk: u64,
    staged: Option<Box<dyn StagedChunk>>,
    tables: BTreeMap<u32, Bytes>,
    /// What holds each table's schema frame until it is first written.
    describing: BTreeMap<u32, Permit>,
    written: BTreeMap<u64, Written>,
    /// The chunks holding frames of each segment.
    holders: BTreeMap<SegmentId, BTreeSet<u64>>,
    /// The segments of each commit without a receipt.
    pending: BTreeMap<CommitSeq, SegmentSet>,
    /// The commits with receipts that no chunk published since records.
    unrecorded: BTreeSet<CommitSeq>,
    /// The segments of commits with receipts, and those abandoned, still in a chunk.
    settled: Settled,
    /// Whether the chunk staged holds seals, which go with the commit that follows them: it is
    /// published by that commit alone, and holds no other frame before it.
    sealing: bool,
    /// The commands that came while the chunk staged held seals, handled in turn once their
    /// commit is published.
    held_back: VecDeque<Command>,
    /// The last commit written, which the next follows.
    last: Option<CommitSeq>,
    /// Bytes: what the batch being written counted for the writer's own frames and they have
    /// not taken yet.
    spare: u64,
    /// What the log holds on disk, and its first failure: after a failed write, what the log
    /// holds is unknown, and no later frame may be trusted to follow it.
    shared: Arc<Shared>,
    /// Where what the writer carries and relieves is counted.
    tally: Arc<Tally>,
}

impl Log {
    async fn run(mut self, mut receiver: mpsc::Receiver<Command>) -> Result<(), Error> {
        while let Some(command) = receiver.recv().await {
            if self.holds_back(&command) {
                self.held_back.push_back(command);
                continue;
            }
            let close = matches!(command, Command::Close { .. });
            self.handle(command).await;
            if close {
                break;
            }
            while !self.holding() {
                let Some(command) = self.held_back.pop_front() else {
                    break;
                };
                self.handle(command).await;
            }
        }
        // What was staged and never published goes, giving back the room it took.
        self.discard().await;
        Ok(())
    }

    /// Whether the chunk staged holds seals and the log has not failed: no frame but the seals'
    /// commit's goes in it, so its open frames fit what a carry may copy.
    fn holding(&self) -> bool {
        self.sealing && self.failure().is_ok()
    }

    /// Whether `command` waits for the commit of the seals the chunk staged holds: a batch frame,
    /// or what may carry frames into the chunk staged, publish it, or name a table or segment a
    /// batch frame held back names.
    ///
    /// A close is not held back: one that comes while seals are staged came without their
    /// commit, and fails the log.
    fn holds_back(&self, command: &Command) -> bool {
        self.holding()
            && matches!(
                command,
                Command::Batch { .. }
                    | Command::Committed { .. }
                    | Command::Abandon { .. }
                    | Command::Retire { .. }
            )
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
            Command::Table { index, frame, held } => self.table(index, frame, held),
            Command::Batch {
                segment,
                table,
                frame,
                schema,
                closing,
                held,
            } => {
                self.spare = closing;
                let result = self.batch(segment, table, frame, schema).await;
                self.shared.release(std::mem::take(&mut self.spare));
                drop(held);
                self.note(&result);
            }
            Command::Seal {
                segment,
                frame,
                held,
            } => {
                let result = self.seal(segment, frame).await;
                drop(held);
                self.note(&result);
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
            Command::Abandon { segment } => self.abandon(segment),
            Command::Retire { tables } => self.retire(tables),
            Command::Relieve { done } => {
                let result = self.relieve().await;
                self.answer(result, done);
            }
            Command::Close { done } => {
                let result = self.closed().await;
                self.answer(result, done);
            }
        }
    }

    /// Writes `frame`, the seal of `segment`: the chunk staged is published by the commit that
    /// takes it alone.
    ///
    /// The schema frames no batch frame took yet are the writer's to keep from the first seal on,
    /// their memory released: the batches that would take them wait for the commit, and its
    /// frames need that memory.
    async fn seal(&mut self, segment: SegmentId, frame: Bytes) -> Result<(), Error> {
        let result = self.written_out(frame).await.map(drop);
        self.sealing = true;
        self.describing.clear();
        self.holds(segment);
        result
    }

    /// Keeps `frame`, the schema frame of the table at `index`, and the memory `held` for it
    /// until it is first written.
    ///
    /// While seals are staged, the batch that would write it waits for their commit, whose frames
    /// need that memory: the frame is the writer's to keep from now on.
    fn table(&mut self, index: u32, frame: Bytes, held: Permit) {
        self.tables.insert(index, frame);
        if self.holding() {
            drop(held);
        } else {
            self.describing.insert(index, held);
        }
    }

    /// Closes the log, where no seals are staged: a close that comes while they are came without
    /// their commit, and fails the log.
    async fn closed(&mut self) -> Result<(), Error> {
        if self.holding() {
            return Err(Error::internal(
                "the write-ahead log was closed after seals no commit followed",
            ));
        }
        self.close().await
    }

    /// Settles `segment`, which its partition ended without sealing.
    fn abandon(&mut self, segment: SegmentId) {
        self.settle([segment]);
        self.note_room();
    }

    /// Keeps the schema frames of `tables` no more.
    fn retire(&mut self, tables: Vec<u32>) {
        for table in tables {
            self.tables.remove(&table);
            drop(self.describing.remove(&table));
        }
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
            self.settle(segments.iter());
            self.unrecorded.insert(seq);
            self.note_oldest();
        }
        self.carry().await?;
        self.note_room();
        Ok(())
    }
}
