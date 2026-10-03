//! The task that owns a load's write-ahead log: it stages frames in the order they are sent,
//! publishes the chunk they make at each commit before answering, and deletes the chunks nothing
//! needs any more.

mod carry;
mod publish;
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
use crate::crash::crash_point;
use crate::error::Error;

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
    /// is written.
    Batch {
        segment: SegmentId,
        table: u32,
        frame: Bytes,
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
    /// The load stopped: a closing chunk is published, then the log deleted where every commit
    /// in it has a receipt.
    Close {
        done: oneshot::Sender<Result<(), Error>>,
    },
}

/// What a load's writer and its senders share: what the log holds on disk, and its first
/// failure.
#[derive(Default)]
pub(crate) struct Shared {
    /// Bytes: what the log holds on disk, its chunks published and the chunk staged; a batch
    /// frame counted once it is sent, every other frame once it is written.
    pub(crate) held: AtomicU64,
    /// The first failure, which every later batch and command is answered with.
    pub(crate) failed: parking_lot::Mutex<Option<Error>>,
    /// The oldest commit a replay of the log may repeat: one waiting for its receipt, or whose
    /// receipt no published chunk records yet; none where there is none.
    pub(crate) oldest: parking_lot::Mutex<Option<CommitSeq>>,
}

impl Shared {
    /// The failure every command answers with once one failed.
    pub(crate) fn failure(&self) -> Result<(), Error> {
        match &*self.failed.lock() {
            Some(failed) => Err(Error::wal_failed_before(failed)),
            None => Ok(()),
        }
    }
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
    pub(crate) fn start(
        store: Arc<dyn WalStore>,
        owner: Owner,
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
            chunk: 0,
            staged: None,
            tables: BTreeMap::new(),
            describing: BTreeMap::new(),
            written: BTreeMap::new(),
            pending: BTreeMap::new(),
            unrecorded: BTreeSet::new(),
            settled: Settled::default(),
        };
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
                held,
            } => {
                let result = self.batch(segment, table, frame).await;
                drop(held);
                self.note(&result);
            }
            Command::Seal {
                segment,
                frame,
                held,
            } => {
                let result = self.append(frame).await.map(drop);
                drop(held);
                self.note(&result);
                self.current().segments.insert(segment);
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
                self.note(&result);
                drop(durable.send(result));
            }
            Command::Committed { seq } => {
                let result = self.committed(seq).await;
                self.note(&result);
            }
            Command::Abandon { segment } => self.settled.settle([segment]),
            Command::Retire { tables } => {
                for table in tables {
                    self.tables.remove(&table);
                    drop(self.describing.remove(&table));
                }
            }
            Command::Close { done } => {
                let result = self.close().await;
                self.note(&result);
                drop(done.send(result));
            }
        }
    }

    /// Keeps the first failure.
    fn note(&mut self, result: &Result<(), Error>) {
        let mut failed = self.shared.failed.lock();
        if let (Err(error), None) = (result, &*failed) {
            *failed = Some(Error::wal_failed_before(error));
        }
    }

    /// The failure every command answers with once one failed.
    fn failure(&self) -> Result<(), Error> {
        self.shared.failure()
    }

    fn current(&mut self) -> &mut Written {
        self.written.entry(self.chunk).or_default()
    }

    /// Writes `frame` to the chunk staged, as [`Log::written`] does, counting it on disk.
    async fn append(&mut self, frame: Bytes) -> Result<Span, Error> {
        self.shared
            .held
            .fetch_add(count(frame.len()), Ordering::Relaxed);
        self.written_out(frame).await
    }

    /// Writes `frame` to the chunk staged, staging it with its preamble and header where it is
    /// the first: where it lies there.
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
            head.extend_from_slice(&self.header()?);
            let len = count(head.len());
            self.shared.held.fetch_add(len, Ordering::Relaxed);
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

    /// The header frame of the chunk staged.
    fn header(&self) -> Result<Bytes, Error> {
        Frame::Header(Header {
            pipeline: self.owner.pipeline.clone(),
            load: self.owner.load,
            chunk: self.chunk,
            epoch: self.owner.epoch,
            opened: self.owner.opened,
        })
        .encode()
    }

    async fn batch(&mut self, segment: SegmentId, table: u32, frame: Bytes) -> Result<(), Error> {
        if !self.current().schemas.contains_key(&table) {
            let schema = self.tables.get(&table).cloned().ok_or_else(|| {
                Error::internal(format!(
                    "a batch of table {table}, whose schema was never sent"
                ))
            })?;
            self.describe(table, schema).await?;
            // Written once, the frame is the writer's to keep for the chunks after.
            drop(self.describing.remove(&table));
        }
        // Counted on disk once it was sent.
        let span = self.written_out(frame).await?;
        let current = self.current();
        current.segments.insert(segment);
        current.batches.push(Logged {
            segment,
            table,
            span,
        });
        Ok(())
    }

    /// Writes `frame`, the schema frame of `table`, to the chunk staged.
    async fn describe(&mut self, table: u32, frame: Bytes) -> Result<(), Error> {
        let span = self.append(frame).await?;
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
        self.append(frame).await?;
        self.current().commits.insert(seq);
        self.publish().await?;
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
        self.carry().await
    }
}

fn count(bytes: usize) -> u64 {
    u64::try_from(bytes).unwrap_or(u64::MAX)
}
