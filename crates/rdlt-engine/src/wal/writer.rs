//! The task that owns a load's write-ahead log: it appends frames in the order they are sent,
//! makes a commit's frame durable before answering, and removes chunks nothing waits for.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use rdlt_connector::{CommitSeq, LoadId, Permit, PipelineId, SegmentId, SegmentSet};
use tokio::sync::{mpsc, oneshot};

use super::store::{Chunk, Claim, WalStore};
use crate::crash::crash_point;
use crate::error::Error;

/// Frames queued for the writer before a sender waits: a batch's frame holds its whole batch.
const QUEUED: usize = 16;

/// A frame for the writer, encoded, with what it says about the log.
pub(crate) enum Command {
    /// A table's schema frame, appended to each chunk before the first batch of the table in it,
    /// and the memory it holds until it is first appended.
    Table {
        index: u32,
        frame: Bytes,
        held: Permit,
    },
    /// A batch frame of `segment`, for the table at `table`, and the memory it holds until it
    /// is appended.
    Batch {
        segment: SegmentId,
        table: u32,
        frame: Bytes,
        held: Permit,
    },
    /// A seal frame of `segment`, and the memory it holds until it is appended.
    Seal {
        segment: SegmentId,
        frame: Bytes,
        held: Permit,
    },
    /// The frame of commit `seq` of `segments`, and the memory it holds until it is appended:
    /// appended, made durable, then answered; the chunk after it starts a new one.
    Commit {
        seq: CommitSeq,
        segments: SegmentSet,
        frame: Bytes,
        held: Permit,
        durable: oneshot::Sender<Result<(), Error>>,
    },
    /// The receipt frame of commit `seq`.
    Committed { seq: CommitSeq, frame: Bytes },
    /// A segment its partition ended without sealing, which no commit takes: settled as a
    /// committed one is, so it holds no chunk back.
    Abandon { segment: SegmentId },
    /// Tables whose schema frames no later batch frame names: the writer keeps them no more.
    Retire { tables: Vec<u32> },
    /// The closing frame: appended and made durable, then the log is removed where every commit
    /// in it has a receipt.
    Close {
        frame: Bytes,
        done: oneshot::Sender<Result<(), Error>>,
    },
}

/// The sending end of a load's writer.
#[derive(Clone)]
pub(crate) struct WalWriter {
    commands: mpsc::Sender<Command>,
}

impl WalWriter {
    /// The writer of `load`'s log of `pipeline` in `store`, which `claim` holds, whose chunks
    /// start with `header`, a header frame, and the task that writes it, for the caller's scope
    /// to run; the task ends, letting the claim go, once every sender is dropped or the log is
    /// closed.
    pub(crate) fn start(
        store: Arc<dyn WalStore>,
        pipeline: PipelineId,
        load: LoadId,
        header: Bytes,
        claim: Claim,
    ) -> (
        Self,
        impl Future<Output = Result<(), Error>> + Send + 'static,
    ) {
        let (commands, receiver) = mpsc::channel(QUEUED);
        let log = Log {
            store,
            pipeline,
            chunk: Chunk { load, number: 0 },
            header,
            headed: None,
            tables: BTreeMap::new(),
            describing: BTreeMap::new(),
            written: BTreeMap::new(),
            pending: BTreeMap::new(),
            settled: Settled::default(),
            failed: None,
            _claim: claim,
        };
        (Self { commands }, log.run(receiver))
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
/// keeps only those of the chunks it has not removed.
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
    /// The tables whose schema frames it holds.
    tables: BTreeSet<u32>,
    /// The segments with frames in it.
    segments: BTreeSet<SegmentId>,
    /// The commits whose frames it holds.
    commits: BTreeSet<CommitSeq>,
}

/// The writer's state: the chunk it appends to, and what every chunk it has not removed holds.
struct Log {
    store: Arc<dyn WalStore>,
    pipeline: PipelineId,
    chunk: Chunk,
    header: Bytes,
    /// The chunk whose header frame is written.
    headed: Option<u64>,
    tables: BTreeMap<u32, Bytes>,
    /// What holds each table's schema frame until it is first appended.
    describing: BTreeMap<u32, Permit>,
    written: BTreeMap<u64, Written>,
    /// The segments of each commit without a receipt.
    pending: BTreeMap<CommitSeq, SegmentSet>,
    /// The segments of commits with receipts, and those abandoned, still in a chunk.
    settled: Settled,
    /// The first failure, which every later command answers with: after a failed append or sync,
    /// what the chunk holds is unknown, and no later frame may be trusted to follow it.
    failed: Option<(String, bool)>,
    /// The claim on the log, held while the writer runs: its drop lets the log go.
    _claim: Claim,
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
        Ok(())
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
                let result = self.append(frame).await;
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
            Command::Committed { seq, frame } => {
                let result = self.committed(seq, frame).await;
                self.note(&result);
            }
            Command::Abandon { segment } => self.settled.settle([segment]),
            Command::Retire { tables } => {
                for table in tables {
                    self.tables.remove(&table);
                    drop(self.describing.remove(&table));
                }
            }
            Command::Close { frame, done } => {
                let result = self.close(frame).await;
                self.note(&result);
                drop(done.send(result));
            }
        }
    }

    /// Keeps the first failure.
    fn note(&mut self, result: &Result<(), Error>) {
        if let (Err(error), None) = (result, &self.failed) {
            self.failed = Some((error.to_string(), error.is_retryable()));
        }
    }

    fn current(&mut self) -> &mut Written {
        self.written.entry(self.chunk.number).or_default()
    }

    async fn append(&mut self, frame: Bytes) -> Result<(), Error> {
        if let Some((failed, retryable)) = &self.failed {
            return Err(Error::wal_failed_before(failed, *retryable));
        }
        if self.headed != Some(self.chunk.number) {
            self.current();
            self.store
                .append(&self.pipeline, self.chunk, self.header.clone())
                .await
                .map_err(Error::from_wal)?;
            self.headed = Some(self.chunk.number);
        }
        crash_point!("engine.wal.append");
        self.store
            .append(&self.pipeline, self.chunk, frame)
            .await
            .map_err(Error::from_wal)
    }

    async fn batch(&mut self, segment: SegmentId, table: u32, frame: Bytes) -> Result<(), Error> {
        if !self.current().tables.contains(&table) {
            let schema = self.tables.get(&table).cloned().ok_or_else(|| {
                Error::internal(format!(
                    "a batch of table {table}, whose schema was never sent"
                ))
            })?;
            self.append(schema).await?;
            // Appended once, the frame is the writer's to keep for the chunks after.
            drop(self.describing.remove(&table));
            self.current().tables.insert(table);
        }
        self.append(frame).await?;
        self.current().segments.insert(segment);
        Ok(())
    }

    async fn commit(
        &mut self,
        seq: CommitSeq,
        segments: SegmentSet,
        frame: Bytes,
    ) -> Result<(), Error> {
        self.append(frame).await?;
        crash_point!("engine.wal.sync.before");
        self.store
            .sync(&self.pipeline, self.chunk)
            .await
            .map_err(Error::from_wal)?;
        crash_point!("engine.wal.sync.after");
        self.current().commits.insert(seq);
        self.pending.insert(seq, segments);
        self.chunk.number += 1;
        Ok(())
    }

    async fn committed(&mut self, seq: CommitSeq, frame: Bytes) -> Result<(), Error> {
        self.append(frame).await?;
        crash_point!("engine.receipt.after");
        if let Some(segments) = self.pending.remove(&seq) {
            self.settled.settle(segments.iter());
        }
        self.remove_done().await?;
        self.settled.forget_unwritten(&self.written);
        Ok(())
    }

    /// Removes each chunk before the current one whose segments and commits are all committed.
    async fn remove_done(&mut self) -> Result<(), Error> {
        let done: Vec<u64> = self
            .written
            .iter()
            .filter(|(number, _)| **number < self.chunk.number)
            .filter(|(_, written)| {
                written
                    .segments
                    .iter()
                    .all(|segment| self.settled.contains(*segment))
                    && written
                        .commits
                        .iter()
                        .all(|seq| !self.pending.contains_key(seq))
            })
            .map(|(number, _)| *number)
            .collect();
        for number in done {
            let chunk = Chunk {
                load: self.chunk.load,
                number,
            };
            self.store
                .remove(&self.pipeline, chunk)
                .await
                .map_err(Error::from_wal)?;
            self.written.remove(&number);
            crash_point!("engine.wal.remove");
        }
        Ok(())
    }

    /// Closes the log: the closing frame made durable, then every chunk removed where no commit
    /// waits for a receipt.
    async fn close(&mut self, frame: Bytes) -> Result<(), Error> {
        self.append(frame).await?;
        crash_point!("engine.wal.close.before");
        self.store
            .sync(&self.pipeline, self.chunk)
            .await
            .map_err(Error::from_wal)?;
        crash_point!("engine.wal.close.after");
        if !self.pending.is_empty() {
            return Ok(());
        }
        self.written.clear();
        self.store
            .remove_log(&self.pipeline, self.chunk.load)
            .await
            .map_err(Error::from_wal)?;
        crash_point!("engine.wal.removed");
        Ok(())
    }
}
