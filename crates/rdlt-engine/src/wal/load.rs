//! A load's write-ahead log as its attempt writes it: the tables it describes, the batches its
//! partitions write, and its commits, each made durable before the destination sees it.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

use arrow_array::RecordBatch;
use bytes::Bytes;
use rdlt_connector::{
    CommitMeta, CommitSeq, GenerationId, LoadId, PartitionId, PartitionState, PipelineId, Receipt,
    SchemaVersion, SegmentId, StreamName,
};
use tokio::sync::{Mutex, oneshot};

use super::frame::{self, Frame, Header, VERSION};
use super::store::WalStore;
use super::writer::{Command, WalWriter};
use crate::budget::MemoryBudget;
use crate::compute::{ComputePool, run_all};
use crate::error::Error;
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
    /// The index each table version's schema frame gave it, held while a new one is sent, so no
    /// batch frame of a table precedes its schema frame.
    tables: Arc<Mutex<BTreeMap<TableKey, u32>>>,
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
        };
        Ok((log, task))
    }

    /// Logs `batch` of `segment`, lowered for `view` of the attempt's table `table`, encoded on
    /// `compute`; `budget` holds the frame's bytes until it is appended.
    pub(crate) async fn batch(
        &self,
        compute: &dyn ComputePool,
        budget: &MemoryBudget,
        table: usize,
        view: &TableView,
        segment: SegmentId,
        batch: &RecordBatch,
    ) -> Result<(), Error> {
        let index = self.describe(table, view).await?;
        let batch = frame::Batch {
            segment,
            table: index,
            batch: batch.clone(),
        };
        let mut encoded = run_all(compute, [move || Frame::Batch(batch).encode()]).await;
        let frame = encoded
            .pop()
            .ok_or_else(|| Error::internal("a batch frame's job returned nothing"))??;
        let held = Box::new(budget.charge(frame.len() as u64));
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
    async fn describe(&self, table: usize, view: &TableView) -> Result<u32, Error> {
        let key = (table, view.table.version, view.table.generation);
        let mut tables = self.tables.lock().await;
        if let Some(index) = tables.get(&key) {
            return Ok(*index);
        }
        let index = u32::try_from(tables.len())
            .map_err(|_| Error::internal("a load writes more table versions than a log names"))?;
        let frame = Frame::Schema(frame::Table {
            index,
            table: view.table.clone(),
            schema: view.physical_schema(),
        })
        .encode()?;
        self.writer.send(Command::Table { index, frame }).await?;
        tables.insert(key, index);
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
    ) -> Result<(), Error> {
        // The commit settles every segment it sealed, those it publishes nothing of included, so
        // its receipt lets their chunks go.
        let mut segments = meta.segments.clone();
        for seal in &sealed {
            segments.insert(seal.segment);
        }
        for seal in sealed {
            let segment = seal.segment;
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
            let held = Box::new(budget.charge(frame.len() as u64));
            let seal = Command::Seal {
                segment,
                frame,
                held,
            };
            self.writer.send(seal).await?;
        }
        let mut frames = Vec::new();
        for begun in begun {
            frames.extend_from_slice(&Frame::Begun(begun).encode()?);
        }
        frames.extend_from_slice(&frame::commit(meta)?);
        let held = Box::new(budget.charge(frames.len() as u64));
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
