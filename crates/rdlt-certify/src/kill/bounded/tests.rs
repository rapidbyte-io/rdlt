use std::sync::Arc;

use arrow_array::{ArrayRef, BinaryArray, NullArray, RecordBatch};
use rdlt_connector::{
    BoxFuture, Capabilities, CommitMeta, ConnectorErrorKind, Destination, DestinationSession,
    DestinationWriter, Epoch, LoadId, OpenContext, OpenedSession, PipelineId, Receipt, Result,
    SchemaVersion, SegmentId, TableChange, TablePath, TableRef, WriteStats,
};

use super::{BEYOND, Bounded};
use crate::limits::{LOADED_BYTES, LOADED_ROWS};

/// A destination that takes every write, and counts the rows it took.
struct Sink(Capabilities, Arc<std::sync::atomic::AtomicUsize>);

struct SinkSession(Arc<std::sync::atomic::AtomicUsize>);

impl Destination for Sink {
    fn capabilities(&self) -> &Capabilities {
        &self.0
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn open<'a>(&'a self, _: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        Box::pin(async move {
            Ok(OpenedSession {
                session: Box::new(SinkSession(Arc::clone(&self.1))),
                epoch: Epoch(1),
                state: Vec::new(),
            })
        })
    }
}

impl DestinationSession for SinkSession {
    fn apply_schema<'a>(&'a mut self, _: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn writer<'a>(
        &'a mut self,
        _: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        let writer = SinkSession(Arc::clone(&self.0));
        Box::pin(async move { Ok(Box::new(writer) as Box<dyn DestinationWriter>) })
    }

    fn commit<'a>(&'a mut self, _: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        Box::pin(async { Err(rdlt_connector::ConnectorError::data("no commits")) })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

impl DestinationWriter for SinkSession {
    fn write(&mut self, _: SegmentId, batch: RecordBatch) -> BoxFuture<'_, Result<()>> {
        let taken = Arc::clone(&self.0);
        Box::pin(async move {
            taken.fetch_add(batch.num_rows(), std::sync::atomic::Ordering::SeqCst);
            Ok(())
        })
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<WriteStats>> {
        Box::pin(async { Ok(WriteStats::default()) })
    }
}

fn table() -> TableRef {
    TableRef {
        path: TablePath::new(["events"]).expect("a valid path"),
        name: "events".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    }
}

fn batch(column: ArrayRef) -> RecordBatch {
    RecordBatch::try_from_iter_with_nullable([("column", column, true)]).expect("a valid batch")
}

fn nulls(rows: usize) -> RecordBatch {
    batch(Arc::new(NullArray::new(rows)))
}

/// What a bounded destination took of `batches`, written by two writers in turn: the rows that
/// reached the destination, how each write fared, and whether the load went beyond.
async fn written(batches: Vec<RecordBatch>) -> (usize, Vec<bool>, bool) {
    let taken = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let sink = Sink(Capabilities::minimal(), Arc::clone(&taken));
    let bounded = Bounded::new(Arc::new(sink));
    let beyond = bounded.witness();
    assert_eq!(bounded.capabilities(), &Capabilities::minimal());
    bounded.check().await.expect("the destination checks");
    let context = OpenContext {
        pipeline: PipelineId::parse("bounded").expect("a valid id"),
        load_id: LoadId::from_parts(std::time::UNIX_EPOCH, 7),
    };
    let mut opened = bounded.open(&context).await.expect("it opens");
    assert_eq!(opened.epoch, Epoch(1));
    let mut writers = Vec::new();
    for _ in 0..2 {
        writers.push(opened.session.writer(&table()).await.expect("a writer"));
    }
    let mut fared = Vec::new();
    for (index, batch) in batches.into_iter().enumerate() {
        let written = writers[index % 2].write(SegmentId(1), batch).await;
        if let Err(error) = &written {
            assert_eq!(error.kind(), ConnectorErrorKind::Data);
            assert_eq!(error.code(), Some(BEYOND));
        }
        fared.push(written.is_ok());
        writers[index % 2].flush().await.expect("it flushes");
    }
    let taken = taken.load(std::sync::atomic::Ordering::SeqCst);
    (taken, fared, beyond.unobserved().is_some())
}

#[tokio::test]
async fn a_load_is_taken_up_to_its_rows_all_its_writers_together_and_no_further() {
    let half = LOADED_ROWS / 2;
    assert_eq!(
        written(vec![nulls(half), nulls(half), nulls(0)]).await,
        (LOADED_ROWS, vec![true, true, true], false)
    );
    assert_eq!(
        written(vec![nulls(half), nulls(half), nulls(1), nulls(0)]).await,
        (LOADED_ROWS, vec![true, true, false, true], true)
    );
    assert_eq!(
        written(vec![nulls(LOADED_ROWS + 1), nulls(1)]).await,
        (0, vec![false, false], true)
    );
}

#[tokio::test]
async fn a_load_is_taken_up_to_its_bytes_and_no_further() {
    // A row of a quarter of what a load takes, and the buffers that hold it.
    let quarter = || {
        batch(Arc::new(BinaryArray::from_iter_values([vec![
            0_u8;
            LOADED_BYTES
                / 4
        ]])))
    };
    let (taken, fared, beyond) = written(vec![quarter(), quarter(), quarter()]).await;
    assert_eq!((taken, fared, beyond), (3, vec![true; 3], false));
    let (taken, fared, beyond) = written((0..5).map(|_| quarter()).collect()).await;
    assert_eq!(taken, 3, "the fourth, with its buffers, is beyond");
    assert_eq!(fared, [true, true, true, false, false]);
    assert!(beyond);
}

#[tokio::test]
async fn a_bounded_destination_commits_and_closes_as_its_own_does() {
    let taken = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let bounded = Bounded::new(Arc::new(Sink(Capabilities::minimal(), taken)));
    let context = OpenContext {
        pipeline: PipelineId::parse("bounded").expect("a valid id"),
        load_id: LoadId::from_parts(std::time::UNIX_EPOCH, 7),
    };
    let mut opened = bounded.open(&context).await.expect("it opens");
    let field = rdlt_connector::Field::new("more", rdlt_connector::LogicalType::Int64, true);
    let add = TableChange::AddColumn {
        table: table(),
        field,
    };
    opened.session.apply_schema(&add).await.expect("it applies");
    let meta = CommitMeta {
        load_id: context.load_id,
        commit_seq: rdlt_connector::CommitSeq::FIRST,
        epoch: opened.epoch,
        segments: rdlt_connector::SegmentSet::default(),
        abandoned: rdlt_connector::SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
        horizon: None,
    };
    let refused = opened
        .session
        .commit(&meta)
        .await
        .expect_err("the sink commits nothing");
    assert_eq!(refused.kind(), ConnectorErrorKind::Data);
    assert_eq!(refused.code(), None);
    opened.session.close().await.expect("it closes");
}
