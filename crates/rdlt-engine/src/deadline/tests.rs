#![expect(
    clippy::disallowed_methods,
    reason = "tests drive tokio's paused clock directly"
)]

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use arrow_array::RecordBatch;
use rdlt_connector::{
    BoxFuture, Capabilities, CommitMeta, CommitSeq, ConnectorErrorKind, DEADLINE_EXCEEDED,
    Destination, DestinationSession, DestinationWriter, Epoch, LoadId, OpenContext, OpenedSession,
    PipelineId, Receipt, Result, SchemaVersion, SegmentId, SegmentSet, TableChange, TablePath,
    TableRef, WriteStats,
};
use tokio::time::Instant;

use super::Waits;
use crate::compute::RayonPool;
use crate::env::SystemEnv;

const WAIT: Duration = Duration::from_secs(30);

/// A destination whose open answers at once, and whose every other call answers once `delay`
/// has passed, or never where `None`.
struct Slow {
    delay: Option<Duration>,
    capabilities: Capabilities,
}

async fn after(delay: Option<Duration>) {
    match delay {
        Some(delay) => tokio::time::sleep(delay).await,
        None => std::future::pending().await,
    }
}

impl Destination for Slow {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async {
            after(self.delay).await;
            Ok(())
        })
    }

    fn open<'a>(&'a self, _context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        Box::pin(async move {
            Ok(OpenedSession {
                session: Box::new(SlowSession(self.delay)),
                epoch: Epoch(1),
                state: Vec::new(),
            })
        })
    }
}

struct SlowSession(Option<Duration>);

impl DestinationSession for SlowSession {
    fn apply_schema<'a>(&'a mut self, _change: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        Box::pin(async {
            after(self.0).await;
            Ok(())
        })
    }

    fn writer<'a>(
        &'a mut self,
        _table: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        Box::pin(async { Ok(Box::new(SlowWriter(self.0)) as Box<dyn DestinationWriter>) })
    }

    fn commit<'a>(&'a mut self, meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        Box::pin(async move {
            after(self.0).await;
            Ok(Receipt {
                load_id: meta.load_id,
                commit_seq: meta.commit_seq,
                committed_at: UNIX_EPOCH,
                rows: 0,
                bytes: 0,
            })
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            after(self.0).await;
            Ok(())
        })
    }
}

struct SlowWriter(Option<Duration>);

impl DestinationWriter for SlowWriter {
    fn write(&mut self, _segment: SegmentId, _batch: RecordBatch) -> BoxFuture<'_, Result<()>> {
        Box::pin(async {
            after(self.0).await;
            Ok(())
        })
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<WriteStats>> {
        Box::pin(async {
            after(self.0).await;
            Ok(WriteStats::default())
        })
    }
}

fn waited(delay: Option<Duration>) -> Arc<dyn Destination> {
    let pool = RayonPool::new(NonZeroUsize::MIN).unwrap();
    let waits = Waits::new(Arc::new(SystemEnv::new(pool)), WAIT);
    waits.destination(Arc::new(Slow {
        delay,
        capabilities: Capabilities::minimal(),
    }))
}

fn context() -> OpenContext {
    OpenContext {
        pipeline: PipelineId::parse("waits").unwrap(),
        load_id: LoadId::from_parts(UNIX_EPOCH, 1),
    }
}

fn meta() -> CommitMeta {
    CommitMeta {
        load_id: LoadId::from_parts(UNIX_EPOCH, 1),
        commit_seq: CommitSeq::FIRST,
        epoch: Epoch(1),
        segments: SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
        horizon: None,
    }
}

fn table() -> TableRef {
    TableRef {
        path: TablePath::new(["t"]).unwrap(),
        name: "t".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    }
}

fn batch() -> RecordBatch {
    RecordBatch::new_empty(Arc::new(arrow_schema::Schema::empty()))
}

/// Each call of `destination`'s session and of a writer of it, in turn, with how long each took.
async fn calls(destination: &Arc<dyn Destination>) -> Vec<(Result<()>, Duration)> {
    let mut opened = destination.open(&context()).await.unwrap();
    let mut taken = Vec::new();
    let mut time = |outcome: Result<()>, started: Instant| {
        taken.push((outcome, started.elapsed()));
    };
    let started = Instant::now();
    time(destination.check().await, started);
    let started = Instant::now();
    let change = TableChange::Create {
        table: table(),
        schema: rdlt_connector::TableSchema::new(Vec::new()).unwrap(),
    };
    time(opened.session.apply_schema(&change).await, started);
    let mut writer = opened.session.writer(&table()).await.unwrap();
    let started = Instant::now();
    time(writer.write(SegmentId(1), batch()).await, started);
    let started = Instant::now();
    time(writer.flush().await.map(drop), started);
    let started = Instant::now();
    time(opened.session.commit(&meta()).await.map(drop), started);
    let started = Instant::now();
    time(opened.session.close().await, started);
    taken
}

#[tokio::test(start_paused = true)]
async fn every_call_into_a_destination_but_a_read_ends_at_the_wait() {
    for (outcome, took) in calls(&waited(None)).await {
        let error = outcome.unwrap_err();
        assert_eq!(
            (error.kind(), error.code()),
            (ConnectorErrorKind::Transient, Some(DEADLINE_EXCEEDED))
        );
        assert_eq!(took, WAIT);
    }
    for (outcome, took) in calls(&waited(Some(WAIT / 2))).await {
        outcome.unwrap();
        assert_eq!(took, WAIT / 2);
    }
}

/// A source whose every call answers once `delay` has passed, or never where `None`; its reads
/// answer at once.
struct SlowSource(Option<Duration>);

impl rdlt_connector::Source for SlowSource {
    fn check(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async {
            after(self.0).await;
            Ok(())
        })
    }

    fn discover(&self) -> BoxFuture<'_, Result<rdlt_connector::Catalog>> {
        Box::pin(async {
            after(self.0).await;
            Ok(rdlt_connector::Catalog::default())
        })
    }

    fn plan<'a>(
        &'a self,
        _stream: &'a rdlt_connector::StreamName,
        _state: &'a rdlt_connector::StreamState,
    ) -> BoxFuture<'a, Result<rdlt_connector::PartitionPlan>> {
        Box::pin(async {
            after(self.0).await;
            Ok(rdlt_connector::PartitionPlan::default())
        })
    }

    fn read(
        &self,
        _request: rdlt_connector::ReadRequest,
        _sink: rdlt_connector::PartitionSink,
    ) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn committed<'a>(
        &'a self,
        _stream: &'a rdlt_connector::StreamName,
        _cursors: &'a [(rdlt_connector::PartitionId, rdlt_connector::Cursor)],
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async {
            after(self.0).await;
            Ok(())
        })
    }
}

#[tokio::test(start_paused = true)]
async fn every_call_into_a_source_but_a_read_ends_at_the_wait() {
    let pool = RayonPool::new(NonZeroUsize::MIN).unwrap();
    let waits = Waits::new(Arc::new(SystemEnv::new(pool)), WAIT);
    let stream = rdlt_connector::StreamName::new("s").unwrap();
    let state = rdlt_connector::StreamState::default();
    for (delay, answers) in [(None, false), (Some(WAIT / 2), true)] {
        let source = waits.source(Arc::new(SlowSource(delay)));
        let started = Instant::now();
        let outcomes = [
            source.check().await,
            source.discover().await.map(drop),
            source.plan(&stream, &state).await.map(drop),
            source.committed(&stream, &[]).await,
        ];
        let took = if answers { WAIT / 2 } else { WAIT };
        assert_eq!(started.elapsed(), took * 4);
        for outcome in outcomes {
            assert_eq!(outcome.is_ok(), answers, "{outcome:?}");
            if let Err(error) = outcome {
                assert_eq!(error.code(), Some(DEADLINE_EXCEEDED));
            }
        }
        // A read is never ended by the wait.
        let (sink, _feed) = rdlt_connector::partition_channel(NonZeroUsize::MIN);
        let partition = rdlt_connector::Partition::single();
        let request = rdlt_connector::ReadRequest::new(stream.clone(), partition, None);
        source.read(request, sink).await.unwrap();
    }
}
