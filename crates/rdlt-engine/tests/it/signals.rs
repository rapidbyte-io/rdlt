//! What a source tells a run beside its data: that its partitions changed, how far behind its
//! reads are, and that its retention dropped where a read would resume.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use rdlt_connector::{
    BoxFuture, Catalog, ConnectContext, ConnectorError, Cursor, PartitionId, PartitionPlan,
    PartitionSink, ReadMode, ReadRequest, Source, SourceEvent, StreamName, StreamState,
    partition_channel, source_factory,
};
use rdlt_connector_reference::LogSource;
use rdlt_engine::{
    BatchPolicy, CommitPolicy, EngineConfig, EngineConfigBuilder, LocalWal, RetentionLoss,
    RunStatus, Until, WalStore,
};
use serde_json::{Value, json};

use crate::support::{engine, logging_engine, memory, pipeline, published_json, stream};

async fn log(group: &str, stream: &Value) -> Arc<dyn Source> {
    let config = json!({ "seed": 5, "group": group, "streams": [stream] });
    let source = source_factory::<LogSource>()
        .connect(config, ConnectContext::new())
        .await
        .expect("the log connects");
    Arc::from(source)
}

/// A configuration whose runs plan again only every minute, as the default does.
fn config() -> EngineConfigBuilder {
    EngineConfig::builder()
        .lanes(2)
        .barrier_wait(Duration::from_millis(100))
}

/// The offsets partition `partition` published to `events` in `store`, sorted.
fn offsets(store: &str, partition: &str) -> Vec<u64> {
    let mut offsets: Vec<u64> = published_json(store, "events")
        .iter()
        .filter(|row| row["partition"] == partition)
        .map(|row| row["offset"].as_u64().expect("an offset"))
        .collect();
    offsets.sort_unstable();
    offsets
}

#[tokio::test(start_paused = true)]
async fn a_following_run_plans_again_as_soon_as_its_source_says_its_partitions_changed() {
    let stream = json!({
        "name": "events", "partitions": 1, "messages": 3,
        "partitions_later": 2, "later_after_ms": 500,
    });
    let source = log("signalled", &stream).await;
    let events = stream_plan();
    // A minute between plans: only the source's signal starts the added partitions in time.
    let plan = pipeline("signalled", [events]).with_until(Until::For(Duration::from_secs(3)));
    let outcome = engine(config())
        .run(plan, source, memory("signalled").await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    for partition in ["p0", "p1", "p2"] {
        assert_eq!(offsets("signalled", partition), [0, 1, 2], "{partition}");
    }
}

fn stream_plan() -> rdlt_engine::StreamPlan {
    stream("events").read(ReadMode::Incremental)
}

#[tokio::test(start_paused = true)]
async fn a_report_says_how_far_behind_its_source_each_stream_last_was() {
    let logged = json!({ "name": "events", "partitions": 2, "messages": 12, "batch_rows": 5 });
    let source = log("behind", &logged).await;
    let plan = pipeline("behind", [stream_plan()]);
    let outcome = engine(config())
        .run(plan, source, memory("behind").await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(outcome.report.streams["events"].behind, Some(0));
    // A source that says nothing leaves it unknown.
    let quiet = crate::support::generator(&[("orders", 5, 1, 5)]).await;
    let plan = pipeline("quiet", [stream("orders")]);
    let outcome = engine(config())
        .run(plan, quiet, memory("quiet").await)
        .await;
    assert_eq!(outcome.report.streams["orders"].behind, None);
}

/// Loads 10 messages of a log keeping its newest 4, then, once it has grown by 20, loads it again
/// as `retention` says.
async fn after_retention(group: &str, retention: RetentionLoss) -> rdlt_engine::RunOutcome {
    after_retention_through(group, retention, None).await
}

/// As [`after_retention`], the second load reading through the log reshaped as `shape` says.
async fn after_retention_through(
    group: &str,
    retention: RetentionLoss,
    shape: Option<Shape>,
) -> rdlt_engine::RunOutcome {
    let stream = json!({
        "name": "events", "partitions": 1, "messages": 10, "per_second": 10, "retention": 4,
    });
    let source = log(group, &stream).await;
    let events = stream_plan().on_retention_loss(retention);
    let store = memory(group).await;
    let first = engine(config())
        .run(
            pipeline(group, [events.clone()]),
            Arc::clone(&source),
            Arc::clone(&store),
        )
        .await;
    assert_eq!(
        first.report.status,
        RunStatus::Succeeded,
        "{:?}",
        first.error
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    let source = match shape {
        Some(shape) => Arc::new(Reshaped {
            inner: source,
            shape,
        }) as Arc<dyn Source>,
        None => source,
    };
    engine(config())
        .run(pipeline(group, [events]), source, store)
        .await
}

#[tokio::test(start_paused = true)]
async fn a_run_whose_source_dropped_where_it_would_resume_fails_as_retention_lost() {
    let outcome = after_retention("lost", RetentionLoss::Fail).await;
    assert_eq!(outcome.report.status, RunStatus::Failed);
    let error = outcome.error.expect("the run fails");
    assert_eq!(error.code(), Some(rdlt_connector::RETENTION_LOST));
    assert!(
        !error.is_retryable(),
        "no retry finds what the source dropped"
    );
    let resets = outcome
        .report
        .streams
        .get("events")
        .map_or(0, |stream| stream.retention_resets);
    assert_eq!(resets, 0);
}

#[tokio::test(start_paused = true)]
async fn a_stream_that_resets_on_retention_loss_reads_again_from_the_earliest_and_counts_it() {
    let outcome = after_retention("reset", RetentionLoss::Reset).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(outcome.report.streams["events"].retention_resets, 1);
    // The first run read the log's newest 4; the second, those it kept 2 s and 20 messages on.
    let published = offsets("reset", "p0");
    assert_eq!(published, [6, 7, 8, 9, 26, 27, 28, 29]);
}

/// The log source, its reads reshaped as a test needs.
struct Reshaped {
    inner: Arc<dyn Source>,
    shape: Shape,
}

enum Shape {
    /// Reads from a cursor push what they read but no checkpoint, then fail as though the log
    /// had dropped where they began.
    Dropping,
    /// The reads of partitions whose ids start with the prefix say only that they are 100
    /// records behind.
    Lagging(&'static str),
    /// Its first three reads, counted here, fail as though the log had dropped where they
    /// began, even one from its beginning.
    Lost(AtomicUsize),
    /// Every read, where `signal` holds, first says its stream's partitions changed; plans are
    /// counted here.
    Signalling { plans: AtomicUsize, signal: bool },
}

impl Reshaped {
    /// Forwards the inner read of `request` to `sink`, first `first` and then each event `kept`
    /// keeps, until the engine asks the read to stop: dropping the feed then ends the inner read.
    async fn forwarding(
        &self,
        request: ReadRequest,
        mut sink: PartitionSink,
        first: Option<SourceEvent>,
        kept: fn(&SourceEvent) -> bool,
    ) -> rdlt_connector::Result<()> {
        let (inner, mut feed) = partition_channel(NonZeroUsize::new(64).expect("not zero"));
        let forwarding = async move {
            if let Some(first) = first {
                sink.send(first).await?;
            }
            loop {
                let event = tokio::select! {
                    biased;
                    () = sink.stopped() => break,
                    event = feed.recv() => event,
                };
                let Some(event) = event else {
                    break;
                };
                if kept(&event) {
                    sink.send(event).await?;
                }
            }
            Ok::<(), ConnectorError>(())
        };
        let (read, forwarded) = tokio::join!(self.inner.read(request, inner), forwarding);
        read.and(forwarded)
    }
}

impl Source for Reshaped {
    fn check(&self) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        self.inner.check()
    }

    fn discover(&self) -> BoxFuture<'_, rdlt_connector::Result<Catalog>> {
        self.inner.discover()
    }

    fn plan<'a>(
        &'a self,
        stream: &'a StreamName,
        state: &'a StreamState,
    ) -> BoxFuture<'a, rdlt_connector::Result<PartitionPlan>> {
        if let Shape::Signalling { plans, .. } = &self.shape {
            plans.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.plan(stream, state)
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, rdlt_connector::Result<()>> {
        self.inner.committed(stream, cursors)
    }

    fn read(
        &self,
        request: ReadRequest,
        sink: PartitionSink,
    ) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        Box::pin(async move {
            match self.shape {
                Shape::Dropping if request.cursor.is_some() => {
                    let unsealed =
                        |event: &SourceEvent| !matches!(event, SourceEvent::Checkpoint { .. });
                    self.forwarding(request, sink, None, unsealed).await?;
                    Err(ConnectorError::retention_lost(
                        "the log dropped the read's start",
                    ))
                }
                Shape::Lagging(prefix) if request.partition.id().as_str().starts_with(prefix) => {
                    let lag = Some(SourceEvent::Behind { records: 100 });
                    let quiet = |event: &SourceEvent| !matches!(event, SourceEvent::Behind { .. });
                    self.forwarding(request, sink, lag, quiet).await
                }
                Shape::Lost(ref reads) if reads.fetch_add(1, Ordering::SeqCst) < 3 => Err(
                    ConnectorError::retention_lost("the log dropped every message"),
                ),
                Shape::Signalling { signal: true, .. } => {
                    let every = |_: &SourceEvent| true;
                    self.forwarding(request, sink, Some(SourceEvent::Replan), every)
                        .await
                }
                _ => self.inner.read(request, sink).await,
            }
        })
    }
}

#[tokio::test(start_paused = true)]
async fn a_stream_s_lag_leaves_out_partitions_its_source_retired() {
    let logged = json!({
        "name": "events", "partitions": 3, "messages": 2, "per_second": 20,
        "partitions_retired": 1, "retired_after_ms": 1000,
    });
    let source = Arc::new(Reshaped {
        inner: log("retired_lag", &logged).await,
        shape: Shape::Lagging("p2"),
    });
    let plan =
        pipeline("retired_lag", [stream_plan()]).with_until(Until::For(Duration::from_secs(2)));
    let config = config().replan(Duration::from_millis(200));
    let outcome = engine(config)
        .run(plan, source, memory("retired_lag").await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(
        offsets("retired_lag", "p2").first(),
        Some(&0),
        "p2 was read"
    );
    // p2 said it was 100 behind until its source retired it; p0 and p1 caught up.
    assert_eq!(outcome.report.streams["events"].behind, Some(0));
}

#[tokio::test]
async fn a_read_reset_after_rows_it_never_sealed_holds_no_log_chunk_back() {
    // Each poll of the partition reads its new messages, then fails as retention lost and is
    // reset: the rows it read before failing sit in a segment no checkpoint seals.
    let logged = json!({
        "name": "events", "partitions": 1, "messages": 2, "per_second": 20, "bounded": true,
    });
    let source = Arc::new(Reshaped {
        inner: log("dropping", &logged).await,
        shape: Shape::Dropping,
    });
    let base = tempfile::tempdir().expect("a temporary directory");
    let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path()));
    let events = stream_plan().on_retention_loss(RetentionLoss::Reset);
    let plan = pipeline("dropping", [events])
        .with_until(Until::For(Duration::from_secs(2)))
        .with_wal(true);
    // Each push is written, and logged, at once: the failed reads' rows reach the log unsealed.
    let batch = BatchPolicy::new(1 << 20, 1, Duration::from_millis(10), 64 << 10)
        .expect("a valid batch policy");
    let every = CommitPolicy::new(Some(Duration::from_millis(100)), None, None)
        .expect("an interval is valid");
    let config = config()
        .replan(Duration::from_millis(200))
        .commit(every)
        .batch(batch);
    let run =
        logging_engine(config, Arc::clone(&store)).run(plan, source, memory("dropping").await);
    let pipeline_id = rdlt_connector::PipelineId::parse("dropping").expect("a valid id");
    let watch = async {
        tokio::time::sleep(Duration::from_millis(1800)).await;
        let mut chunks = 0;
        for load in store
            .loads(&pipeline_id)
            .await
            .expect("the store lists loads")
        {
            chunks += store
                .chunks(&pipeline_id, load)
                .await
                .expect("the store lists chunks")
                .len();
        }
        chunks
    };
    let (outcome, chunks) = tokio::join!(run, watch);
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert!(outcome.report.streams["events"].retention_resets > 1);
    // The chunk it is writing, and at most one whose commit awaits its receipt.
    assert!(chunks <= 2, "{chunks} chunks held");
}

#[tokio::test(start_paused = true)]
async fn a_stream_s_lag_leaves_out_the_partitions_of_a_phase_it_finished() {
    let source = Arc::new(Reshaped {
        inner: crate::changes::changes(8, &crate::changes::orders(&[])).await,
        shape: Shape::Lagging("snapshot-"),
    });
    let merged = stream("orders")
        .read(ReadMode::Cdc)
        .write(rdlt_engine::WriteMode::Merge);
    let outcome = engine(config())
        .run(
            pipeline("phased_lag", [merged]),
            source,
            memory("phased_lag").await,
        )
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    // The snapshot's partitions said they were behind; the changes' said nothing.
    assert_eq!(outcome.report.streams["orders"].behind, None);
}

#[tokio::test(start_paused = true)]
async fn a_reset_read_whose_source_dropped_its_beginning_too_fails_as_retention_lost() {
    let lost = Some(Shape::Lost(AtomicUsize::new(0)));
    let outcome = after_retention_through("lost_again", RetentionLoss::Reset, lost).await;
    assert_eq!(outcome.report.status, RunStatus::Failed);
    let error = outcome.error.expect("the run fails");
    assert_eq!(error.code(), Some(rdlt_connector::RETENTION_LOST));
    // Reset once, from its committed offset; its beginning has nothing earlier to reset to.
    assert_eq!(outcome.report.streams["events"].retention_resets, 1);
}

/// How many plans an exhausted run of the change source makes, its reads signalling if `signal`.
async fn plans(signal: bool) -> usize {
    let source = Arc::new(Reshaped {
        inner: crate::changes::changes(8, &crate::changes::orders(&[])).await,
        shape: Shape::Signalling {
            plans: AtomicUsize::new(0),
            signal,
        },
    });
    let store = format!("unfollowed_{signal}");
    let merged = stream("orders")
        .read(ReadMode::Cdc)
        .write(rdlt_engine::WriteMode::Merge);
    let outcome = engine(config())
        .run(
            pipeline("unfollowed", [merged]),
            Arc::clone(&source) as Arc<dyn Source>,
            memory(&store).await,
        )
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    match &source.shape {
        Shape::Signalling { plans, .. } => plans.load(Ordering::SeqCst),
        _ => unreachable!("the source signals"),
    }
}

#[tokio::test(start_paused = true)]
async fn a_run_that_does_not_follow_its_source_plans_only_at_its_phases_whatever_it_signals() {
    let quiet = plans(false).await;
    assert!(quiet >= 2, "the snapshot and the changes are planned");
    assert_eq!(plans(true).await, quiet);
}
