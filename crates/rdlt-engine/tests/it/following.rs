//! Following runs as the engine keeps their books: read slots, partitions read again, the stop
//! that precedes a deadline, and a write-ahead log that stays bounded.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use rdlt_connector::{
    BoxFuture, Catalog, ConnectContext, Cursor, PartitionId, PartitionPlan, PartitionSink,
    ReadMode, ReadRequest, Source, SourceEvent, StreamName, StreamState, partition_channel,
    source_factory,
};
use rdlt_connector_reference::LogSource;
use rdlt_engine::{
    BatchPolicy, CommitPolicy, EngineConfig, EngineConfigBuilder, LocalWal, RunStatus, StopMode,
    Until, WalStore,
};
use serde_json::{Value, json};

use crate::support::{engine, logging_engine, memory, pipeline, published_json, stream};

/// The log source, shaped as each test needs.
#[derive(Default)]
struct Shaped {
    inner: Option<Arc<dyn Source>>,
    /// Holds the end of partition p0's first read this long.
    lag: Option<Duration>,
    /// Holds the end of every read this long.
    hold: Option<Duration>,
    lagged: AtomicBool,
    /// A partition whose reads send no checkpoint.
    withheld: Option<&'static str>,
    /// Whether each plan names the phase it plans, as a phased source's do.
    naming: bool,
    reading: AtomicUsize,
    busiest: AtomicUsize,
}

impl Shaped {
    fn inner(&self) -> &Arc<dyn Source> {
        self.inner.as_ref().expect("a shaped source wraps one")
    }

    /// Forwards what `request`'s read sends to `sink`, but its checkpoints, until the engine asks
    /// the read to stop: dropping the feed then ends the inner read.
    async fn withholding(
        &self,
        request: ReadRequest,
        mut sink: PartitionSink,
    ) -> rdlt_connector::Result<()> {
        let (inner, mut feed) = partition_channel(NonZeroUsize::new(64).expect("not zero"));
        let forwarding = async move {
            loop {
                let event = tokio::select! {
                    biased;
                    () = sink.stopped() => break,
                    event = feed.recv() => event,
                };
                let Some(event) = event else {
                    break;
                };
                if !matches!(event, SourceEvent::Checkpoint { .. }) {
                    sink.send(event).await?;
                }
            }
            Ok::<(), rdlt_connector::ConnectorError>(())
        };
        let (read, forwarded) = tokio::join!(self.inner().read(request, inner), forwarding);
        read.and(forwarded)
    }
}

impl Source for Shaped {
    fn check(&self) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        self.inner().check()
    }

    fn discover(&self) -> BoxFuture<'_, rdlt_connector::Result<Catalog>> {
        self.inner().discover()
    }

    fn plan<'a>(
        &'a self,
        stream: &'a StreamName,
        state: &'a StreamState,
    ) -> BoxFuture<'a, rdlt_connector::Result<PartitionPlan>> {
        Box::pin(async move {
            let plan = self.inner().plan(stream, state).await?;
            Ok(if self.naming {
                plan.phase(state.phase)
            } else {
                plan
            })
        })
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, rdlt_connector::Result<()>> {
        self.inner().committed(stream, cursors)
    }

    fn read(
        &self,
        request: ReadRequest,
        sink: PartitionSink,
    ) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        Box::pin(async move {
            let now = self.reading.fetch_add(1, Ordering::SeqCst) + 1;
            self.busiest.fetch_max(now, Ordering::SeqCst);
            let id = request.partition.id().as_str().to_owned();
            let read = if self.withheld == Some(id.as_str()) {
                self.withholding(request, sink).await
            } else {
                self.inner().read(request, sink).await
            };
            if let Some(lag) = self.lag
                && id == "p0"
                && !self.lagged.swap(true, Ordering::SeqCst)
            {
                tokio::time::sleep(lag).await;
            }
            if let Some(hold) = self.hold {
                tokio::time::sleep(hold).await;
            }
            self.reading.fetch_sub(1, Ordering::SeqCst);
            read
        })
    }
}

async fn log(group: &str, stream: &Value) -> Arc<dyn Source> {
    let config = json!({ "seed": 5, "group": group, "streams": [stream] });
    let source = source_factory::<LogSource>()
        .connect(config, ConnectContext::new())
        .await
        .expect("the log connects");
    Arc::from(source)
}

fn following(commit_every: Duration) -> EngineConfigBuilder {
    let every = CommitPolicy::new(Some(commit_every), None, None).expect("an interval is valid");
    EngineConfig::builder()
        .lanes(2)
        .barrier_wait(Duration::from_millis(100))
        .replan(Duration::from_millis(200))
        .commit(every)
}

/// The offsets each of `partitions` partitions published to `events` in `store`, sorted.
fn offsets(store: &str, partitions: u32) -> Vec<Vec<u64>> {
    let rows = published_json(store, "events");
    (0..partitions)
        .map(|index| {
            let partition = format!("p{index}");
            let mut offsets: Vec<u64> = rows
                .iter()
                .filter(|row| row["partition"] == partition.as_str())
                .map(|row| row["offset"].as_u64().expect("an offset"))
                .collect();
            offsets.sort_unstable();
            offsets
        })
        .collect()
}

/// Asserts each partition published every offset from 0 once, and returns how many each did.
fn contiguous(store: &str, partitions: u32) -> Vec<u64> {
    offsets(store, partitions)
        .into_iter()
        .enumerate()
        .map(|(index, offsets)| {
            let count = u64::try_from(offsets.len()).expect("a count");
            assert_eq!(offsets, (0..count).collect::<Vec<_>>(), "p{index}");
            count
        })
        .collect()
}

fn incremental() -> rdlt_engine::StreamPlan {
    stream("events").read(ReadMode::Incremental)
}

#[tokio::test(start_paused = true)]
async fn a_following_run_reads_bounded_partitions_no_more_at_once_than_it_has_slots() {
    let stream = json!({
        "name": "events", "partitions": 6, "messages": 3, "per_second": 4, "bounded": true,
    });
    let shaped = Arc::new(Shaped {
        inner: Some(log("slotted", &stream).await),
        hold: Some(Duration::from_millis(50)),
        ..Shaped::default()
    });
    let plan = pipeline("slotted", [incremental()]).with_until(Until::For(Duration::from_secs(2)));
    let source = Arc::clone(&shaped) as Arc<dyn Source>;
    let outcome = engine(following(Duration::from_millis(100)).partitions(2))
        .run(plan, source, memory("slotted").await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert!(shaped.busiest.load(Ordering::SeqCst) <= 2);
    assert!(contiguous("slotted", 6).iter().all(|count| *count >= 3));
}

#[tokio::test(start_paused = true)]
async fn a_partition_read_again_while_another_s_end_waits_to_commit_loads_each_message_once() {
    // p0's first read ends five seconds late, after one commit and before the next: p1 is read
    // again meanwhile, while p0's end waits in the coordinator.
    let stream = json!({
        "name": "events", "partitions": 2, "messages": 4, "per_second": 2, "bounded": true,
    });
    let shaped = Arc::new(Shaped {
        inner: Some(log("lagging", &stream).await),
        lag: Some(Duration::from_millis(5100)),
        ..Shaped::default()
    });
    let plan = pipeline("lagging", [incremental()]).with_until(Until::For(Duration::from_secs(12)));
    let outcome = engine(following(Duration::from_secs(5)))
        .run(plan, shaped, memory("lagging").await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let counts = contiguous("lagging", 2);
    assert!(counts.iter().all(|count| *count > 4), "{counts:?}");
}

#[tokio::test(start_paused = true)]
async fn a_following_run_starts_partitions_a_source_adds_to_plans_naming_their_phase() {
    let stream = json!({
        "name": "events", "partitions": 1, "messages": 4,
        "partitions_later": 2, "later_after_ms": 500,
    });
    let shaped = Arc::new(Shaped {
        inner: Some(log("named", &stream).await),
        naming: true,
        ..Shaped::default()
    });
    let plan = pipeline("named", [incremental()]).with_until(Until::For(Duration::from_secs(2)));
    let outcome = engine(following(Duration::from_millis(100)))
        .run(plan, shaped, memory("named").await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(contiguous("named", 3), [4, 4, 4]);
}

#[tokio::test(start_paused = true)]
async fn a_following_run_stopped_before_its_deadline_is_stopped() {
    let stream = json!({ "name": "events", "partitions": 1, "messages": 3, "per_second": 2 });
    let source = log("stopped_early", &stream).await;
    let plan =
        pipeline("stopped_early", [incremental()]).with_until(Until::For(Duration::from_secs(10)));
    let run = engine(following(Duration::from_millis(100))).run(
        plan,
        source,
        memory("stopped_early").await,
    );
    let control = run.control();
    let stop = async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        control.stop(StopMode::AfterCommit);
    };
    let (outcome, ()) = tokio::join!(run, stop);
    assert_eq!(
        outcome.report.status,
        RunStatus::Stopped,
        "{:?}",
        outcome.error
    );
}

#[tokio::test]
async fn a_partition_dropped_with_rows_it_never_sealed_holds_no_log_chunk_back() {
    // p2 checkpoints nothing, so its rows sit in an open segment until the source retires it.
    let stream = json!({
        "name": "events", "partitions": 3, "messages": 2, "per_second": 20,
        "partitions_retired": 1, "retired_after_ms": 1000,
    });
    let shaped = Arc::new(Shaped {
        inner: Some(log("retired_logged", &stream).await),
        withheld: Some("p2"),
        ..Shaped::default()
    });
    let base = tempfile::tempdir().expect("a temporary directory");
    let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path()));
    let plan = pipeline("retired_logged", [incremental()])
        .with_until(Until::For(Duration::from_secs(2)))
        .with_wal(true);
    // Each push is written, and logged, at once: p2's rows reach the log unsealed.
    let batch = BatchPolicy::new(1 << 20, 1, Duration::from_millis(10), 64 << 10)
        .expect("a valid batch policy");
    let config = following(Duration::from_millis(100)).batch(batch);
    let run = logging_engine(config, Arc::clone(&store)).run(
        plan,
        shaped,
        memory("retired_logged").await,
    );
    let pipeline_id = rdlt_connector::PipelineId::parse("retired_logged").expect("a valid id");
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
    // The chunk it is writing, and at most one whose commit awaits its receipt.
    assert!(chunks <= 2, "{chunks} chunks held");
}
