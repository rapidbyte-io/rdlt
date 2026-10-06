//! Continuous runs: a run that follows its source reads what arrives until its deadline or a stop,
//! plans its streams again as it reads, and loads each message exactly once.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{
    BoxFuture, Catalog, ConnectContext, ConnectorError, ConnectorErrorKind, PartitionId,
    PartitionPlan, PartitionSink, ReadMode, ReadRequest, Source, SourceFactory, StreamName,
    StreamState, acknowledging_source_factory,
};
use rdlt_connector_reference::LogSource;
use rdlt_engine::{
    CommitPolicy, EngineConfig, EngineConfigBuilder, ErrorKind, RetryPolicy, RunStatus, StopMode,
    Until,
};
use serde_json::{Value, json};

use crate::support::{engine, memory, pipeline, published_json, stream};

/// A log source of one stream, `events`, as `stream` describes it, keeping offsets in `group`.
async fn log(group: &str, stream: Value) -> Arc<dyn Source> {
    let config = json!({ "seed": 5, "group": group, "streams": [stream] });
    let source = factory()
        .connect(config, ConnectContext::new())
        .await
        .expect("the log connects");
    Arc::from(source)
}

fn factory() -> Box<dyn SourceFactory> {
    acknowledging_source_factory::<LogSource>()
}

/// A configuration that plans again every 200 ms, with two lanes.
fn following() -> EngineConfigBuilder {
    EngineConfig::builder()
        .lanes(2)
        .barrier_wait(Duration::from_millis(100))
        .replan(Duration::from_millis(200))
}

/// The offsets each partition published to `events` in `store`, sorted.
fn offsets(store: &str) -> BTreeMap<String, Vec<u64>> {
    let mut offsets: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for row in published_json(store, "events") {
        let partition = row["partition"].as_str().expect("a partition").to_owned();
        let offset = row["offset"].as_u64().expect("an offset");
        offsets.entry(partition).or_default().push(offset);
    }
    for published in offsets.values_mut() {
        published.sort_unstable();
    }
    offsets
}

/// Asserts each partition published every offset from 0 once, up to the offset `group` committed
/// for it, and returns how many each published.
async fn exactly_once(store: &str, group: &str, partitions: u32) -> Vec<u64> {
    let published = offsets(store);
    let config = json!({
        "seed": 5, "group": group,
        "streams": [{ "name": "events", "partitions": partitions, "messages": 0 }],
    });
    let (_, reader) = factory()
        .connect_acknowledging(config, ConnectContext::new())
        .await
        .expect("the log connects with its reader");
    let mut counts = Vec::new();
    for index in 0..partitions {
        let id = PartitionId::parse(format!("p{index}")).expect("a valid partition");
        let offsets = published.get(id.as_str()).cloned().unwrap_or_default();
        let count = u64::try_from(offsets.len()).expect("a count");
        assert_eq!(offsets, (0..count).collect::<Vec<_>>(), "partition {id}");
        let events = StreamName::new("events").expect("a valid stream");
        let committed = reader
            .acknowledged(&events, &id)
            .await
            .expect("the group answers");
        let committed = committed.map(|cursor| {
            cursor.decode::<Value>(1).expect("an offset")["next"]
                .as_u64()
                .expect("a next offset")
        });
        assert_eq!(committed.unwrap_or(0), count, "partition {id}");
        counts.push(count);
    }
    counts
}

fn incremental() -> rdlt_engine::StreamPlan {
    stream("events").read(ReadMode::Incremental)
}

#[tokio::test(start_paused = true)]
async fn a_run_for_a_while_follows_its_source_and_loads_each_message_once() {
    let source = log(
        "for_a_while",
        json!({
            "name": "events", "partitions": 2, "messages": 5, "per_second": 4,
        }),
    )
    .await;
    let plan = pipeline("streamed", [incremental()]).with_until(Until::For(Duration::from_secs(3)));
    let outcome = engine(following())
        .run(plan, source, memory("for_a_while").await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let counts = exactly_once("for_a_while", "for_a_while", 2).await;
    // Five at first, four a second after: the run read past the first five.
    assert!(
        counts.iter().all(|count| (6..=17).contains(count)),
        "{counts:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_run_forever_reads_until_stopped() {
    let source = log(
        "forever",
        json!({
            "name": "events", "partitions": 1, "messages": 3, "per_second": 2,
        }),
    )
    .await;
    let plan = pipeline("forever", [incremental()]).with_until(Until::Forever);
    let run = engine(following()).run(plan, source, memory("forever").await);
    let control = run.control();
    let stop = async {
        tokio::time::sleep(Duration::from_secs(2)).await;
        control.stop(StopMode::AfterCommit);
    };
    let (outcome, ()) = tokio::join!(run, stop);
    assert_eq!(
        outcome.report.status,
        RunStatus::Stopped,
        "{:?}",
        outcome.error
    );
    let counts = exactly_once("forever", "forever", 1).await;
    assert!((4..=7).contains(&counts[0]), "{counts:?}");
}

#[tokio::test(start_paused = true)]
async fn an_exhausted_run_ends_at_the_head_its_reads_started_at() {
    let source = log(
        "exhausted",
        json!({
            "name": "events", "partitions": 2, "messages": 7, "per_second": 100,
        }),
    )
    .await;
    let plan = pipeline("exhausted", [incremental()]);
    let outcome = engine(following())
        .run(plan, source, memory("exhausted").await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(exactly_once("exhausted", "exhausted", 2).await, [7, 7]);
}

#[tokio::test(start_paused = true)]
async fn a_following_run_starts_the_partitions_its_source_adds() {
    let source = log(
        "added",
        json!({
            "name": "events", "partitions": 1, "messages": 4,
            "partitions_later": 2, "later_after_ms": 500,
        }),
    )
    .await;
    let plan = pipeline("added", [incremental()]).with_until(Until::For(Duration::from_secs(2)));
    let outcome = engine(following())
        .run(plan, source, memory("added").await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(exactly_once("added", "added", 3).await, [4, 4, 4]);
}

#[tokio::test(start_paused = true)]
async fn a_following_run_reads_its_unbounded_partitions_each_in_a_slot_of_its_own() {
    let source = log(
        "wide",
        json!({ "name": "events", "partitions": 6, "messages": 3 }),
    )
    .await;
    let plan = pipeline("wide", [incremental()]).with_until(Until::For(Duration::from_secs(1)));
    // One slot more than the unbounded partitions, for every other read to take in turn.
    let outcome = engine(following().partitions(7))
        .run(plan, source, memory("wide").await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(exactly_once("wide", "wide", 6).await, [3; 6]);
}

#[tokio::test(start_paused = true)]
async fn seventeen_followed_reads_each_keeping_all_its_part_read_at_once() {
    // Each read keeps the reads' share of the default budget over the run's eighteen slots.
    let part = (256 << 20) / 4 / 18;
    let source = Arc::new(crate::lowering::Keeping {
        source: log(
            "kept_parts",
            json!({ "name": "events", "partitions": 17, "messages": 3 }),
        )
        .await,
        bytes: part,
    });
    let plan =
        pipeline("kept_parts", [incremental()]).with_until(Until::For(Duration::from_secs(1)));
    let outcome = engine(following().partitions(18))
        .run(plan, source, memory("kept_parts").await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert!(outcome.report.peak_memory >= 17 * part);
    assert_eq!(exactly_once("kept_parts", "kept_parts", 17).await, [3; 17]);
}

#[tokio::test(start_paused = true)]
async fn a_following_run_with_as_many_unbounded_partitions_as_slots_is_refused_at_once() {
    let source = log(
        "too_wide",
        json!({ "name": "events", "partitions": 6, "messages": 3 }),
    )
    .await;
    let plan =
        pipeline("too_wide", [incremental()]).with_until(Until::For(Duration::from_secs(60)));
    let started = tokio::time::Instant::now();
    let outcome = engine(
        following()
            .partitions(6)
            .retry(RetryPolicy::default().max_attempts(1)),
    )
    .run(plan, source, memory("too_wide").await)
    .await;
    let error = outcome.error.expect("the run fails");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Config, Some("partitions_too_few"))
    );
    assert!(!error.is_retryable());
    // Refused as the read that would take the last slot starts, not after any wait.
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
}

#[tokio::test(start_paused = true)]
async fn a_following_run_commits_every_ten_seconds_unless_told_otherwise() {
    let source = log(
        "ticking",
        json!({
            "name": "events", "partitions": 1, "messages": 1, "per_second": 1,
        }),
    )
    .await;
    let plan = pipeline("ticking", [incremental()]).with_until(Until::For(Duration::from_secs(35)));
    let outcome = engine(following())
        .run(plan, source, memory("ticking").await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    // At 10, 20 and 30 seconds, then as the deadline ends the run.
    assert_eq!(outcome.report.commits, 4);
    exactly_once("ticking", "ticking", 1).await;
}

#[tokio::test(start_paused = true)]
async fn a_following_run_reads_a_bounded_stream_again_as_it_grows() {
    let source = log(
        "polled",
        json!({
            "name": "events", "partitions": 2, "messages": 3, "per_second": 5, "bounded": true,
        }),
    )
    .await;
    let plan = pipeline("polled", [incremental()]).with_until(Until::For(Duration::from_secs(3)));
    // A partition read to its end is read again once its end is committed.
    let every = CommitPolicy::new(Some(Duration::from_millis(100)), None, None)
        .expect("an interval is valid");
    let outcome = engine(following().commit(every))
        .run(plan, source, memory("polled").await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let counts = exactly_once("polled", "polled", 2).await;
    assert!(
        counts.iter().all(|count| (8..=18).contains(count)),
        "{counts:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_following_run_stops_the_partitions_its_source_no_longer_plans() {
    let source = log(
        "retired",
        json!({
            "name": "events", "partitions": 3, "messages": 2, "per_second": 4,
            "partitions_retired": 1, "retired_after_ms": 500,
        }),
    )
    .await;
    let plan = pipeline("retired", [incremental()]).with_until(Until::For(Duration::from_secs(3)));
    let outcome = engine(following())
        .run(plan, source, memory("retired").await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let counts = exactly_once("retired", "retired", 3).await;
    // The retired partition stopped within a plan of its retirement; the others read on.
    assert!(counts[2] < counts[0] && counts[2] < counts[1], "{counts:?}");
    assert!(counts[2] <= 2 + 4, "{counts:?}");
}

/// A source whose every read fails, as one whose broker is down does.
struct Down(Arc<dyn Source>);

impl Source for Down {
    fn check(&self) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        self.0.check()
    }

    fn discover(&self) -> BoxFuture<'_, rdlt_connector::Result<Catalog>> {
        self.0.discover()
    }

    fn plan<'a>(
        &'a self,
        stream: &'a StreamName,
        state: &'a StreamState,
    ) -> BoxFuture<'a, rdlt_connector::Result<PartitionPlan>> {
        self.0.plan(stream, state)
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, rdlt_connector::Cursor)],
    ) -> BoxFuture<'a, rdlt_connector::Result<()>> {
        self.0.committed(stream, cursors)
    }

    fn read(
        &self,
        _request: ReadRequest,
        _sink: PartitionSink,
    ) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        Box::pin(async {
            Err(ConnectorError::new(
                ConnectorErrorKind::Transient,
                "the broker is down",
            ))
        })
    }
}

#[tokio::test(start_paused = true)]
async fn a_deadline_that_finds_the_run_failing_ends_it_failed() {
    let source = log(
        "down",
        json!({ "name": "events", "partitions": 1, "messages": 3 }),
    )
    .await;
    let plan = pipeline("down", [incremental()]).with_until(Until::For(Duration::from_secs(2)));
    let retry = RetryPolicy::default()
        .max_attempts(1_000)
        .initial(Duration::from_millis(50))
        .max_delay(Duration::from_millis(200));
    let outcome = engine(following().retry(retry))
        .run(plan, Arc::new(Down(source)), memory("down").await)
        .await;
    assert_eq!(outcome.report.status, RunStatus::Failed);
    assert!(outcome.error.is_some());
    assert!(
        outcome.report.attempted > 1,
        "the run retried until its deadline"
    );
}

/// The log source, offering its streams to full reads too.
struct Replaced(Arc<dyn Source>);

impl Source for Replaced {
    fn check(&self) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        self.0.check()
    }

    fn discover(&self) -> BoxFuture<'_, rdlt_connector::Result<Catalog>> {
        Box::pin(async {
            let catalog = self.0.discover().await?;
            let streams = catalog
                .iter()
                .map(|stream| {
                    stream
                        .clone()
                        .with_read_modes([ReadMode::Incremental, ReadMode::Full])
                })
                .collect();
            Ok(Catalog::new(streams).expect("the log's streams are distinct"))
        })
    }

    fn plan<'a>(
        &'a self,
        stream: &'a StreamName,
        state: &'a StreamState,
    ) -> BoxFuture<'a, rdlt_connector::Result<PartitionPlan>> {
        self.0.plan(stream, state)
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, rdlt_connector::Cursor)],
    ) -> BoxFuture<'a, rdlt_connector::Result<()>> {
        self.0.committed(stream, cursors)
    }

    fn read(
        &self,
        request: ReadRequest,
        sink: PartitionSink,
    ) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        self.0.read(request, sink)
    }
}

#[tokio::test(start_paused = true)]
async fn a_following_run_reads_a_full_stream_to_its_end_and_swaps_it_in() {
    let source = log(
        "replaced",
        json!({
            "name": "events", "partitions": 2, "messages": 5, "per_second": 4,
        }),
    )
    .await;
    let full = stream("events")
        .read(ReadMode::Full)
        .write(rdlt_engine::WriteMode::Replace);
    let plan = pipeline("replaced", [full]).with_until(Until::For(Duration::from_secs(3)));
    let outcome = engine(following())
        .run(plan, Arc::new(Replaced(source)), memory("replaced").await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    // A full read ends at the head it started at, however long the run follows, and is published.
    let published = offsets("replaced");
    assert_eq!(published.values().map(Vec::len).collect::<Vec<_>>(), [5, 5]);
    assert_eq!(outcome.report.streams["events"].generations_swapped, 1);
}

/// How many partitions of `events` the state `store` holds for pipeline `pipeline` records.
async fn recorded_partitions(store: &str, pipeline: &str) -> usize {
    let context = rdlt_connector::OpenContext {
        pipeline: rdlt_connector::PipelineId::parse(pipeline).expect("a valid pipeline"),
        load_id: rdlt_connector::LoadId::from_parts(std::time::UNIX_EPOCH, 1),
    };
    let opened = memory(store)
        .await
        .open(&context)
        .await
        .expect("the store opens");
    let state =
        rdlt_connector::PipelineState::from_records(&opened.state).expect("the state reads");
    let events = StreamName::new("events").expect("a name");
    state
        .streams
        .get(&events)
        .map_or(0, |stream| stream.partitions.len())
}

#[tokio::test(start_paused = true)]
async fn a_partition_a_replan_drops_keeps_its_position_across_runs() {
    let retiring = json!({
        "name": "events", "partitions": 3, "messages": 2, "per_second": 4,
        "partitions_retired": 1, "retired_after_ms": 500,
    });
    let run = || async {
        let plan = pipeline("kept", [incremental()]).with_until(Until::For(Duration::from_secs(3)));
        let source = log("kept", retiring.clone()).await;
        let outcome = engine(following())
            .run(plan, source, memory("kept").await)
            .await;
        assert_eq!(
            outcome.report.status,
            RunStatus::Succeeded,
            "{:?}",
            outcome.error
        );
    };
    // A plan of the run that follows drops the retired partition: its position stays, should a
    // plan name it again.
    run().await;
    assert_eq!(recorded_partitions("kept", "kept").await, 3);
    // The next run's plans do not name it either, and it stays.
    run().await;
    assert_eq!(recorded_partitions("kept", "kept").await, 3);
}
