//! Regression tests for the defects of the previous engine that M2 fixes by design.

use std::time::Duration;

use rdlt_connector::{ConnectorErrorKind, PipelineId, ReadMode, StreamName};
use rdlt_engine::{
    CommitPolicy, EngineConfig, ErrorKind, PipelinePlan, RetryPolicy, RunStatus, StopMode,
    StreamPlan, WriteMode,
};

use crate::HEAP;
use crate::support::destinations::null;
use crate::support::script::{Fault, Hang, Script, ScriptStream, id, reconnect};
use crate::support::{
    commit_every, engine, every_id, generator, memory, pipeline, published_ids, published_rows,
    stream, until,
};

fn ids(partitions: usize, rows: u64) -> Vec<i64> {
    let mut ids: Vec<i64> = (0..partitions)
        .flat_map(|partition| (0..rows).map(move |offset| id(partition, offset)))
        .collect();
    ids.sort_unstable();
    ids
}

fn idle(name: &str, rows: u64) -> ScriptStream {
    let mut stream = ScriptStream::new(name, 1, rows, 5);
    stream.idle = true;
    stream
}

/// D5: staging left by a crashed run is discarded when the next run opens.
#[tokio::test(start_paused = true)]
async fn redelivered_segments_are_never_published_twice() {
    // The partition never checkpoints, so its rows stay in one unsealed segment. The first
    // interval commit records the table's schema, which flushes that segment into staging.
    let mut unsealed = idle("events", 20);
    unsealed.checkpoint_every = u64::MAX;
    unsealed.final_checkpoint = false;
    let (script, source) = Script::new(vec![unsealed]).connect("d5").await;
    let plan = pipeline("d5", [stream("events").read(ReadMode::Incremental)]);
    let interval = CommitPolicy::new(Some(Duration::from_secs(1)), None, None).unwrap();
    let engine = engine(EngineConfig::builder().commit(interval).lanes(1));
    let crashed = engine.run(plan.clone(), source, memory("d5").await);
    tokio::select! {
        biased;
        _ = crashed => panic!("the idle run never ends by itself"),
        () = tokio::time::sleep(Duration::from_secs(5)) => {}
    }
    assert_eq!(
        published_rows("d5", "events"),
        0,
        "the crashed run left its rows staged"
    );
    script.streams[0].idle_off();
    let outcome = engine
        .run(plan, reconnect("d5").await, memory("d5").await)
        .await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    assert_eq!(published_ids("d5", "events"), ids(1, 20));
}

/// D6: a report counts committed rows only, never rows merely written.
#[tokio::test(start_paused = true)]
async fn report_counts_only_committed_rows() {
    let (script, source) = Script::new(vec![idle("events", 40)]).connect("d6").await;
    let plan = pipeline("d6", [stream("events").read(ReadMode::Incremental)]);
    let run = engine(commit_every(15)).run(plan, source, memory("d6").await);
    let control = run.control();
    let stop = async {
        until(|| !script.acks.lock().is_empty()).await;
        control.stop(StopMode::Now);
    };
    let (outcome, ()) = tokio::join!(run, stop);
    assert_eq!(outcome.report.status, RunStatus::Cancelled);
    let published = u64::try_from(published_rows("d6", "events")).unwrap();
    assert_eq!(outcome.report.rows, published);
    assert!(published > 0);
}

/// D7: a report's totals cover every attempt, not only the last.
#[tokio::test(start_paused = true)]
async fn report_totals_span_all_attempts() {
    let fault = Fault {
        batch: 4,
        kind: ConnectorErrorKind::Transient,
        retry_after: None,
    };
    let (_, source) = Script::new(vec![ScriptStream::new("events", 1, 50, 5)])
        .fail(fault)
        .connect("d7")
        .await;
    let plan = pipeline("d7", [stream("events").read(ReadMode::Incremental)]);
    let retry = RetryPolicy::default()
        .initial(Duration::from_millis(1))
        .max_delay(Duration::from_millis(1));
    let outcome = engine(commit_every(5).retry(retry))
        .run(plan, source, memory("d7").await)
        .await;
    let attempts = &outcome.report.attempts;
    assert_eq!(attempts.len(), 2);
    assert!(
        attempts.iter().all(|attempt| attempt.rows > 0),
        "{attempts:?}"
    );
    assert_eq!(attempts.iter().map(|attempt| attempt.rows).sum::<u64>(), 50);
    assert_eq!(outcome.report.rows, 50);
    assert_eq!(published_ids("d7", "events"), ids(1, 50));
}

/// L1: two partitions whose batches each take more than half the budget both make progress.
#[tokio::test(start_paused = true)]
async fn two_partitions_with_half_budget_frames_make_progress() {
    let source = generator(&[("orders", 20_000, 2, 1_000)]).await;
    let config = commit_every(4_000).memory(40_000).lanes(1).lane_window(1);
    let run = engine(config).run(
        pipeline("l1", [stream("orders")]),
        source,
        memory("l1").await,
    );
    let outcome = tokio::time::timeout(Duration::from_secs(60), run)
        .await
        .expect("both partitions make progress");
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    assert!(
        outcome.report.peak_memory > 20_000,
        "{}",
        outcome.report.peak_memory
    );
    assert_eq!(published_ids("l1", "orders"), every_id(20_000));
}

/// L2: a failed partition fails the attempt at once, even while another partition hangs.
#[tokio::test(start_paused = true)]
async fn failed_partition_fails_attempt_promptly() {
    let mut hanging = ScriptStream::new("events", 2, 20, 5);
    hanging.hang = Hang::Partition(1);
    let fault = Fault {
        batch: 1,
        kind: ConnectorErrorKind::Data,
        retry_after: None,
    };
    let (_, source) = Script::new(vec![hanging]).fail(fault).connect("l2").await;
    let run = engine(commit_every(10)).run(
        pipeline("l2", [stream("events")]),
        source,
        memory("l2").await,
    );
    let outcome = tokio::time::timeout(Duration::from_secs(60), run)
        .await
        .expect("the failure ends the attempt");
    assert_eq!(outcome.report.status, RunStatus::Failed);
    assert_eq!(
        outcome.error.map(|error| error.kind()),
        Some(ErrorKind::Source)
    );
}

/// L4: a quiet source still commits its checkpoints when the commit interval passes.
#[tokio::test(start_paused = true)]
async fn quiet_source_commits_on_interval() {
    let (_, source) = Script::new(vec![idle("events", 5)]).connect("l4").await;
    let interval = CommitPolicy::new(Some(Duration::from_secs(10)), None, None).unwrap();
    let plan = pipeline("l4", [stream("events").read(ReadMode::Incremental)]);
    let run =
        engine(EngineConfig::builder().commit(interval)).run(plan, source, memory("l4").await);
    let control = run.control();
    let watch = async {
        tokio::time::sleep(Duration::from_secs(9)).await;
        let before = published_rows("l4", "events");
        tokio::time::sleep(Duration::from_secs(2)).await;
        let after = published_rows("l4", "events");
        control.stop(StopMode::AfterCommit);
        (before, after)
    };
    let (outcome, (before, after)) = tokio::join!(run, watch);
    assert_eq!((before, after), (0, 5));
    assert_eq!(outcome.report.status, RunStatus::Stopped);
}

/// L5: dropping a run ends every task it started.
#[tokio::test(start_paused = true)]
async fn dropping_run_leaves_no_tasks() {
    let alive = || {
        tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks()
    };
    let before = alive();
    let (_, source) = Script::new(vec![idle("events", 50)]).connect("l5").await;
    let run = engine(commit_every(10)).run(
        pipeline("l5", [stream("events")]),
        source,
        memory("l5").await,
    );
    let running = async {
        tokio::select! {
            biased;
            _ = run => panic!("the idle run never ends by itself"),
            () = tokio::time::sleep(Duration::from_secs(3)) => alive(),
        }
    };
    assert!(running.await > before, "the run started tasks");
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(alive(), before);
}

/// L6: invalid configuration is refused when it is built, before any run.
#[test]
fn invalid_config_is_rejected_at_build() {
    let config = |builder: rdlt_engine::EngineConfigBuilder| builder.build().unwrap_err();
    let backwards = RetryPolicy::default()
        .initial(Duration::from_secs(9))
        .max_delay(Duration::from_secs(1));
    for error in [
        config(EngineConfig::builder().memory(0)),
        config(EngineConfig::builder().lanes(0)),
        config(EngineConfig::builder().lane_window(0)),
        config(EngineConfig::builder().partitions(0)),
        config(EngineConfig::builder().partition_buffer(0)),
        config(EngineConfig::builder().retry(backwards)),
    ] {
        assert_eq!(
            (error.kind(), error.code()),
            (ErrorKind::Config, Some("config_invalid"))
        );
    }
    let policy = CommitPolicy::new(None, None, None).unwrap_err();
    assert_eq!(policy.code(), Some("commit_policy_invalid"));
    let pipeline = PipelineId::parse("l6").unwrap();
    let events = || StreamPlan::new(StreamName::new("events").unwrap());
    let plans = [
        (
            PipelinePlan::new(pipeline.clone(), []).unwrap_err(),
            "plan_empty",
        ),
        (
            PipelinePlan::new(pipeline.clone(), [events(), events()]).unwrap_err(),
            "plan_duplicate_stream",
        ),
        (
            PipelinePlan::new(
                pipeline,
                [events()
                    .read(ReadMode::Incremental)
                    .write(WriteMode::Replace)],
            )
            .unwrap_err(),
            "plan_mode_invalid",
        ),
    ];
    for (error, code) in plans {
        assert_eq!(
            (error.kind(), error.code()),
            (ErrorKind::Config, Some(code))
        );
    }
}

/// L7: an attempt that commits resets the count of failed attempts.
#[tokio::test(start_paused = true)]
async fn retry_budget_resets_after_commit() {
    let faults = |name: &str| {
        let script = Script::new(vec![ScriptStream::new("events", 1, 60, 5)]);
        let script = [3, 6, 9].into_iter().fold(script, |script, batch| {
            script.fail(Fault {
                batch,
                kind: ConnectorErrorKind::Transient,
                retry_after: None,
            })
        });
        let name = name.to_owned();
        async move { script.connect(&name).await.1 }
    };
    let retry = RetryPolicy::default()
        .max_attempts(2)
        .initial(Duration::from_millis(1))
        .max_delay(Duration::from_millis(1));
    let plan = pipeline("l7", [stream("events").read(ReadMode::Incremental)]);
    let resetting = engine(commit_every(5).retry(retry))
        .run(plan.clone(), faults("l7").await, memory("l7").await)
        .await;
    assert_eq!(resetting.report.status, RunStatus::Succeeded);
    assert_eq!(resetting.report.attempts.len(), 4);
    let strict = engine(commit_every(5).retry(retry.reset_after_progress(false)))
        .run(plan, faults("l7_strict").await, memory("l7_strict").await)
        .await;
    assert_eq!(strict.report.status, RunStatus::Failed);
    assert_eq!(strict.report.attempts.len(), 2);
}

/// L8: the engine's memory stays within its budget plus a fixed overhead, even when the destination
/// is far slower than the source and every partition reads at once with the default buffers.
#[tokio::test(start_paused = true)]
async fn memory_stays_within_the_budget() {
    const BUDGET: u64 = 4 << 20;
    const PARTITIONS: u64 = 16;
    const ROWS: u64 = 2_000_000;
    let source = generator(&[("orders", ROWS, PARTITIONS, 20_000)]).await;
    let destination = null().await;
    // A deep lane queue, so the budget alone holds the source back.
    let config = commit_every(500_000)
        .memory(BUDGET)
        .partitions(16)
        .lanes(1)
        .lane_window(1_000);
    let engine = engine(config);
    let plan = pipeline("l8", [stream("orders")]);
    HEAP.reset_peak_usage();
    let before = HEAP.current_usage();
    let outcome = engine.run(plan, source, destination).await;
    let peak = HEAP.peak_usage().saturating_sub(before);
    assert_eq!(outcome.report.rows, ROWS);
    let bound = BUDGET * 12 / 10 + (32 << 20) + PARTITIONS * (64 << 10);
    assert!(
        u64::try_from(peak).unwrap() <= bound,
        "peak {peak} bytes; bound {bound}"
    );
}
