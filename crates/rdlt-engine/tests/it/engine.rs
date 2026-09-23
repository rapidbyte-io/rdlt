use std::sync::atomic::Ordering;
use std::time::Duration;

use rdlt_connector::{Checkpointing, ConnectorErrorKind, ReadMode};
use rdlt_engine::{
    CommitPolicy, EngineConfig, ErrorKind, RetryPolicy, RunStatus, StopMode, WriteMode,
};

use crate::support::destinations::limited;
use crate::support::script::{Fault, PushKind, Script, ScriptStream, id, reconnect};
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

#[tokio::test(start_paused = true)]
async fn a_run_publishes_every_row_once() {
    let source = generator(&[("orders", 1000, 4, 37)]).await;
    let destination = memory("every_row").await;
    let outcome = engine(commit_every(250))
        .run(
            pipeline("every-row", [stream("orders")]),
            source,
            destination,
        )
        .await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    assert_eq!(published_ids("every_row", "orders"), every_id(1000));
    assert_eq!(outcome.report.rows, 1000);
    assert_eq!(outcome.report.streams["orders"].rows, 1000);
    assert!(outcome.report.streams["orders"].bytes >= outcome.report.rows * 8);
}

#[tokio::test(start_paused = true)]
async fn an_incremental_run_reads_only_the_rows_added_since_the_last_run() {
    let (script, source) = Script::new(vec![ScriptStream::new("events", 2, 30, 7)])
        .connect("incremental")
        .await;
    let plan = pipeline(
        "incremental",
        [stream("events").read(ReadMode::Incremental)],
    );
    let engine = engine(commit_every(10));
    let first = engine
        .run(plan.clone(), source, memory("incremental").await)
        .await;
    assert_eq!(first.report.rows, 60);
    script.streams[0].grow(5);
    let second = engine
        .run(
            plan,
            reconnect("incremental").await,
            memory("incremental").await,
        )
        .await;
    assert_eq!(second.report.status, RunStatus::Succeeded);
    assert_eq!(second.report.rows, 10);
    assert_eq!(published_ids("incremental", "events"), ids(2, 35));
}

#[tokio::test(start_paused = true)]
async fn a_full_append_run_appends_a_fresh_copy_every_run() {
    let plan = pipeline("full-append", [stream("orders")]);
    let engine = engine(commit_every(100));
    for run in 1..=2 {
        let source = generator(&[("orders", 300, 3, 25)]).await;
        let outcome = engine
            .run(plan.clone(), source, memory("full_append").await)
            .await;
        assert_eq!(outcome.report.rows, 300);
        assert_eq!(published_rows("full_append", "orders"), 300 * run);
    }
}

#[tokio::test(start_paused = true)]
async fn a_replace_run_swaps_in_one_complete_copy() {
    let plan = pipeline("replace", [stream("orders").write(WriteMode::Replace)]);
    let engine = engine(commit_every(50));
    for _ in 0..2 {
        let source = generator(&[("orders", 400, 2, 30)]).await;
        let outcome = engine
            .run(plan.clone(), source, memory("replace").await)
            .await;
        assert_eq!(outcome.report.status, RunStatus::Succeeded);
        assert_eq!(outcome.report.streams["orders"].generations_swapped, 1);
        assert_eq!(published_ids("replace", "orders"), every_id(400));
    }
}

#[tokio::test(start_paused = true)]
async fn a_stopped_replace_keeps_its_rows_hidden_and_resumes_into_the_same_generation() {
    let mut stream_script = ScriptStream::new("orders", 2, 40, 5);
    stream_script.idle = true;
    let (script, source) = Script::new(vec![stream_script])
        .connect("stopped_replace")
        .await;
    let plan = pipeline(
        "stopped-replace",
        [stream("orders").write(WriteMode::Replace)],
    );
    let engine = engine(commit_every(20));
    let run = engine.run(plan.clone(), source, memory("stopped_replace").await);
    let control = run.control();
    let stop = async {
        until(|| script.acks.lock().len() >= 4).await;
        control.stop(StopMode::AfterCommit);
    };
    let (outcome, ()) = tokio::join!(run, stop);
    assert_eq!(outcome.report.status, RunStatus::Stopped);
    assert!(outcome.report.rows > 0);
    assert_eq!(
        published_rows("stopped_replace", "orders"),
        0,
        "the generation is hidden"
    );
    let mut done = ScriptStream::new("orders", 2, 40, 5);
    done.idle = false;
    let (_, source) = Script::new(vec![done])
        .connect("stopped_replace_done")
        .await;
    let outcome = engine
        .run(plan, source, memory("stopped_replace").await)
        .await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    assert_eq!(published_ids("stopped_replace", "orders"), ids(2, 40));
}

#[tokio::test(start_paused = true)]
async fn stopping_after_commit_commits_what_is_sealed_and_the_next_run_resumes_from_it() {
    let mut idle = ScriptStream::new("events", 1, 25, 5);
    idle.idle = true;
    let (script, source) = Script::new(vec![idle]).connect("stop_after").await;
    let plan = pipeline("stop-after", [stream("events").read(ReadMode::Incremental)]);
    let engine = engine(commit_every(1000));
    let run = engine.run(plan.clone(), source, memory("stop_after").await);
    let control = run.control();
    let stop = async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        control.stop(StopMode::AfterCommit);
    };
    let (outcome, ()) = tokio::join!(run, stop);
    assert_eq!(outcome.report.status, RunStatus::Stopped);
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(published_rows("stop_after", "events"), 25);
    assert_eq!(outcome.report.rows, 25);
    script.streams[0].idle_off();
    script.streams[0].grow(3);
    let outcome = engine
        .run(
            plan,
            reconnect("stop_after").await,
            memory("stop_after").await,
        )
        .await;
    assert_eq!(outcome.report.rows, 3);
    assert_eq!(published_ids("stop_after", "events"), ids(1, 28));
}

#[tokio::test(start_paused = true)]
async fn stopping_now_cancels_the_run_and_reports_what_committed() {
    let mut idle = ScriptStream::new("events", 1, 25, 5);
    idle.idle = true;
    let (script, source) = Script::new(vec![idle]).connect("stop_now").await;
    let plan = pipeline("stop-now", [stream("events").read(ReadMode::Incremental)]);
    let run = engine(commit_every(10)).run(plan, source, memory("stop_now").await);
    let control = run.control();
    let stop = async {
        until(|| !script.acks.lock().is_empty()).await;
        control.stop(StopMode::Now);
    };
    let (outcome, ()) = tokio::join!(run, stop);
    assert_eq!(outcome.report.status, RunStatus::Cancelled);
    assert_eq!(
        outcome.error.map(|error| error.kind()),
        Some(ErrorKind::Cancelled)
    );
    assert_eq!(
        usize::try_from(outcome.report.rows).unwrap(),
        published_rows("stop_now", "events")
    );
}

#[tokio::test(start_paused = true)]
async fn a_newer_run_fences_an_older_one() {
    let mut idle = ScriptStream::new("events", 1, 10, 5);
    idle.idle = true;
    let (script, source) = Script::new(vec![idle]).connect("fenced").await;
    let plan = pipeline("fenced", [stream("events").read(ReadMode::Incremental)]);
    let policy = CommitPolicy::new(Some(Duration::from_secs(1)), None, None).unwrap();
    let engine = engine(EngineConfig::builder().commit(policy).lanes(1));
    let older = engine.run(plan.clone(), source, memory("fenced").await);
    let newer = async {
        until(|| !script.acks.lock().is_empty()).await;
        let newer = engine.run(
            plan.clone(),
            reconnect("fenced").await,
            memory("fenced").await,
        );
        let control = newer.control();
        let stop = async {
            // Once the newer run holds the pipeline, new rows make the older run commit.
            tokio::time::sleep(Duration::from_secs(2)).await;
            script.streams[0].grow(5);
            tokio::time::sleep(Duration::from_secs(10)).await;
            control.stop(StopMode::AfterCommit);
        };
        tokio::join!(newer, stop).0
    };
    let (older, newer) = tokio::join!(older, newer);
    assert_eq!(older.report.status, RunStatus::Failed);
    assert_eq!(
        older.error.map(|error| error.kind()),
        Some(ErrorKind::Fenced)
    );
    assert_eq!(newer.report.status, RunStatus::Stopped);
    assert_eq!(published_ids("fenced", "events"), ids(1, 15));
}

#[tokio::test(start_paused = true)]
async fn the_row_threshold_commits_as_rows_arrive() {
    let (_, source) = Script::new(vec![ScriptStream::new("events", 1, 100, 10)])
        .connect("threshold")
        .await;
    let plan = pipeline("threshold", [stream("events")]);
    let outcome = engine(commit_every(20))
        .run(plan, source, memory("threshold").await)
        .await;
    assert_eq!(outcome.report.rows, 100);
    assert!(outcome.report.commits >= 5, "{}", outcome.report.commits);
}

#[tokio::test(start_paused = true)]
async fn the_source_hears_every_committed_cursor() {
    let (script, source) = Script::new(vec![ScriptStream::new("events", 2, 30, 10)])
        .connect("acks")
        .await;
    let plan = pipeline("acks", [stream("events").read(ReadMode::Incremental)]);
    engine(commit_every(10))
        .run(plan, source, memory("acks").await)
        .await;
    let acks = script.acks.lock().clone();
    for partition in ["p0", "p1"] {
        let offsets: Vec<u64> = acks
            .iter()
            .filter(|(stream, id, _)| stream == "events" && id == partition)
            .map(|(_, _, next)| *next)
            .collect();
        assert!(
            offsets.windows(2).all(|pair| pair[0] <= pair[1]),
            "{offsets:?}"
        );
        assert_eq!(offsets.last(), Some(&30), "{offsets:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn naturally_checkpointing_partitions_are_never_asked_for_a_checkpoint() {
    let (script, source) = Script::new(vec![ScriptStream::new("events", 2, 50, 5)])
        .connect("natural")
        .await;
    let plan = pipeline("natural", [stream("events")]);
    let outcome = engine(commit_every(10))
        .run(plan, source, memory("natural").await)
        .await;
    assert_eq!(outcome.report.rows, 100);
    assert!(!script.asked.load(Ordering::SeqCst));
}

#[tokio::test(start_paused = true)]
async fn on_demand_partitions_checkpoint_when_a_commit_asks() {
    let mut on_demand = ScriptStream::new("events", 1, 60, 5);
    on_demand.checkpointing = Checkpointing::OnDemand;
    on_demand.final_checkpoint = false;
    on_demand.idle = true;
    let (script, source) = Script::new(vec![on_demand]).connect("on_demand").await;
    let plan = pipeline("on-demand", [stream("events").read(ReadMode::Incremental)]);
    let run = engine(commit_every(10)).run(plan, source, memory("on_demand").await);
    let control = run.control();
    let stop = async {
        until(|| !script.acks.lock().is_empty()).await;
        control.stop(StopMode::AfterCommit);
    };
    let (outcome, ()) = tokio::join!(run, stop);
    assert!(script.asked.load(Ordering::SeqCst));
    assert_eq!(outcome.report.status, RunStatus::Stopped);
    assert_eq!(published_ids("on_demand", "events"), ids(1, 60));
}

#[tokio::test(start_paused = true)]
async fn partitions_read_no_more_at_once_than_configured() {
    let (script, source) = Script::new(vec![ScriptStream::new("events", 6, 20, 2)])
        .connect("slots")
        .await;
    let plan = pipeline("slots", [stream("events")]);
    let config = commit_every(10).partitions(2);
    let outcome = engine(config)
        .run(plan, source, memory("slots").await)
        .await;
    assert_eq!(outcome.report.rows, 120);
    assert!(script.peak_reading.load(Ordering::SeqCst) <= 2);
}

#[tokio::test(start_paused = true)]
async fn an_empty_stream_succeeds_and_commits_its_state() {
    let (_, source) = Script::new(vec![ScriptStream::new("events", 2, 0, 5)])
        .connect("empty")
        .await;
    let plan = pipeline("empty", [stream("events").read(ReadMode::Incremental)]);
    let outcome = engine(commit_every(10))
        .run(plan, source, memory("empty").await)
        .await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    assert_eq!(outcome.report.rows, 0);
    assert_eq!(outcome.report.commits, 1);
}

#[tokio::test(start_paused = true)]
async fn streams_the_run_cannot_load_are_configuration_errors() {
    let (_, source) = Script::new(vec![ScriptStream::new("events", 1, 5, 5)])
        .connect("unknown")
        .await;
    let engine = engine(commit_every(10));
    let unknown = engine
        .run(
            pipeline("unknown", [stream("missing")]),
            source,
            memory("unknown").await,
        )
        .await;
    let mut undeclared = ScriptStream::new("events", 1, 5, 5);
    undeclared.declares_schema = false;
    let (_, source) = Script::new(vec![undeclared]).connect("undeclared").await;
    let schemaless = engine
        .run(
            pipeline("undeclared", [stream("events")]),
            source,
            memory("undeclared").await,
        )
        .await;
    let generator_source = generator(&[("orders", 5, 1, 5)]).await;
    let incremental = engine
        .run(
            pipeline("unreadable", [stream("orders").read(ReadMode::Incremental)]),
            generator_source,
            memory("unreadable").await,
        )
        .await;
    let no_replace = limited(memory("no_replace").await, |caps| {
        caps.write_modes.replace = false;
    });
    let unwritable = engine
        .run(
            pipeline("unwritable", [stream("orders").write(WriteMode::Replace)]),
            generator(&[("orders", 5, 1, 5)]).await,
            no_replace,
        )
        .await;
    for (outcome, code) in [
        (unknown, "stream_not_found"),
        (schemaless, "schema_required"),
        (incremental, "read_mode_unsupported"),
        (unwritable, "write_mode_unsupported"),
    ] {
        let error = outcome.error.expect("the run fails");
        assert_eq!(error.kind(), ErrorKind::Config);
        assert_eq!(error.code(), Some(code));
        assert!(error.stream().is_some());
        assert_eq!(
            outcome.report.attempts.len(),
            1,
            "configuration errors are not retried"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn batches_that_do_not_match_the_table_are_schema_errors() {
    for (kind, name) in [
        (PushKind::WrongType, "wrong_type"),
        (PushKind::Nulls, "nulls"),
    ] {
        let mut bad = ScriptStream::new("events", 1, 5, 5);
        bad.push = kind;
        let (_, source) = Script::new(vec![bad]).connect(name).await;
        let outcome = engine(commit_every(10))
            .run(
                pipeline(name.replace('_', "-").as_str(), [stream("events")]),
                source,
                memory(name).await,
            )
            .await;
        let error = outcome.error.expect("the run fails");
        assert_eq!(error.kind(), ErrorKind::Schema, "{name}");
        assert_eq!(error.code(), Some("batch_schema_mismatch"));
        assert_eq!(published_rows(name, "events"), 0);
    }
}

#[tokio::test(start_paused = true)]
async fn a_changed_schema_is_refused_until_schema_evolution_exists() {
    let engine = engine(commit_every(10));
    let first = engine
        .run(
            pipeline("changed", [stream("orders")]),
            generator(&[("orders", 5, 1, 5)]).await,
            memory("changed").await,
        )
        .await;
    assert_eq!(first.report.status, RunStatus::Succeeded);
    let (_, source) = Script::new(vec![ScriptStream::new("orders", 1, 5, 5)])
        .connect("changed")
        .await;
    let second = engine
        .run(
            pipeline("changed", [stream("orders")]),
            source,
            memory("changed").await,
        )
        .await;
    let error = second.error.expect("the run fails");
    assert_eq!(error.kind(), ErrorKind::Schema);
    assert_eq!(error.code(), Some("schema_changed"));
}

#[tokio::test(start_paused = true)]
async fn json_pushes_are_refused_until_the_shredder_exists() {
    let mut json = ScriptStream::new("events", 1, 5, 5);
    json.push = PushKind::Json;
    let (_, source) = Script::new(vec![json]).connect("json").await;
    let outcome = engine(commit_every(10))
        .run(
            pipeline("json", [stream("events")]),
            source,
            memory("json").await,
        )
        .await;
    let error = outcome.error.expect("the run fails");
    assert_eq!(error.kind(), ErrorKind::Source);
    assert_eq!(error.code(), Some("push_unsupported"));
}

fn retrying(attempts: u32) -> rdlt_engine::EngineConfigBuilder {
    let retry = RetryPolicy::default()
        .max_attempts(attempts)
        .initial(Duration::from_millis(10))
        .max_delay(Duration::from_millis(100));
    commit_every(10).retry(retry)
}

#[tokio::test(start_paused = true)]
async fn a_transient_failure_is_retried_and_the_run_completes() {
    let fault = Fault {
        batch: 3,
        kind: ConnectorErrorKind::Transient,
        retry_after: None,
    };
    let (_, source) = Script::new(vec![ScriptStream::new("events", 1, 50, 5)])
        .fail(fault)
        .connect("transient")
        .await;
    let plan = pipeline("transient", [stream("events").read(ReadMode::Incremental)]);
    let outcome = engine(retrying(3))
        .run(plan, source, memory("transient").await)
        .await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    assert_eq!(outcome.report.attempts.len(), 2);
    assert_eq!(
        outcome.report.attempts[0]
            .error
            .as_ref()
            .map(|error| error.kind),
        Some(ErrorKind::Source)
    );
    assert_eq!(published_ids("transient", "events"), ids(1, 50));
}

#[tokio::test(start_paused = true)]
async fn a_rate_limited_attempt_waits_as_long_as_it_is_asked() {
    let fault = Fault {
        batch: 1,
        kind: ConnectorErrorKind::RateLimited,
        retry_after: Some(Duration::from_secs(90)),
    };
    let (_, source) = Script::new(vec![ScriptStream::new("events", 1, 10, 5)])
        .fail(fault)
        .connect("rate_limited")
        .await;
    let plan = pipeline("rate-limited", [stream("events")]);
    let outcome = engine(retrying(3))
        .run(plan, source, memory("rate_limited").await)
        .await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    assert_eq!(outcome.report.attempts.len(), 2);
    let elapsed = outcome.report.elapsed;
    assert!(elapsed >= Duration::from_secs(90), "{elapsed:?}");
}

#[tokio::test(start_paused = true)]
async fn a_data_failure_is_not_retried() {
    let fault = Fault {
        batch: 2,
        kind: ConnectorErrorKind::Data,
        retry_after: None,
    };
    let (_, source) = Script::new(vec![ScriptStream::new("events", 1, 50, 5)])
        .fail(fault)
        .connect("data_failure")
        .await;
    let outcome = engine(retrying(5))
        .run(
            pipeline("data-failure", [stream("events")]),
            source,
            memory("data_failure").await,
        )
        .await;
    assert_eq!(outcome.report.status, RunStatus::Failed);
    assert_eq!(outcome.report.attempts.len(), 1);
    assert_eq!(
        outcome.error.map(|error| error.kind()),
        Some(ErrorKind::Source)
    );
}

#[tokio::test(start_paused = true)]
async fn retries_stop_after_the_last_attempt() {
    let script = (1..=10).fold(
        Script::new(vec![ScriptStream::new("events", 1, 50, 5)]),
        |script, batch| {
            script.fail(Fault {
                batch,
                kind: ConnectorErrorKind::Transient,
                retry_after: None,
            })
        },
    );
    let (_, source) = script.connect("exhausted").await;
    let config = retrying(3).commit(CommitPolicy::new(None, Some(1000), None).unwrap());
    let outcome = engine(config)
        .run(
            pipeline("exhausted", [stream("events")]),
            source,
            memory("exhausted").await,
        )
        .await;
    assert_eq!(outcome.report.status, RunStatus::Failed);
    assert_eq!(outcome.report.attempts.len(), 3);
    assert!(outcome.error.is_some_and(|error| error.is_retryable()));
}

#[tokio::test(start_paused = true)]
async fn a_failure_at_any_step_of_an_attempt_names_its_side() {
    use crate::support::destinations::{Step, failing};
    let run = |destination, script: Script, name: &'static str| async move {
        let (_, source) = script.connect(name).await;
        let retry = RetryPolicy::default().max_attempts(1);
        engine(commit_every(10).retry(retry))
            .run(
                pipeline(name, [stream("events").read(ReadMode::Incremental)]),
                source,
                destination,
            )
            .await
    };
    let events = || Script::new(vec![ScriptStream::new("events", 1, 20, 5)]);
    for (step, name) in [
        (Step::Open, "fail-open"),
        (Step::CreateTable, "fail-create"),
        (Step::Writer, "fail-writer"),
        (Step::Commit, "fail-commit"),
    ] {
        let outcome = run(failing(memory(name).await, step), events(), name).await;
        let error = outcome.error.expect("the attempt fails");
        assert_eq!(error.kind(), ErrorKind::Destination, "{step:?}");
        assert!(error.is_retryable(), "{step:?}");
        assert_eq!(outcome.report.rows, 0, "{step:?}");
    }
    let mut discover = events();
    discover.fail_discover = true;
    let mut plan = events();
    plan.fail_plan = true;
    let mut ack = events();
    ack.fail_ack = true;
    let discovered = run(memory("fail-discover").await, discover, "fail-discover").await;
    assert_eq!(
        discovered
            .error
            .as_ref()
            .map(|error| (error.kind(), error.is_retryable())),
        Some((ErrorKind::Source, true))
    );
    let planned = run(memory("fail-plan").await, plan, "fail-plan").await;
    let planned = planned.error.expect("planning fails");
    assert_eq!(
        (planned.kind(), planned.is_retryable()),
        (ErrorKind::Source, false)
    );
    assert!(planned.stream().is_some());
    let acknowledged = run(memory("fail-ack").await, ack, "fail-ack").await;
    let error = acknowledged.error.expect("acknowledging fails");
    assert_eq!(error.kind(), ErrorKind::Source);
    assert!(error.stream().is_some());
    assert!(
        acknowledged.report.rows > 0,
        "the commit landed before the acknowledgement failed"
    );
}

#[tokio::test(start_paused = true)]
async fn a_stopped_partition_commits_only_up_to_its_last_checkpoint() {
    let mut sparse = ScriptStream::new("events", 1, 15, 5);
    sparse.checkpoint_every = 2;
    sparse.final_checkpoint = false;
    sparse.idle = true;
    let (script, source) = Script::new(vec![sparse]).connect("stopped_tail").await;
    let plan = pipeline(
        "stopped-tail",
        [stream("events").read(ReadMode::Incremental)],
    );
    let engine = engine(commit_every(1000));
    let run = engine.run(plan.clone(), source, memory("stopped_tail").await);
    let control = run.control();
    let stop = async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        control.stop(StopMode::AfterCommit);
    };
    let (outcome, ()) = tokio::join!(run, stop);
    assert_eq!(outcome.report.status, RunStatus::Stopped);
    assert_eq!(
        published_ids("stopped_tail", "events"),
        ids(1, 10),
        "the tail after the checkpoint waits"
    );
    script.streams[0].idle_off();
    script.streams[0].grow(5);
    let outcome = engine
        .run(
            plan,
            reconnect("stopped_tail").await,
            memory("stopped_tail").await,
        )
        .await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    assert_eq!(published_ids("stopped_tail", "events"), ids(1, 20));
}

#[tokio::test(start_paused = true)]
async fn an_incremental_partition_that_starts_empty_reads_rows_added_later() {
    let mut empty = ScriptStream::new("events", 1, 0, 5);
    empty.final_checkpoint = false;
    let (script, source) = Script::new(vec![empty]).connect("starts_empty").await;
    let plan = pipeline(
        "starts-empty",
        [stream("events").read(ReadMode::Incremental)],
    );
    let engine = engine(commit_every(10));
    let first = engine
        .run(plan.clone(), source, memory("starts_empty").await)
        .await;
    assert_eq!(
        (first.report.status, first.report.rows),
        (RunStatus::Succeeded, 0)
    );
    script.streams[0].grow(7);
    let second = engine
        .run(
            plan,
            reconnect("starts_empty").await,
            memory("starts_empty").await,
        )
        .await;
    assert_eq!(second.report.rows, 7);
    assert_eq!(published_ids("starts_empty", "events"), ids(1, 7));
}

#[tokio::test(start_paused = true)]
async fn stopping_does_not_start_partitions_still_waiting_for_a_read_slot() {
    let mut queued = ScriptStream::new("events", 8, 10, 5);
    queued.idle = true;
    let (script, source) = Script::new(vec![queued]).connect("queued").await;
    let plan = pipeline("queued", [stream("events").read(ReadMode::Incremental)]);
    let run = engine(commit_every(10).partitions(1)).run(plan, source, memory("queued").await);
    let control = run.control();
    let stop = async {
        until(|| !script.acks.lock().is_empty()).await;
        control.stop(StopMode::AfterCommit);
    };
    let (outcome, ()) = tokio::join!(run, stop);
    assert_eq!(outcome.report.status, RunStatus::Stopped);
    assert_eq!(
        script.reads.load(Ordering::SeqCst),
        1,
        "queued partitions never start"
    );
}
