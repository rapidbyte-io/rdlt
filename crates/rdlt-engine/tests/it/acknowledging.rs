//! What the engine tells a source is committed: the position of every partition a commit covers,
//! moved or not, so a report that failed is made again by the next commit, else by the next
//! attempt.
//!
//! A served source hears a host for what it sent that host and for where the host reads from,
//! so a partition that is sent nothing is still reported where its read started.

use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rdlt_connector::{ConnectorId, ReadMode, Source};
use rdlt_engine::{Acks, ErrorKind, RunStatus, Until};
use rdlt_host::{ConnectorRef, Identity, Provider as _, Remote};
use rdlt_testkit::tls::Pki;
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::process::{Child, Command};

use crate::support::destinations::{Step, failing};
use crate::support::hooked::{At, Hook, hooked};
use crate::support::script::{Script, ScriptStream, id, reconnect};
use crate::support::{
    commit_every, engine, memory, pipeline, published_ids, published_json, retrying, stream,
};

/// The log source's configuration: one stream, `events`, of one partition holding `messages`
/// messages, whose group keeps its offsets in `group`.
fn log(messages: u64, replayable: bool, group: &Path) -> serde_json::Value {
    serde_json::json!({
        "seed": 1,
        "group_path": group,
        "streams": [{
            "name": "events",
            "partitions": 1,
            "messages": messages,
            "bounded": true,
            "replayable": replayable,
        }],
    })
}

fn reference() -> ConnectorRef {
    ConnectorRef::new(ConnectorId::parse("io.rapidbyte.log").expect("a valid id"))
}

/// The log source in a process of its own, spawned for this placement.
async fn spawned(config: &serde_json::Value) -> Arc<dyn Source> {
    let reference = reference().path(crate::support::example("serve_log"));
    let placed = crate::support::local().source(&reference, config).await;
    Arc::from(placed.expect("the log source starts").connector)
}

/// The log source listening over mutual TLS: its process, started again at will at one address.
struct Listening {
    pki: Pki,
    port: u16,
    process: tokio::sync::Mutex<Child>,
}

impl Listening {
    async fn start() -> Arc<Self> {
        let pki = Pki::new("ca");
        let (process, port) = Self::process(&pki, 0).await;
        Arc::new(Self {
            pki,
            port,
            process: tokio::sync::Mutex::new(process),
        })
    }

    /// A process listening at `port`, any free one where it is 0, and the port it announced.
    async fn process(pki: &Pki, port: u16) -> (Child, u16) {
        let server = pki.server("server", &["localhost"]);
        let mut child = Command::new(crate::support::example("serve_log"))
            .args(["--listen", &format!("127.0.0.1:{port}")])
            .arg("--tls-cert")
            .arg(&server.cert)
            .arg("--tls-key")
            .arg(&server.key)
            .arg("--tls-client-ca")
            .arg(pki.ca())
            .args(["--tls-allow-host", "host"])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .process_group(0)
            .spawn()
            .expect("the connector starts");
        crate::support::guarded(&child);
        let stdout = child.stdout.take().expect("its output is piped");
        let line = BufReader::new(stdout).lines().next_line().await;
        let line = line.expect("its output reads").expect("it announces");
        let port = line.rsplit_once(':').expect("an address and port").1;
        (child, port.parse().expect("a port"))
    }

    /// Ends the process and starts another at the same address: it remembers nothing it sent.
    async fn restart(&self) {
        let mut process = self.process.lock().await;
        process.kill().await.expect("the connector ends");
        *process = Self::process(&self.pki, self.port).await.0;
    }

    /// The source placed at the connector's endpoint, redialed when it is lost.
    async fn source(&self, config: &serde_json::Value) -> Arc<dyn Source> {
        let host = self.pki.client("host");
        let identity = Identity {
            cert: host.cert,
            key: host.key,
        };
        let reference = reference().endpoint(format!("grpcs://localhost:{}", self.port));
        let placed = Remote::new(identity, self.pki.ca())
            .source(&reference, config)
            .await;
        Arc::from(placed.expect("the log source is placed").connector)
    }
}

/// The offsets of the messages published to `events` in `store`, sorted.
fn offsets(store: &str) -> Vec<u64> {
    let mut offsets: Vec<u64> = published_json(store, "events")
        .iter()
        .map(|row| row["offset"].as_u64().expect("a message has its offset"))
        .collect();
    offsets.sort_unstable();
    offsets
}

/// The offset the log source's group file at `group` keeps for the partition of `events`:
/// what the source was last told is committed, as it outlives the source's process.
fn kept(group: &Path) -> Option<u64> {
    let bytes = std::fs::read(group).ok()?;
    let kept: Vec<(String, String, u64)> =
        serde_json::from_slice(&bytes).expect("the group file lists its positions");
    let events = kept.iter().find(|(stream, _, _)| stream == "events");
    events.map(|(_, _, offset)| *offset)
}

fn incremental(name: &str) -> rdlt_engine::PipelinePlan {
    pipeline(name, [stream("events").read(ReadMode::Incremental)])
}

#[tokio::test]
async fn a_run_with_nothing_new_from_a_served_source_completes_in_one_attempt() {
    for replayable in [true, false] {
        for listens in [false, true] {
            let name = format!("idle-{replayable}-{listens}");
            let base = tempfile::tempdir().expect("a temporary directory");
            let group = base.path().join("events.group");
            let config = log(50, replayable, &group);
            let engine = engine(retrying(3));
            let listening = Listening::start().await;
            let place = || async {
                if listens {
                    listening.source(&config).await
                } else {
                    spawned(&config).await
                }
            };
            let first = engine
                .run(incremental(&name), place().await, memory(&name).await)
                .await;
            assert_eq!(first.report.status, RunStatus::Succeeded, "{name}");
            assert_eq!(first.report.rows, 50, "{name}");
            assert_eq!(kept(&group), Some(50), "{name}");
            // The source's next process sent no host anything. It hears the host for where it
            // reads from, which is all an idle run reports.
            listening.restart().await;
            for _ in 0..2 {
                let idle = engine
                    .run(incremental(&name), place().await, memory(&name).await)
                    .await;
                assert_eq!(idle.report.status, RunStatus::Succeeded, "{name}");
                assert_eq!((idle.report.attempted, idle.report.rows), (1, 0), "{name}");
                assert_eq!(kept(&group), Some(50), "{name}");
            }
            assert_eq!(offsets(&name), (0..50).collect::<Vec<_>>(), "{name}");
        }
    }
}

/// The acknowledgements `script` heard, each as its partition and position, sorted.
fn heard(script: &Script) -> Vec<(String, u64)> {
    let mut heard: Vec<(String, u64)> = script
        .acks
        .lock()
        .iter()
        .map(|(_, partition, next)| (partition.clone(), *next))
        .collect();
    heard.sort();
    heard
}

#[tokio::test(start_paused = true)]
async fn every_partition_a_commit_covers_is_reported_whether_it_moved_or_not() {
    let (script, source) = Script::new(vec![ScriptStream::new("events", 2, 10, 5)])
        .connect("ack_covered")
        .await;
    let run = || async {
        let destination = memory("ack_covered").await;
        let plan = incremental("ack-covered");
        engine(commit_every(100))
            .run(plan, Arc::clone(&source), destination)
            .await
    };
    let first = run().await;
    assert_eq!(first.report.status, RunStatus::Succeeded);
    let told: Vec<u64> = heard(&script).into_iter().map(|(_, next)| next).collect();
    assert_eq!(told, [10, 10]);
    // One partition gains rows, the other none: both are reported, each where it stands.
    script.acks.lock().clear();
    script.streams[0].rows[0].store(25, Ordering::SeqCst);
    let second = run().await;
    assert_eq!(second.report.status, RunStatus::Succeeded);
    assert_eq!((second.report.attempted, second.report.rows), (1, 15));
    let mut positions: Vec<u64> = heard(&script).into_iter().map(|(_, next)| next).collect();
    positions.sort_unstable();
    assert_eq!(positions, [10, 25]);
    // Nothing new anywhere: every partition is reported again, in one attempt.
    script.acks.lock().clear();
    let third = run().await;
    assert_eq!(third.report.status, RunStatus::Succeeded);
    assert_eq!((third.report.attempted, third.report.rows), (1, 0));
    let mut positions: Vec<u64> = heard(&script).into_iter().map(|(_, next)| next).collect();
    positions.sort_unstable();
    assert_eq!(positions, [10, 25]);
}

#[tokio::test(start_paused = true)]
async fn a_report_refused_in_one_attempt_is_made_again_by_the_next_though_nothing_moves() {
    for replayable in [true, false] {
        let name = format!("ack_again_{replayable}");
        let mut events = ScriptStream::new("events", 1, 10, 5);
        events.replayable = replayable;
        let (script, source) = Script::new(vec![events]).connect(&name).await;
        // The first report is refused, as one lost on its way is.
        script.limited_acks.store(1, Ordering::SeqCst);
        let engine = engine(retrying(3).commit(commit_all()));
        let plan = || incremental(&name.replace('_', "-"));
        let first = engine
            .run(plan(), Arc::clone(&source), memory(&name).await)
            .await;
        assert_eq!(first.report.status, RunStatus::Succeeded, "{name}");
        assert_eq!(
            (first.report.attempted, first.report.rows),
            (2, 10),
            "{name}"
        );
        // The second attempt read nothing, and still told the source where it stands.
        assert_eq!(heard(&script), [("p0".to_owned(), 10)], "{name}");
        assert_eq!(script.early_reads.load(Ordering::SeqCst), 0, "{name}");
        // The partition never moves again, and the source keeps hearing the same position.
        for _ in 0..2 {
            script.acks.lock().clear();
            let idle = engine
                .run(plan(), reconnect(&name).await, memory(&name).await)
                .await;
            assert_eq!(idle.report.status, RunStatus::Succeeded, "{name}");
            assert_eq!((idle.report.attempted, idle.report.rows), (1, 0), "{name}");
            assert_eq!(heard(&script), [("p0".to_owned(), 10)], "{name}");
        }
    }
}

/// A commit policy that commits once, when every read has ended.
fn commit_all() -> rdlt_engine::CommitPolicy {
    rdlt_engine::CommitPolicy::new(None, Some(1000), None).expect("a row threshold is valid")
}

#[tokio::test]
async fn a_connector_started_again_before_it_hears_of_a_commit_costs_one_attempt_and_no_row() {
    // Each source is told once the destination's commit has landed, whether it reads again or
    // forgets: the connector is started again just before.
    for replayable in [true, false] {
        let name = format!("restarted-{replayable}");
        let base = tempfile::tempdir().expect("a temporary directory");
        let group = base.path().join("events.group");
        // One commit, once the read has ended: the connector is idle when it is started again.
        let config = commit_every(1000).retry(
            rdlt_engine::RetryPolicy::default()
                .max_attempts(3)
                .initial(Duration::from_millis(10))
                .max_delay(Duration::from_millis(100)),
        );
        let engine = engine(config);
        let listening = Listening::start().await;
        let restarting = Arc::clone(&listening);
        let hook: Hook = Arc::new(move || {
            let listening = Arc::clone(&restarting);
            Box::pin(async move { listening.restart().await })
        });
        let destination = hooked(memory(&name).await, At::Landed, hook);
        let source = listening.source(&log(50, replayable, &group)).await;
        let loaded = engine.run(incremental(&name), source, destination).await;
        assert_eq!(loaded.report.status, RunStatus::Succeeded, "{name}");
        // The attempt whose report the new process refused, then one that had nothing to report.
        assert_eq!(loaded.report.attempted, 2, "{name}");
        assert_eq!(offsets(&name), (0..50).collect::<Vec<_>>(), "{name}");
        // The attempt that had nothing to read told the source where it stands all the same.
        assert_eq!(kept(&group), Some(50), "{name}");
        let grown = listening.source(&log(80, replayable, &group)).await;
        let more = engine
            .run(incremental(&name), grown, memory(&name).await)
            .await;
        assert_eq!(more.report.status, RunStatus::Succeeded, "{name}");
        assert_eq!((more.report.attempted, more.report.rows), (1, 30), "{name}");
        assert_eq!(offsets(&name), (0..80).collect::<Vec<_>>(), "{name}");
        let idle = listening.source(&log(80, replayable, &group)).await;
        let last = engine
            .run(incremental(&name), idle, memory(&name).await)
            .await;
        assert_eq!((last.report.attempted, last.report.rows), (1, 0), "{name}");
        assert_eq!(kept(&group), Some(80), "{name}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_source_that_refuses_every_report_fails_the_run_once_no_attempt_is_left() {
    for replayable in [true, false] {
        let name = format!("ack_refused_{replayable}");
        let mut events = ScriptStream::new("events", 1, 10, 5);
        events.replayable = replayable;
        let (script, source) = Script::new(vec![events]).connect(&name).await;
        script.limited_acks.store(usize::MAX, Ordering::SeqCst);
        let engine = engine(retrying(3).commit(commit_all()));
        let plan = incremental(&name.replace('_', "-"));
        let running = engine.run(plan, source, memory(&name).await);
        // Bounded on the paused clock: a run that retried without end would fail here at once.
        let outcome = tokio::time::timeout(Duration::from_secs(600), running).await;
        let outcome = outcome.expect("the run ends once no attempt is left");
        // Every attempt reported the rows' position and was refused, and none that landed
        // nothing reset the count of failures, whether the source reads again or forgets.
        let attempts = 3;
        assert_eq!(outcome.report.status, RunStatus::Failed, "{name}");
        assert_eq!(outcome.report.attempted, attempts, "{name}");
        assert!(script.acks.lock().is_empty(), "{name}");
        let refused = usize::MAX - script.limited_acks.load(Ordering::SeqCst);
        assert_eq!(refused as u64, attempts, "{name}");
        let error = outcome.error.expect("the run failed");
        assert_eq!(error.kind(), ErrorKind::Source, "{name}");
        assert!(error.is_retryable(), "{name}");
        let stream = error.stream().map(ToString::to_string);
        assert_eq!(stream.as_deref(), Some("events"), "{name}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_report_refused_with_no_wait_fails_a_default_run_once_no_attempt_is_left() {
    let (script, source) = Script::new(vec![ScriptStream::new("events", 1, 10, 5)])
        .connect("ack_no_wait")
        .await;
    script.limited_acks.store(usize::MAX, Ordering::SeqCst);
    *script.limited_wait.lock() = Some(Duration::ZERO);
    // One commit, at the partition's end, lands every row in the first attempt.
    let running = engine(commit_every(100)).run(
        incremental("ack-no-wait"),
        source,
        memory("ack_no_wait").await,
    );
    // Each attempt after the first lands a commit that records the partition where it stood:
    // no progress, so the default policy's five attempts end the run.
    let outcome = tokio::time::timeout(Duration::from_secs(600), running).await;
    let outcome = outcome.expect("the run ends once no attempt is left");
    assert_eq!(outcome.report.status, RunStatus::Failed);
    assert_eq!(outcome.report.attempted, 5);
    assert_eq!(outcome.report.rows, 10);
}

/// A stream of fifteen rows in batches of five, with a checkpoint after the second batch and
/// none after the last: its partition ends done after its last checkpoint.
fn ending_past_its_checkpoint(replayable: bool) -> ScriptStream {
    let mut events = ScriptStream::new("events", 1, 15, 5);
    events.checkpoint_every = 2;
    events.final_checkpoint = false;
    events.replayable = replayable;
    events
}

#[tokio::test(start_paused = true)]
async fn a_report_refused_is_made_again_though_its_partition_then_ends_done() {
    for replayable in [true, false] {
        let name = format!("ack_done_{replayable}");
        let events = ending_past_its_checkpoint(replayable);
        let (script, source) = Script::new(vec![events]).connect(&name).await;
        // The report of the checkpoint at ten is refused.
        script.limited_acks.store(1, Ordering::SeqCst);
        let engine = engine(retrying(3).commit(each_seal()));
        let plan = incremental(&name.replace('_', "-"));
        let outcome = engine.run(plan, source, memory(&name).await).await;
        assert_eq!(outcome.report.status, RunStatus::Succeeded, "{name}");
        assert_eq!(outcome.report.attempted, 1, "{name}");
        // The commit that ends the partition has no checkpoint of its own to report, and tells
        // the source the position it stood at, which the refused report owed it.
        assert_eq!(heard(&script), [("p0".to_owned(), 10)], "{name}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_run_whose_partition_ends_done_succeeds_though_its_source_refused_every_report() {
    let events = ending_past_its_checkpoint(true);
    let (script, source) = Script::new(vec![events]).connect("ack_done_refused").await;
    script.limited_acks.store(usize::MAX, Ordering::SeqCst);
    let running = engine(retrying(3).commit(each_seal())).run(
        incremental("ack-done-refused"),
        source,
        memory("ack_done_refused").await,
    );
    let outcome = tokio::time::timeout(Duration::from_secs(600), running).await;
    let outcome = outcome.expect("the run ends");
    // Refused are the checkpoint's report, the done commit's of where it stood, and the last
    // commit's, which publishes nothing else: the attempt reads on, then gives the done
    // partition's cursor up, as no later attempt reads the partition.
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    assert_eq!(outcome.report.attempted, 1);
    assert_eq!(outcome.report.rows, 15);
    assert!(script.acks.lock().is_empty());
    assert_eq!(script.limited_acks.load(Ordering::SeqCst), usize::MAX - 3);
    assert_eq!(
        outcome.report.counters.acks,
        Acks {
            failed: 3,
            dropped: 1
        }
    );
}

/// A commit policy that commits each seal.
fn each_seal() -> rdlt_engine::CommitPolicy {
    rdlt_engine::CommitPolicy::new(None, Some(1), None).expect("a row threshold is valid")
}

#[tokio::test(start_paused = true)]
async fn a_report_refused_mid_run_is_made_by_the_next_commit_of_the_same_attempt() {
    for replayable in [true, false] {
        let name = format!("ack_mid_{replayable}");
        let mut events = ScriptStream::new("events", 1, 15, 5);
        events.replayable = replayable;
        let (script, source) = Script::new(vec![events]).connect(&name).await;
        // The report of the first checkpoint, at five, is refused.
        script.limited_acks.store(1, Ordering::SeqCst);
        let engine = engine(retrying(3).commit(each_seal()));
        let plan = incremental(&name.replace('_', "-"));
        let outcome = engine.run(plan, source, memory(&name).await).await;
        assert_eq!(outcome.report.status, RunStatus::Succeeded, "{name}");
        assert_eq!(outcome.report.attempted, 1, "{name}");
        let acks = outcome.report.counters.acks;
        assert_eq!(
            acks,
            Acks {
                failed: 1,
                dropped: 0
            },
            "{name}"
        );
        // The next commit tells the partition its own checkpoint, in place of the cursor refused,
        // and every later commit where it stands.
        let told: Vec<u64> = script.acks.lock().iter().map(|ack| ack.2).collect();
        assert_eq!(told[..2], [10, 15], "{name}");
        assert!(told[2..].iter().all(|next| *next == 15), "{name}: {told:?}");
        let ids: Vec<i64> = (0..15).map(|offset| id(0, offset)).collect();
        assert_eq!(published_ids(&name, "events"), ids, "{name}");
        assert_eq!(script.early_reads.load(Ordering::SeqCst), 0, "{name}");
    }
}

#[tokio::test(start_paused = true)]
async fn an_idle_partition_whose_report_failed_is_told_by_the_next_interval_commit() {
    const EVERY: Duration = Duration::from_secs(10);
    let mut events = ScriptStream::new("events", 1, 10, 5);
    events.idle = true;
    let (script, source) = Script::new(vec![events]).connect("ack_idle").await;
    script.limited_acks.store(1, Ordering::SeqCst);
    let policy = rdlt_engine::CommitPolicy::new(Some(EVERY), None, None).expect("a valid policy");
    let plan = incremental("ack-idle").with_until(Until::For(Duration::from_secs(60)));
    let outcome = engine(retrying(3).commit(policy))
        .run(plan, source, memory("ack_idle").await)
        .await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    // One commit landed the rows, and its report was refused; the partition sealed nothing
    // after it, and the next interval told it again.
    assert_eq!((outcome.report.attempted, outcome.report.commits), (1, 1));
    assert_eq!(heard(&script), [("p0".to_owned(), 10)]);
    let (refused, told) = (script.refused_at.lock(), script.told_at.lock());
    let [refused] = refused[..] else {
        panic!("one report is refused: {refused:?}");
    };
    let [told] = told[..] else {
        panic!("one report is told: {told:?}");
    };
    assert!(
        told.duration_since(refused) <= EVERY,
        "told {:?} after it was refused",
        told.duration_since(refused)
    );
}

#[tokio::test(start_paused = true)]
async fn a_report_failed_at_the_last_commit_ends_the_attempt_once_every_stream_is_told() {
    let streams = vec![
        ScriptStream::new("events", 1, 10, 5),
        ScriptStream::new("orders", 1, 10, 5),
    ];
    let (script, source) = Script::new(streams).connect("ack_streams").await;
    script.limited_acks.store(1, Ordering::SeqCst);
    let plan = pipeline(
        "ack-streams",
        [
            stream("events").read(ReadMode::Incremental),
            stream("orders").read(ReadMode::Incremental),
        ],
    );
    let outcome = engine(retrying(3).commit(commit_all()))
        .run(plan, source, memory("ack_streams").await)
        .await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    assert_eq!(outcome.report.attempted, 2);
    // The first attempt's last commit tells one stream though the other's report was refused,
    // and the second attempt, which reads nothing, tells both.
    let told: Vec<String> = script.acks.lock().iter().map(|ack| ack.0.clone()).collect();
    assert_eq!(told.len(), 3, "{told:?}");
    for stream in ["events", "orders"] {
        assert!(told.iter().any(|told| told == stream), "{told:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_source_that_cannot_read_again_serves_again_what_a_failed_commit_held() {
    let mut events = ScriptStream::new("events", 2, 30, 7);
    events.replayable = false;
    let (script, source) = Script::new(vec![events]).connect("unheard_retried").await;
    let destination = failing(memory("unheard_retried").await, Step::CommitOnce);
    let outcome = engine(retrying(3))
        .run(incremental("unheard-retried"), source, destination)
        .await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(outcome.report.attempts.len(), 2);
    // The failed commit's rows were never acknowledged: the source serves them again, and they
    // land once.
    let mut ids: Vec<i64> = (0..2)
        .flat_map(|partition| (0..30).map(move |offset| id(partition, offset)))
        .collect();
    ids.sort_unstable();
    assert_eq!(published_ids("unheard_retried", "events"), ids);
    assert_eq!(script.early_reads.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn attempts_that_commit_no_change_and_fail_end_the_run_once_none_is_left() {
    let (_, source) = Script::new(vec![ScriptStream::new("events", 1, 10, 5)])
        .connect("no_progress")
        .await;
    let first = engine(retrying(3))
        .run(
            incremental("no-progress"),
            Arc::clone(&source),
            memory("no_progress").await,
        )
        .await;
    assert_eq!(first.report.status, RunStatus::Succeeded);
    // Every later attempt commits where the partition already stood, and then fails.
    let closes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = Arc::clone(&closes);
    let hook: Hook = Arc::new(move || {
        counted.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {})
    });
    let failing = hooked(memory("no_progress").await, At::Close, hook);
    let running = engine(retrying(3)).run(incremental("no-progress"), source, failing);
    // Bounded on the paused clock: three attempts wait well under a second between them, and a
    // run that retried without end would fail here at once, not at the suite's own limit.
    let outcome = tokio::time::timeout(Duration::from_secs(5), running).await;
    let outcome = outcome.expect("the run ends once no attempt is left");
    assert_eq!(outcome.report.status, RunStatus::Failed);
    assert_eq!(outcome.report.attempted, 3);
    assert_eq!(closes.load(Ordering::SeqCst), 3);
    let error = outcome.error.expect("the run failed");
    assert_eq!(error.kind(), ErrorKind::Destination);
    assert!(error.is_retryable());
}
