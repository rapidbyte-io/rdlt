//! What the engine tells a source is committed: the position of every partition a commit covers,
//! moved or not, so a report that failed is made again by the next attempt.
//!
//! A served source hears a host for what it sent that host and for where the host reads from,
//! so a partition that is sent nothing is still reported where its read started.

use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arrow_array::RecordBatch;
use rdlt_connector::{
    BoxFuture, Capabilities, CommitMeta, ConnectorErrorKind, ConnectorId, Destination,
    DestinationSession, DestinationWriter, OpenContext, OpenedSession, ReadMode, Receipt, Result,
    SegmentId, Source, TableChange, TableRef, WriteStats,
};
use rdlt_engine::{ErrorKind, LocalWal, RunStatus, WalStore};
use rdlt_host::{ConnectorRef, Identity, Provider as _, Remote};
use rdlt_testkit::tls::Pki;
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::process::{Child, Command};

use crate::support::script::{Script, ScriptStream, reconnect};
use crate::support::{
    commit_every, engine, logging_engine, memory, pipeline, published_json, retrying, stream,
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
            .spawn()
            .expect("the connector starts");
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
            let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path().join("wal")));
            let engine = logging_engine(retrying(3), store);
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
    let base = tempfile::tempdir().expect("a temporary directory");
    for replayable in [true, false] {
        let name = format!("ack_again_{replayable}");
        let mut events = ScriptStream::new("events", 1, 10, 5);
        events.replayable = replayable;
        let (script, source) = Script::new(vec![events]).connect(&name).await;
        // The first report is refused, as one lost on its way is.
        script.limited_acks.store(1, Ordering::SeqCst);
        let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path().join(&name)));
        let engine = logging_engine(retrying(3).commit(commit_all()), store);
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

/// When a [`Hooked`] destination calls its hook.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum At {
    /// Before the first flush of a writer: before a commit is logged or reported.
    Flush,
    /// Once the first commit has landed, before it is reported.
    Landed,
    /// At every close of a session, which then fails.
    Close,
}

type Hook = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

/// `inner`, calling `hook` once, at `at`.
struct Hooked {
    inner: Arc<dyn Destination>,
    at: At,
    hook: Hook,
    called: Arc<AtomicBool>,
}

/// `inner`, calling `hook` at `at`.
fn hooked(inner: Arc<dyn Destination>, at: At, hook: Hook) -> Arc<dyn Destination> {
    Arc::new(Hooked {
        inner,
        at,
        hook,
        called: Arc::new(AtomicBool::new(false)),
    })
}

impl Destination for Hooked {
    fn capabilities(&self) -> &Capabilities {
        self.inner.capabilities()
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        self.inner.check()
    }

    fn open<'a>(&'a self, context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        Box::pin(async move {
            let opened = self.inner.open(context).await?;
            let session = HookedSession {
                inner: opened.session,
                at: self.at,
                hook: Arc::clone(&self.hook),
                called: Arc::clone(&self.called),
            };
            Ok(OpenedSession {
                session: Box::new(session),
                ..opened
            })
        })
    }
}

struct HookedSession {
    inner: Box<dyn DestinationSession>,
    at: At,
    hook: Hook,
    called: Arc<AtomicBool>,
}

/// Calls `hook` the first time it is reached.
async fn once(called: &AtomicBool, hook: &Hook) {
    if !called.swap(true, Ordering::SeqCst) {
        hook().await;
    }
}

impl DestinationSession for HookedSession {
    fn apply_schema<'a>(&'a mut self, change: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        self.inner.apply_schema(change)
    }

    fn writer<'a>(
        &'a mut self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        Box::pin(async move {
            let inner = self.inner.writer(table).await?;
            let flushing = (self.at == At::Flush).then(|| Arc::clone(&self.hook));
            let writer = HookedWriter {
                inner,
                hook: flushing,
                called: Arc::clone(&self.called),
            };
            Ok(Box::new(writer) as Box<dyn DestinationWriter>)
        })
    }

    fn commit<'a>(&'a mut self, meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        Box::pin(async move {
            let receipt = self.inner.commit(meta).await?;
            if self.at == At::Landed {
                once(&self.called, &self.hook).await;
            }
            Ok(receipt)
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            self.inner.close().await?;
            if self.at == At::Close {
                (self.hook)().await;
                let message = "the session did not close";
                return Err(rdlt_connector::ConnectorError::new(
                    ConnectorErrorKind::Transient,
                    message,
                ));
            }
            Ok(())
        })
    }
}

struct HookedWriter {
    inner: Box<dyn DestinationWriter>,
    hook: Option<Hook>,
    called: Arc<AtomicBool>,
}

impl DestinationWriter for HookedWriter {
    fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> BoxFuture<'_, Result<()>> {
        self.inner.write(segment, batch)
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<WriteStats>> {
        Box::pin(async move {
            if let Some(hook) = &self.hook {
                once(&self.called, hook).await;
            }
            self.inner.flush().await
        })
    }
}

#[tokio::test]
async fn a_connector_started_again_before_it_hears_of_a_commit_costs_one_attempt_and_no_row() {
    // A source that reads again is told after the destination's commit; one that forgets, once
    // the commit is logged and before the destination has it.
    for (replayable, at) in [(true, At::Landed), (false, At::Flush)] {
        let name = format!("restarted-{replayable}");
        let base = tempfile::tempdir().expect("a temporary directory");
        let group = base.path().join("events.group");
        let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path().join("wal")));
        // One commit, once the read has ended: the connector is idle when it is started again.
        let config = commit_every(1000).retry(
            rdlt_engine::RetryPolicy::default()
                .max_attempts(3)
                .initial(Duration::from_millis(10))
                .max_delay(Duration::from_millis(100)),
        );
        let engine = logging_engine(config, store);
        let listening = Listening::start().await;
        let restarting = Arc::clone(&listening);
        let hook: Hook = Arc::new(move || {
            let listening = Arc::clone(&restarting);
            Box::pin(async move { listening.restart().await })
        });
        let destination = hooked(memory(&name).await, at, hook);
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
    let base = tempfile::tempdir().expect("a temporary directory");
    for replayable in [true, false] {
        let name = format!("ack_refused_{replayable}");
        let mut events = ScriptStream::new("events", 1, 10, 5);
        events.replayable = replayable;
        let (script, source) = Script::new(vec![events]).connect(&name).await;
        script.limited_acks.store(usize::MAX, Ordering::SeqCst);
        let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path().join(&name)));
        let engine = logging_engine(retrying(3).commit(commit_all()), store);
        let plan = incremental(&name.replace('_', "-"));
        let running = engine.run(plan, source, memory(&name).await);
        // Bounded on the paused clock: a run that retried without end would fail here at once.
        let outcome = tokio::time::timeout(Duration::from_secs(600), running).await;
        let outcome = outcome.expect("the run ends once no attempt is left");
        // Every attempt reported the rows' position and was refused, and none that landed
        // nothing reset the count of failures. A source that forgets is told before the rows
        // land: the second attempt landed them from the log, so one more was made.
        let attempts = if replayable { 3 } else { 4 };
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
    let base = tempfile::tempdir().expect("a temporary directory");
    for replayable in [true, false] {
        let name = format!("ack_done_{replayable}");
        let events = ending_past_its_checkpoint(replayable);
        let (script, source) = Script::new(vec![events]).connect(&name).await;
        // The report of the checkpoint at ten is refused.
        script.limited_acks.store(1, Ordering::SeqCst);
        let each = rdlt_engine::CommitPolicy::new(None, Some(1), None).expect("a valid policy");
        let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path().join(&name)));
        let engine = logging_engine(retrying(3).commit(each), store);
        let plan = incremental(&name.replace('_', "-"));
        let outcome = engine.run(plan, source, memory(&name).await).await;
        assert_eq!(outcome.report.status, RunStatus::Succeeded, "{name}");
        assert_eq!(outcome.report.attempted, 2, "{name}");
        // The commit that ends the partition has no checkpoint of its own to report, and tells
        // the source the position it stood at, which the refused report was of.
        assert_eq!(heard(&script), [("p0".to_owned(), 10)], "{name}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_run_whose_partition_ends_done_succeeds_though_its_source_refused_every_report() {
    let events = ending_past_its_checkpoint(true);
    let (script, source) = Script::new(vec![events]).connect("ack_done_refused").await;
    script.limited_acks.store(usize::MAX, Ordering::SeqCst);
    let each = rdlt_engine::CommitPolicy::new(None, Some(1), None).expect("a valid policy");
    let running = engine(retrying(3).commit(each)).run(
        incremental("ack-done-refused"),
        source,
        memory("ack_done_refused").await,
    );
    let outcome = tokio::time::timeout(Duration::from_secs(600), running).await;
    let outcome = outcome.expect("the run ends");
    // The first attempt reports the checkpoint and the second, which ends the partition, where
    // it stood: both are refused and fail. Each landed rows, so neither is counted against the
    // run, and the third finds the partition done, which state holds no cursor for.
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    assert_eq!(outcome.report.attempted, 3);
    assert_eq!(outcome.report.rows, 15);
    assert!(script.acks.lock().is_empty());
    assert_eq!(script.limited_acks.load(Ordering::SeqCst), usize::MAX - 2);
}

#[tokio::test(start_paused = true)]
async fn rows_an_attempt_lands_from_the_log_an_earlier_one_left_are_progress() {
    let base = tempfile::tempdir().expect("a temporary directory");
    let mut events = ScriptStream::new("events", 1, 15, 5);
    events.replayable = false;
    let (script, source) = Script::new(vec![events]).connect("ack_replayed").await;
    // The first two reports are refused, each before its commit lands: the attempt after
    // lands it from the log, and reads on.
    script.limited_acks.store(2, Ordering::SeqCst);
    let each = rdlt_engine::CommitPolicy::new(None, Some(1), None).expect("a valid policy");
    let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path().join("ack_replayed")));
    let engine = logging_engine(retrying(2).commit(each), store);
    let plan = incremental("ack-replayed");
    let outcome = engine.run(plan, source, memory("ack_replayed").await).await;
    // Two failed attempts are as many as the run allows, were the rows the second landed not
    // counted: it lands the first's, so the third is made.
    assert_eq!(outcome.report.status, RunStatus::Succeeded);
    assert_eq!((outcome.report.attempted, outcome.report.rows), (3, 15));
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
