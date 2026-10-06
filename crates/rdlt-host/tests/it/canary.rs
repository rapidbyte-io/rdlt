//! Secrets a configuration refers to, in every place of it, said back by a connector in every
//! way it can: none is in anything the host, the engine or a destination keeps or shows.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;
use std::sync::{Arc, Mutex};

use rdlt_connector::{BoxFuture, PipelineId, Secret, StreamName};
use rdlt_engine::{
    CommitPolicy, Engine, EngineConfig, LocalWal, PipelinePlan, RetryPolicy, RunStatus, StreamPlan,
};
use rdlt_host::{
    Local, Provider as _, Registry, SecretFault, SecretKind, SecretReference, SecretResolver,
};

use crate::process::{local, scripted};
use crate::support::connectors::Echo;

/// What every secret holds, and nothing else the tests write does.
const CANARY: &str = "CANARY";

/// The secrets, each named and each of a kind of text a scrub could miss.
fn secrets() -> BTreeMap<&'static str, &'static str> {
    BTreeMap::from([
        ("plain", "CANARY-plain-0123456789"),
        ("quoted", "CANARY\"quoted\\by\"JSON"),
        ("spaced", "CANARY  with   spaces"),
        ("control", "CANARY\twith\u{1b}[2Jcontrols\r\n"),
        ("wide", "CANARY-päss-名-🦀"),
        ("short", "CANARY"),
    ])
}

/// Resolves `${secret:name}` to the secret of that name.
#[derive(Debug)]
struct Vault;

impl SecretResolver for Vault {
    fn resolve<'a>(
        &'a self,
        reference: &'a SecretReference,
    ) -> BoxFuture<'a, Result<Secret<String>, SecretFault>> {
        Box::pin(async move {
            let secrets = secrets();
            match (reference.kind, secrets.get(reference.name.as_str())) {
                (SecretKind::Named, Some(secret)) => Ok(Secret::new((*secret).to_owned())),
                _ => Err(SecretFault::Missing),
            }
        })
    }
}

/// A value with a secret in every place a value may hold one: a text of its own, within a
/// longer text, twice in one, in a list, in an object within a list, and deep within objects.
fn everywhere() -> serde_json::Value {
    serde_json::json!({
        "token": "${secret:plain}",
        "dsn": "postgres://app:${secret:quoted}@db.internal/prod?sslmode=require",
        "pair": "${secret:plain}${secret:short}",
        "list": ["${secret:spaced}", { "inner": "${secret:control}" }, 7, null],
        "deep": { "deeper": { "deepest": "key=${secret:wide};" } },
    })
}

/// Every text an error holds: its own and each of its causes', as `Display` and `Debug`.
fn texts(error: &(dyn std::error::Error + 'static)) -> String {
    let mut texts = format!("{error} {error:?}");
    let mut cause = error.source();
    while let Some(error) = cause {
        write!(texts, " {error} {error:?}").ok();
        cause = error.source();
    }
    texts
}

/// Every field of every event the host logged, as text.
#[derive(Clone, Default)]
struct Logged(Arc<Mutex<String>>);

impl tracing::field::Visit for Logged {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let mut logged = self.0.lock().expect("no panic holds it");
        writeln!(logged, "{}={value:?}", field.name()).ok();
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        let mut logged = self.0.lock().expect("no panic holds it");
        writeln!(logged, "{}={value}", field.name()).ok();
    }
}

impl tracing::Subscriber for Logged {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        event.record(&mut self.clone());
    }

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

impl Logged {
    fn text(&self) -> String {
        self.0.lock().expect("no panic holds it").clone()
    }

    /// Waits, for five seconds at most, until what was logged holds `expected`.
    async fn until(&self, expected: &str) -> String {
        for _ in 0..500 {
            if self.text().contains(expected) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        self.text()
    }
}

/// Runs `test` on a runtime of one thread, with every event logged on it kept.
fn logging<T>(test: impl Future<Output = T>) -> (T, Logged) {
    let logged = Logged::default();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    let ran = tracing::subscriber::with_default(logged.clone(), || runtime.block_on(test));
    (ran, logged)
}

/// Fails unless `text`, which is `what`, holds none of the secrets, and shows that one was
/// scrubbed where `scrubbed` says some were said.
#[track_caller]
fn clean(what: &str, text: &str, scrubbed: bool) {
    assert!(!text.contains(CANARY), "{what} holds a secret: {text}");
    assert!(
        !scrubbed || text.contains("***"),
        "{what} says nothing: {text}"
    );
}

/// Every byte of every file beneath `dir`, as text.
fn files(dir: &Path) -> String {
    let mut held = String::new();
    for entry in std::fs::read_dir(dir)
        .expect("the directory lists")
        .flatten()
    {
        let path = entry.path();
        writeln!(held, "{}", path.display()).ok();
        if path.is_dir() {
            held.push_str(&files(&path));
        } else {
            held.push_str(&String::from_utf8_lossy(
                &std::fs::read(&path).unwrap_or_default(),
            ));
        }
    }
    held
}

/// A spawned connector told its secrets says them on its output and in its check's error:
/// what `local` keeps of both.
async fn said_by_a_spawned_connector(local: Local, logged: &Logged) {
    let script = serde_json::json!({ "said": everywhere() });
    let source = local
        .clone()
        .secrets(Vault)
        .source(&scripted(), &script)
        .await
        .expect("the connector starts")
        .connector;
    let error = source
        .check()
        .await
        .expect_err("its check says what it was told");
    clean("the check's error", &texts(&error), true);
    // What it wrote as it connected: a line to its standard output, and lines to its error.
    let lines = logged.until("deepest").await;
    clean("the log", &lines, true);
    assert!(
        lines.contains("stream=stdout") && lines.contains("stream=stderr"),
        "{lines}"
    );
    // And what it says as it dies, placed as the caller places it.
    let crashing = serde_json::json!({ "crash": "dying with ${secret:quoted} and ${secret:wide}" });
    let source = local
        .secrets(Vault)
        .source(&scripted(), &crashing)
        .await
        .expect("the connector starts")
        .connector;
    let error = source.check().await.expect_err("the connector crashed");
    clean("the crash's error", &texts(&error), true);
}

#[test]
fn no_secret_a_spawned_connector_says_back_is_in_its_errors_or_the_hosts_log() {
    let logged = Logged::default();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    tracing::subscriber::with_default(logged.clone(), || {
        runtime.block_on(said_by_a_spawned_connector(local(), &logged));
    });
    clean("the log", &logged.text(), true);
}

#[cfg(target_os = "linux")]
#[test]
fn no_secret_a_sandboxed_connector_says_back_is_in_its_errors_or_the_hosts_log() {
    let sandbox = rdlt_host::Bubblewrap::new();
    if let Err(unusable) = sandbox.usable() {
        rdlt_testkit::process::without_sandbox(&unusable);
        return;
    }
    let logged = Logged::default();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    tracing::subscriber::with_default(logged.clone(), || {
        runtime.block_on(said_by_a_spawned_connector(
            Local::sandboxed(sandbox),
            &logged,
        ));
    });
    clean("the log", &logged.text(), true);
}

#[tokio::test]
async fn no_secret_an_in_process_connector_says_back_is_in_its_errors() {
    let registry = Registry::trusted().trusted_source::<Echo>().secrets(Vault);
    let echo = rdlt_host::ConnectorRef::new(
        rdlt_connector::ConnectorId::parse("test.echo").expect("a valid id"),
    );
    let config = serde_json::json!({ "said": everywhere() });
    let placed = registry.source(&echo, &config).await.expect("placed");
    let error = placed
        .connector
        .check()
        .await
        .expect_err("its check says it");
    clean("the check's error", &texts(&error), true);
    assert!(error.code().is_some_and(|code| !code.contains(CANARY)));
    // A connect that fails says it too, in the provider's error.
    let refusing = serde_json::json!({ "said": everywhere(), "refuses": true });
    let refused = registry
        .source(&echo, &refusing)
        .await
        .err()
        .expect("refused");
    clean("the provider's error", &texts(&refused), true);
}

#[test]
fn no_secret_is_in_a_runs_report_its_log_its_state_or_what_its_destination_keeps() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let (wal, out) = (dir.path().join("wal"), dir.path().join("out"));
    // The source says its secrets as it connects, fails a read once saying them again, and
    // is told a path it may keep its mark of that in.
    let script = serde_json::json!({
        "rows": 300,
        "said": everywhere(),
        "fail_once_at": 120,
        "marker": dir.path().join("failed-once"),
    });
    let ((outcome, error_texts), logged) = logging(async {
        let source = local()
            .secrets(Vault)
            .source(&scripted(), &script)
            .await
            .expect("the source starts")
            .connector;
        let files = serde_json::json!({ "root": out, "format": "jsonl" });
        let destination = Registry::trusted()
            .trusted_destination::<rdlt_connector_reference::FilesDestination>()
            .secrets(Vault)
            .destination(&files_reference(), &files)
            .await
            .expect("the destination connects")
            .connector;
        let engine = engine(&wal);
        let stream = StreamPlan::new(StreamName::new("rows").expect("a valid stream name"));
        let plan = PipelinePlan::new(PipelineId::parse("canary").unwrap(), [stream])
            .expect("a valid plan")
            .with_wal(true);
        let outcome = engine
            .run(plan, Arc::from(source), Arc::from(destination))
            .await;
        let error_texts = outcome
            .error
            .as_ref()
            .map(|error| texts(error))
            .unwrap_or_default();
        (outcome, error_texts)
    });
    assert_eq!(outcome.report.status, RunStatus::Succeeded, "{error_texts}");
    // The read that failed is in the report, with what the source said of it scrubbed.
    assert!(outcome.report.attempts.len() >= 2, "no attempt failed");
    let report = serde_json::to_string(&outcome.report).expect("a report serializes");
    clean("the report", &report, true);
    clean("the report as text", &format!("{:?}", outcome.report), true);
    clean("the run's error", &error_texts, false);
    clean("the log", &logged.text(), true);
    // The write-ahead log, the state the destination keeps, and every file it published.
    let kept = files(dir.path());
    assert!(
        kept.contains("manifest") || kept.contains("rows"),
        "nothing was kept: {kept}"
    );
    clean("what the run kept on disk", &kept, false);
}

/// An engine that commits every fifty rows, retries a failed attempt at once, and keeps its
/// write-ahead logs in `wal`.
fn engine(wal: &Path) -> Engine {
    let policy = CommitPolicy::new(None, Some(50), None).expect("a row threshold is valid");
    let retry = RetryPolicy::default()
        .max_attempts(5)
        .initial(std::time::Duration::from_millis(10));
    let config = EngineConfig::builder()
        .commit(policy)
        .retry(retry)
        .lanes(2)
        .build()
        .expect("the engine's configuration is valid");
    let env = crate::support::system_env().with_wal(Arc::new(LocalWal::new(wal)));
    Engine::new(config, Arc::new(env))
}

/// The reference to the files destination.
fn files_reference() -> rdlt_host::ConnectorRef {
    rdlt_host::ConnectorRef::new(
        rdlt_connector::ConnectorId::parse("io.rapidbyte.files").expect("a valid id"),
    )
}
