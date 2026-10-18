//! A connector's configuration and its secrets: resolved for the connector that was verified,
//! and scrubbed from whatever it says back.

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use rdlt_connector::serve::{Served, serve_connection};
use rdlt_connector::{
    BoxFuture, ConnectContext, ConnectorErrorKind, ConnectorId, ConnectorSpec, Destination,
    DestinationFactory, Secret, destination_factory, source_factory,
};
use rdlt_connector_reference::{MemoryDestination, MemorySource};
use rdlt_host::{
    Connect, ConnectorRef, DEADLINE_EXCEEDED, Deadlines, Kills, LastWords, Options, Provider as _,
    ProviderError, SecretFault, SecretKind, SecretReference, SecretResolver,
};

use crate::process::{example, local, scripted};
use crate::redial::posing;

/// What no error, log or report may hold.
const CANARY: &str = "hunter2-canary-secret";

/// Resolves every `${secret:canary}` to [`CANARY`], and counts how often it was asked.
#[derive(Debug, Default)]
struct Vault {
    asked: Arc<AtomicUsize>,
}

impl SecretResolver for Vault {
    fn resolve<'a>(
        &'a self,
        reference: &'a SecretReference,
    ) -> BoxFuture<'a, Result<Secret<String>, SecretFault>> {
        Box::pin(async move {
            self.asked.fetch_add(1, Ordering::SeqCst);
            match (reference.kind, reference.name.as_str()) {
                (SecretKind::Named, "canary") => Ok(Secret::new(CANARY.to_owned())),
                _ => Err(SecretFault::Missing),
            }
        })
    }
}

/// Every text an error holds: its own, its code's and each of its causes'.
fn texts(error: &(dyn std::error::Error + 'static)) -> String {
    let mut texts = format!("{error} {error:?}");
    let mut cause = error.source();
    while let Some(error) = cause {
        write!(texts, " {error} {error:?}").ok();
        cause = error.source();
    }
    texts
}

#[tokio::test]
async fn a_secret_is_resolved_for_the_connector_and_reaches_it_as_its_value() {
    let vault = Vault::default();
    let asked = Arc::clone(&vault.asked);
    // The connector's check writes what it was sent to its standard error, and exits.
    let script = serde_json::json!({ "crash": "${secret:canary}" });
    let source = local()
        .secrets(vault)
        .source(&scripted(), &script)
        .await
        .expect("the connector starts")
        .connector;
    assert_eq!(asked.load(Ordering::SeqCst), 1);
    // It crashes saying the secret it was sent: what it said is scrubbed where it is kept.
    let error = source.check().await.expect_err("the connector crashed");
    let words = std::error::Error::source(&error)
        .and_then(|source| source.downcast_ref::<LastWords>())
        .expect("the error carries the connector's last words");
    assert_eq!(words.stderr, r"***\n");
    assert!(!texts(&error).contains("hunter2"), "{}", texts(&error));
}

#[tokio::test]
async fn no_secret_is_resolved_for_a_connector_that_is_not_the_one_named() {
    let config = serde_json::json!({ "pid_file": "${secret:canary}" });
    let other = ConnectorRef::new(ConnectorId::parse("test.other").expect("a valid id"))
        .path(example("scripted_connector"));
    let newer = scripted().version(semver::VersionReq::parse(">=9").expect("valid"));
    for unverified in [other, newer] {
        let vault = Vault::default();
        let asked = Arc::clone(&vault.asked);
        let refused = local().secrets(vault).source(&unverified, &config).await;
        assert!(refused.is_err());
        assert_eq!(
            asked.load(Ordering::SeqCst),
            0,
            "a secret was resolved for it"
        );
    }
}

#[tokio::test]
async fn a_secret_that_does_not_resolve_fails_the_placement_without_configuring_the_connector() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let pid_file = dir.path().join("pid");
    let config = serde_json::json!({
        "pid_file": pid_file,
        "crash": format!("{CANARY} ${{secret:unknown}}"),
    });
    let refused = local()
        .secrets(Vault::default())
        .source(&scripted(), &config)
        .await;
    let error = refused.err().expect("the secret does not resolve");
    assert!(matches!(error, ProviderError::Secret { .. }), "{error}");
    assert_eq!(error.code(), "secret_unresolved");
    let said = texts(&error);
    assert!(said.contains("config field crash"), "{said}");
    assert!(
        !said.contains("hunter2") && !said.contains("unknown"),
        "{said}"
    );
    assert!(!pid_file.exists(), "the connector was configured");
}

#[tokio::test]
async fn a_connector_error_that_says_a_secret_back_is_scrubbed_of_it() {
    // An environment the check does not find: its error quotes what it was told to expect.
    let script = serde_json::json!({ "env": { "RDLT_TEST_ABSENT": "${secret:canary}" } });
    let source = local()
        .secrets(Vault::default())
        .source(&scripted(), &script)
        .await
        .expect("the connector starts")
        .connector;
    let error = source.check().await.expect_err("the variable is not set");
    let said = texts(&error);
    assert!(
        said.contains("RDLT_TEST_ABSENT") && said.contains("***"),
        "{said}"
    );
    assert!(!said.contains("hunter2"), "{said}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_secret_is_resolved_again_each_time_the_connector_is_started() {
    let vault = Vault::default();
    let asked = Arc::clone(&vault.asked);
    let kills = Kills::new();
    let script = serde_json::json!({ "env": { "RDLT_TEST_ABSENT": "${secret:canary}" } });
    let source = local()
        .kills(&kills)
        .secrets(vault)
        .source(&scripted(), &script)
        .await
        .expect("the connector starts")
        .connector;
    kills.kill();
    // Spawned again for a later call, and told its secret anew: its error is scrubbed still.
    for _ in 0..200 {
        let error = source.check().await.expect_err("the check never passes");
        assert!(!texts(&error).contains("hunter2"), "{}", texts(&error));
        if asked.load(Ordering::SeqCst) == 2 && error.to_string().contains("RDLT_TEST_ABSENT") {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the connector was not started again with its secret");
}

/// Resolves `${secret:rotating}` to a new value each time it is asked: `rotated-1`, then
/// `rotated-2`.
#[derive(Debug, Default)]
struct Rotating {
    asked: AtomicUsize,
}

impl SecretResolver for Rotating {
    fn resolve<'a>(
        &'a self,
        _reference: &'a SecretReference,
    ) -> BoxFuture<'a, Result<Secret<String>, SecretFault>> {
        let asked = self.asked.fetch_add(1, Ordering::SeqCst) + 1;
        Box::pin(async move { Ok(Secret::new(format!("rotated-{asked}"))) })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_secret_an_earlier_start_was_sent_is_scrubbed_from_what_a_later_one_says() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let kills = Kills::new();
    let script = serde_json::json!({
        "said": "${secret:rotating}",
        "remembers": dir.path().join("kept"),
    });
    let source = local()
        .kills(&kills)
        .secrets(Rotating::default())
        .source(&scripted(), &script)
        .await
        .expect("the connector starts")
        .connector;
    let first = source.check().await.expect_err("it says what it was sent");
    assert!(!texts(&first).contains("rotated"), "{}", texts(&first));
    kills.kill();
    // Started again and sent another value, it says both: the earlier one is scrubbed too.
    for _ in 0..200 {
        let error = source.check().await.expect_err("it never passes");
        let said = texts(&error);
        assert!(!said.contains("rotated"), "{said}");
        if std::fs::read_to_string(dir.path().join("kept")).is_ok_and(|kept| kept.contains('2')) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the connector was not started again");
}

/// Reaches, through a function, the connector `serving` serves at each opening, numbered from
/// 0; `kills` cuts every connection it opened.
fn reaching(serving: impl Fn(usize) -> Served + Send + Sync + 'static, kills: &Kills) -> Connect {
    let opened = AtomicUsize::new(0);
    Connect::new(move || {
        let stream = crate::support::served(serving(opened.fetch_add(1, Ordering::SeqCst)));
        Box::pin(async move { Ok(Box::new(stream) as Box<dyn rdlt_host::Stream>) })
    })
    .kills(kills)
}

/// The first answer of `check` that is not a transient failure, as an engine's retries would
/// reach it: the first calls may still meet the lost connection.
async fn after_transients<F, Fut>(check: F) -> rdlt_connector::Result<()>
where
    F: Fn() -> Fut,
    Fut: Future<Output = rdlt_connector::Result<()>>,
{
    loop {
        match check().await {
            Err(error) if error.kind() == ConnectorErrorKind::Transient => {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            answer => return answer,
        }
    }
}

/// A destination factory that declares another spec than it serves.
struct Declaring {
    spec: ConnectorSpec,
    inner: Box<dyn DestinationFactory>,
}

impl DestinationFactory for Declaring {
    fn spec(&self) -> &ConnectorSpec {
        &self.spec
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Box<dyn Destination>>> {
        self.inner.connect(config, context)
    }
}

/// The memory destination, declaring itself at `version`.
fn destination_at(version: &str) -> Served {
    let inner = destination_factory::<MemoryDestination>();
    let spec = ConnectorSpec {
        version: version.to_owned(),
        ..inner.spec().clone()
    };
    Served::new().with_destination(Box::new(Declaring { spec, inner }))
}

#[tokio::test]
async fn a_secret_a_source_started_again_gives_as_its_version_is_scrubbed_from_its_refusal() {
    let vault = Vault::default();
    let asked = Arc::clone(&vault.asked);
    let kills = Kills::new();
    let spec = source_factory::<MemorySource>().spec().clone();
    let reference = ConnectorRef::new(spec.id.clone());
    let connect = reaching(
        move |opened| {
            let version = match opened {
                0 => spec.version.clone(),
                _ => format!("9.9.9-{CANARY}"),
            };
            posing(spec.id.as_str(), &version)
        },
        &kills,
    )
    .secrets(vault);
    let config = serde_json::json!({ "streams": { "rows": [{ "id": "${secret:canary}" }] } });
    let refused = tokio::time::timeout(Duration::from_secs(3), async {
        let source = connect
            .source(&reference, &config)
            .await
            .expect("the connector starts")
            .connector;
        kills.kill();
        after_transients(|| source.check()).await
    })
    .await
    .expect("the connector started again is refused within 3 s");
    let error = refused.expect_err("a connector of another version is refused");
    assert_eq!(error.code(), Some("connector_changed"), "{error}");
    assert!(!texts(&error).contains("hunter2"), "{}", texts(&error));
    assert_eq!(
        asked.load(Ordering::SeqCst),
        1,
        "the refused connector was sent a secret"
    );
}

#[tokio::test]
async fn a_secret_a_destination_started_again_gives_as_its_version_is_scrubbed_from_its_refusal() {
    let vault = Vault::default();
    let asked = Arc::clone(&vault.asked);
    let kills = Kills::new();
    let spec = destination_factory::<MemoryDestination>().spec().clone();
    let reference = ConnectorRef::new(spec.id.clone());
    let connect = reaching(
        move |opened| match opened {
            0 => destination_at(&spec.version),
            _ => destination_at(&format!("9.9.9-{CANARY}")),
        },
        &kills,
    )
    .secrets(vault);
    let config = serde_json::json!({ "store": "${secret:canary}" });
    let refused = tokio::time::timeout(Duration::from_secs(3), async {
        let destination = connect
            .destination(&reference, &config)
            .await
            .expect("the connector starts")
            .connector;
        kills.kill();
        after_transients(|| destination.check()).await
    })
    .await
    .expect("the connector started again is refused within 3 s");
    let error = refused.expect_err("a connector of another version is refused");
    assert_eq!(error.code(), Some("connector_changed"), "{error}");
    assert!(!texts(&error).contains("hunter2"), "{}", texts(&error));
    assert_eq!(
        asked.load(Ordering::SeqCst),
        1,
        "the refused connector was sent a secret"
    );
}

/// Resolves `${secret:canary}` for the first `answered` asks and never answers another, as a
/// secret store that stopped answering; counts the asks.
#[derive(Debug, Default)]
struct Stalls {
    answered: usize,
    asked: Arc<AtomicUsize>,
}

impl SecretResolver for Stalls {
    fn resolve<'a>(
        &'a self,
        _reference: &'a SecretReference,
    ) -> BoxFuture<'a, Result<Secret<String>, SecretFault>> {
        let answers = self.asked.fetch_add(1, Ordering::SeqCst) < self.answered;
        Box::pin(async move {
            if !answers {
                std::future::pending::<()>().await;
            }
            Ok(Secret::new(CANARY.to_owned()))
        })
    }
}

/// The memory source, reached over a pipe whose other end serves it, its secrets resolved by
/// `secrets`, within a connect deadline of 50 ms.
fn piped(secrets: Stalls) -> Connect {
    let options = Options {
        deadlines: Deadlines {
            connect: Duration::from_millis(50),
            ..Deadlines::default()
        },
        ..Options::default()
    };
    Connect::new(|| {
        let (host, connector) = tokio::io::duplex(1 << 16);
        let served = Arc::new(Served::new().with_source(source_factory::<MemorySource>()));
        tokio::spawn(serve_connection(
            served,
            connector,
            rdlt_wire::Limits::default(),
        ));
        Box::pin(async move { Ok(Box::new(host) as Box<dyn rdlt_host::Stream>) })
    })
    .secrets(secrets)
    .options(options)
}

fn memory() -> ConnectorRef {
    ConnectorRef::new(ConnectorId::parse("io.rapidbyte.memory").expect("a valid id"))
}

fn canary_rows() -> serde_json::Value {
    serde_json::json!({ "streams": { "rows": [{ "id": "${secret:canary}" }] } })
}

#[tokio::test(start_paused = true)]
async fn a_secret_store_that_never_answers_fails_the_placement_by_the_connect_deadline() {
    let store = Stalls::default();
    let asked = Arc::clone(&store.asked);
    let (connect, connector, rows) = (piped(store), memory(), canary_rows());
    let placing = connect.source(&connector, &rows);
    let error = tokio::time::timeout(Duration::from_secs(1), placing)
        .await
        .expect("the connect deadline ends the placement")
        .err()
        .expect("no secret resolves");
    let ProviderError::HandshakeFailed { source, .. } = &error else {
        panic!("{error}");
    };
    assert_eq!(
        (source.kind(), source.code()),
        (ConnectorErrorKind::Transient, Some(DEADLINE_EXCEEDED))
    );
    // The handshake passed: it was the resolution that ran out of time.
    assert_eq!(asked.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn a_secret_store_that_stops_answering_fails_a_later_start_as_retryable() {
    let kills = Kills::new();
    let store = Stalls {
        answered: 1,
        ..Stalls::default()
    };
    let asked = Arc::clone(&store.asked);
    let placed = piped(store)
        .kills(&kills)
        .source(&memory(), &canary_rows())
        .await
        .expect("the first start resolves its secret");
    kills.kill();
    // The first calls may meet the cut connection before it is started again.
    let mut codes = Vec::new();
    for _ in 0..50 {
        let checked = tokio::time::timeout(Duration::from_secs(1), placed.connector.check());
        let error = checked
            .await
            .expect("each call ends")
            .expect_err("no start resolves its secret again");
        if error.code() == Some(DEADLINE_EXCEEDED) {
            assert_eq!(error.kind(), ConnectorErrorKind::Transient);
            assert_eq!(asked.load(Ordering::SeqCst), 2);
            return;
        }
        codes.push(error.code().map(str::to_owned));
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("no start ran out of time: {codes:?}");
}
