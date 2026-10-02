//! A connector's configuration and its secrets: resolved for the connector that was verified,
//! and scrubbed from whatever it says back.

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rdlt_connector::{BoxFuture, ConnectorId, Secret};
use rdlt_host::{
    ConnectorRef, LastWords, Provider as _, ProviderError, SecretFault, SecretKind,
    SecretReference, SecretResolver,
};

use crate::process::{example, local, scripted};

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
    let kills = rdlt_host::Kills::new();
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
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("the connector was not started again with its secret");
}
