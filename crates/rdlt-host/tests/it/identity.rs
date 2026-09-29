//! A connector is checked to be the connector placed before it sees its configuration: its
//! handshake's id and version, and its binary's digest, when placed and whenever it is spawned
//! again.

use std::io::Write as _;
use std::time::Duration;

use rdlt_connector::{ConnectorId, Source};
use rdlt_host::{ConnectorRef, Digest, Kills, Provider as _, ProviderError};
use sha2::Digest as _;

use crate::process::{example, local, scripted, wait_gone};

/// A script that has the connector write its process id to `pid_file` as it connects.
fn writing(pid_file: &std::path::Path) -> serde_json::Value {
    serde_json::json!({ "pid_file": pid_file })
}

async fn refused(reference: &ConnectorRef, script: &serde_json::Value) -> ProviderError {
    local()
        .source(reference, script)
        .await
        .err()
        .expect("the connector is refused")
}

#[tokio::test]
async fn a_connector_refused_at_its_handshake_never_sees_its_configuration() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let pid_file = dir.path().join("pid");
    let script = writing(&pid_file);
    let other = ConnectorRef::new(ConnectorId::parse("test.other").expect("a valid id"))
        .path(example("scripted_connector"));
    let error = refused(&other, &script).await;
    assert!(
        matches!(error, ProviderError::HandshakeFailed { .. }),
        "{error}"
    );
    let newer = scripted().version(semver::VersionReq::parse(">=9").expect("valid"));
    let error = refused(&newer, &script).await;
    assert!(
        matches!(error, ProviderError::VersionMismatch { .. }),
        "{error}"
    );
    assert!(!pid_file.exists(), "a refused connector connected");
    local()
        .source(&scripted(), &script)
        .await
        .expect("the connector it names is accepted");
    assert!(pid_file.exists(), "the accepted connector connected");
}

#[tokio::test]
async fn a_binary_of_another_digest_than_its_reference_requires_is_never_spawned() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let pid_file = dir.path().join("pid");
    let script = writing(&pid_file);
    let error = refused(&scripted().digest(Digest([0; 32])), &script).await;
    assert!(
        matches!(error, ProviderError::DigestMismatch { .. }),
        "{error}"
    );
    assert!(!pid_file.exists(), "a refused binary ran");
    let binary = std::fs::read(example("scripted_connector")).expect("the binary reads");
    let own = Digest(sha2::Sha256::digest(&binary).into());
    local()
        .source(&scripted().digest(own), &script)
        .await
        .expect("a binary of the digest required spawns");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_binary_changed_since_it_was_placed_is_not_spawned_again() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let binary = dir.path().join("rdlt-connector-scripted");
    std::fs::copy(example("scripted_connector"), &binary).expect("the binary copies");
    let pid_file = dir.path().join("pid");
    let kills = Kills::new();
    let source = local()
        .kills(&kills)
        .source(&scripted().path(&binary), &writing(&pid_file))
        .await
        .expect("the connector starts")
        .connector;
    let pid: i32 = std::fs::read_to_string(&pid_file)
        .expect("the connector wrote its id")
        .parse()
        .expect("a process id");
    kills.kill();
    assert!(
        wait_gone(pid, Duration::from_secs(5)).await,
        "the connector was killed"
    );
    // Replaced as an upgrade replaces it: a new file renamed over the old.
    let upgrade = dir.path().join("upgrade");
    std::fs::copy(&binary, &upgrade).expect("the binary copies");
    std::fs::OpenOptions::new()
        .append(true)
        .open(&upgrade)
        .and_then(|mut file| file.write_all(b"changed"))
        .expect("the upgrade changes");
    std::fs::rename(&upgrade, &binary).expect("the upgrade replaces the binary");
    changed(source.as_ref()).await;
    let written = std::fs::read_to_string(&pid_file).expect("the pid file reads");
    assert_eq!(
        written,
        pid.to_string(),
        "a changed binary was spawned again"
    );
}

/// Calls `check` until it fails as the connector changed, within a bound: the first calls may
/// still meet the lost connection.
async fn changed(source: &dyn Source) {
    for _ in 0..50 {
        if let Err(error) = source.check().await
            && error.code() == Some("connector_changed")
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the changed binary was never refused");
}
