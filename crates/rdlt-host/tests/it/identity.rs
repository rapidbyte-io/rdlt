//! A connector is checked to be the connector placed before it sees its configuration: its
//! handshake's id and version, and its binary's digest, when placed and whenever it is spawned
//! again.

use rdlt_connector::ConnectorId;
use rdlt_host::{ConnectorRef, Provider as _, ProviderError};

use crate::process::{example, local, scripted};

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
