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

/// What holds where an open file is executed: the digest a reference requires is compared, and
/// the file that was hashed is the file that runs, at placement and at every respawn.
#[cfg(target_os = "linux")]
mod executed {
    use std::io::Write as _;
    use std::time::Duration;

    use rdlt_connector::Source;
    use rdlt_host::{Digest, Kills};
    use sha2::Digest as _;

    use super::*;
    use crate::process::wait_gone;

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

    /// The process id the connector last wrote to `pid_file`.
    fn written(pid_file: &std::path::Path) -> i32 {
        std::fs::read_to_string(pid_file)
            .expect("the connector wrote its id")
            .parse()
            .expect("a process id")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_binary_whose_bytes_changed_since_it_was_placed_is_not_spawned_again() {
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
        let pid = written(&pid_file);
        kills.kill();
        assert!(
            wait_gone(pid, Duration::from_secs(5)).await,
            "the connector was killed"
        );
        // Changed where it lies: the file that was placed now holds other bytes. The kernel lets
        // go of a file a process ran a moment after the process is gone, and refuses a writer
        // until then.
        let mut changing = std::fs::OpenOptions::new().append(true).open(&binary);
        for _ in 0..500 {
            let busy =
                |error: &std::io::Error| error.kind() == std::io::ErrorKind::ExecutableFileBusy;
            if !changing.as_ref().is_err_and(busy) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            changing = std::fs::OpenOptions::new().append(true).open(&binary);
        }
        changing
            .and_then(|mut file| file.write_all(b"changed"))
            .expect("the binary changes");
        changed(source.as_ref()).await;
        assert_eq!(
            written(&pid_file),
            pid,
            "a changed binary was spawned again"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_connector_is_spawned_again_from_the_file_it_was_placed_from_whatever_took_its_name()
    {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let binary = dir.path().join("rdlt-connector-scripted");
        std::fs::copy(example("scripted_connector"), &binary).expect("the binary copies");
        let (pid_file, marker) = (dir.path().join("pid"), dir.path().join("impostor-ran"));
        let kills = Kills::new();
        let placed = local()
            .kills(&kills)
            .source(&scripted().path(&binary), &writing(&pid_file))
            .await
            .expect("the connector starts");
        let pid = written(&pid_file);
        // Another program takes the name, as a rename over it does: it says so if it ever runs.
        let impostor = dir.path().join("impostor");
        let script = format!("#!/bin/sh\ntouch '{}'\n", marker.display());
        std::fs::write(&impostor, script).expect("the impostor writes");
        std::fs::set_permissions(
            &impostor,
            std::os::unix::fs::PermissionsExt::from_mode(0o755),
        )
        .expect("the impostor is executable");
        std::fs::rename(&impostor, &binary).expect("the impostor takes the name");
        kills.kill();
        assert!(wait_gone(pid, Duration::from_secs(5)).await);
        // Spawned again, the connector is the binary that was hashed, and answers as it did.
        let mut again = pid;
        for _ in 0..100 {
            if placed.connector.check().await.is_ok() {
                again = written(&pid_file);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_ne!(again, pid, "the connector was not spawned again");
        assert!(!marker.exists(), "what took the binary's name was run");
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
}
