use std::os::fd::OwnedFd;
use std::time::Duration;

use nix::errno::Errno;
use nix::sys::wait::{WaitPidFlag, waitpid};
use rdlt_connector::ConnectorId;

use super::{Launch, Process, Steps};
use crate::local::binary::Binary;
use crate::secrets::Redactions;

/// A script that serves nothing and ends only when it is made to.
fn sleeper(directory: &std::path::Path) -> Launch {
    let path = directory.join("sleeper");
    std::fs::write(&path, "#!/bin/sh\nexec sleep 300\n").expect("the script writes");
    std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o755))
        .expect("the script is executable");
    Launch {
        id: ConnectorId::parse("test.sleeper").expect("a valid id"),
        binary: std::sync::Arc::new(Binary::at(&path).expect("the script opens")),
        digest: None,
        env_passthrough: Vec::new(),
        grace: Duration::from_millis(100),
        kills: None,
        told: None,
        confinement: None,
    }
}

fn socket() -> OwnedFd {
    let (_host, connector) = std::os::unix::net::UnixStream::pair().expect("a socket pair");
    connector.into()
}

fn failing<T>() -> std::io::Result<T> {
    Err(std::io::Error::other("the step fails"))
}

#[tokio::test]
async fn a_connector_whose_owning_fails_at_any_step_is_killed_reaped_and_forgotten() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let launch = sleeper(directory.path());
    let failures = [
        Steps {
            stdout: |_| failing(),
            ..Steps::TAKEN
        },
        Steps {
            stderr: |_| failing(),
            ..Steps::TAKEN
        },
        Steps {
            thread: |_, _| failing(),
            ..Steps::TAKEN
        },
    ];
    for (step, steps) in failures.iter().enumerate() {
        let spawned = Process::spawn_by(&launch, socket(), Redactions::new(), steps);
        assert!(spawned.is_err(), "step {step}");
        // This process has no child left: the connector was killed, and reaped.
        let child = waitpid(None, Some(WaitPidFlag::WNOHANG));
        assert_eq!(child, Err(Errno::ECHILD), "step {step}");
        // And none is listed as one it spawned, to be waited for by whoever stops them.
        assert_eq!(super::spawned(), Vec::<u32>::new(), "step {step}");
        assert_eq!(super::stop_spawned(Duration::ZERO), Ok(()), "step {step}");
        assert_eq!(super::group::threads(), 0, "step {step}");
    }
    // With every step taken the connector is owned, and ends when it is dropped.
    let process =
        Process::spawn_by(&launch, socket(), Redactions::new(), &Steps::TAKEN).expect("it starts");
    assert_eq!(super::spawned().len(), 1);
    drop(process);
    assert_eq!(super::group::threads(), 1);
    assert_eq!(super::stop_spawned(Duration::from_secs(20)), Ok(()));
    // The thread that owned it is joined: none outlives the group it owned.
    assert_eq!(super::group::threads(), 0);
    assert_eq!(
        waitpid(None, Some(WaitPidFlag::WNOHANG)),
        Err(Errno::ECHILD)
    );
}
