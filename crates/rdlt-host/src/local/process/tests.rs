use std::os::fd::OwnedFd;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use nix::errno::Errno;
use nix::sys::wait::{WaitPidFlag, waitpid};
use rdlt_connector::ConnectorId;

use super::{Confinement, Launch, Process, Steps, Unspawned};
use crate::local::binary::Binary;
use crate::local::grants::{Claim, Guarded, Lease, Leases, Roots};
use crate::local::sandbox::{Confined, Grants, Launcher, NetworkGrant, Sandbox, SandboxError};
use crate::secrets::Redactions;

/// What `leases` hold for a placement granted to write `written`.
fn writing(leases: &Leases, written: &Path) -> Result<Lease, SandboxError> {
    let grants = Grants {
        write: vec![written.to_owned()],
        ..Grants::default()
    };
    let roots = Roots {
        read: Vec::new(),
        write: vec![written.to_owned()],
    };
    leases.take(&Claim {
        grants: &grants,
        shared_reads: &[],
        roots: &roots,
        guarded: &Guarded::default(),
        programs: &[],
    })
}

/// A script that serves nothing and ends only when it is made to.
fn sleeper(directory: &Path) -> Launch {
    script(directory, "exec sleep 300")
}

/// A script of `body` that serves nothing.
fn script(directory: &Path, body: &str) -> Launch {
    let path = directory.join("sleeper");
    std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).expect("the script writes");
    std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o755))
        .expect("the script is executable");
    Launch {
        id: ConnectorId::parse("test.sleeper").expect("a valid id"),
        binary: Arc::new(Binary::at(&path).expect("the script opens")),
        digest: None,
        env_passthrough: Vec::new(),
        grace: Duration::from_millis(100),
        kills: None,
        told: None,
        confinement: None,
        lease: Arc::new(writing(&Leases::default(), directory).expect("held")),
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

/// A sandbox no test may ask for a launcher.
#[derive(Debug)]
struct Unasked;

impl Sandbox for Unasked {
    fn launcher(&self, _confined: &Confined<'_>) -> Result<Launcher, SandboxError> {
        panic!("a launcher was asked for");
    }
}

#[test]
fn a_sandboxed_connector_is_not_spawned_where_descriptors_cannot_be_marked_at_once() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let mut launch = sleeper(directory.path());
    launch.confinement = Some(Confinement {
        sandbox: Arc::new(Unasked),
        network: NetworkGrant::Denied,
    });
    let unmarked = Steps {
        marks_at_once: || false,
        ..Steps::TAKEN
    };
    let refused = Process::spawn_by(&launch, socket(), Redactions::new(), &unmarked);
    let Err(Unspawned::Sandbox(refused)) = refused else {
        panic!("it was not refused");
    };
    assert_eq!(refused, SandboxError::Descriptors);
    assert_eq!(refused.code(), "sandbox_descriptors");
}

#[tokio::test(flavor = "multi_thread")]
async fn what_a_placement_holds_is_held_until_its_last_connector_is_reaped() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let granted = directory.path().join("granted");
    std::fs::create_dir(&granted).expect("a directory");
    let leases = Leases::default();
    // A connector that ignores its stop once it says so, and has a grace to linger through.
    let ignoring = directory.path().join("ignoring");
    let body = format!(
        "trap '' TERM\ntouch '{}'\nexec sleep 300",
        ignoring.display()
    );
    let mut launch = script(directory.path(), &body);
    launch.grace = Duration::from_secs(3);
    launch.lease = Arc::new(writing(&leases, &granted).expect("held"));
    let process = Process::spawn_by(&launch, socket(), Redactions::new(), &Steps::TAKEN);
    let process = process.expect("it starts");
    for _ in 0..1000 {
        if ignoring.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        ignoring.exists(),
        "the connector never came to ignore its stop"
    );
    drop(launch);
    drop(process);
    // Its placement and its handle are gone, and it still runs: what it was granted is held.
    let refused = writing(&leases, &granted).expect_err("held still");
    assert_eq!(refused.code(), "grant_overlap");
    let stopping = tokio::task::spawn_blocking(|| super::stop_spawned(Duration::from_secs(20)));
    assert_eq!(stopping.await.expect("it returns"), Ok(()));
    drop(writing(&leases, &granted).expect("reaped, it is free"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_panic_while_a_connector_is_launched_is_raised_where_it_was_asked_for() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let mut launch = sleeper(directory.path());
    launch.told = Some(super::Told::new(|_| panic!("the host's own code panics")));
    let launching = tokio::spawn(async move {
        Process::launching(launch, Redactions::new())
            .await
            .map(drop)
            .map_err(drop)
    });
    let joined = launching.await.expect_err("it panics, not fails");
    assert!(joined.is_panic());
    // What it spawned before the panic is stopped with every connector.
    let stopping = tokio::task::spawn_blocking(|| super::stop_spawned(Duration::from_secs(20)));
    assert_eq!(stopping.await.expect("it returns"), Ok(()));
}
