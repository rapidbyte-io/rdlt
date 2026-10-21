//! Connectors spawned through `Local` as real processes, beyond what a simulated seed runs: starts
//! racing on real threads for the last of the groups a process may own.
//!
//! These run real processes on the real clock, so a failure names its round and does not replay.

use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::time::Duration;

use rdlt_connector::ConnectorId;
use rdlt_host::{ConnectorRef, Local, ProviderError, Wire};
use tokio::task::JoinSet;

use crate::process::local;

/// The groups a process may own at once, those still starting among them (ADR 0050).
const OWNED: usize = 1024;

/// How many starts race for the last group in each round.
const RACERS: usize = 32;

/// How many rounds of racing starts the test runs.
const ROUNDS: usize = 10;

/// How long a stopped connector's group may take to end.
const ENDING: Duration = Duration::from_secs(20);

/// A connector that serves nothing and ends at its stop: a shell that becomes `sleep`.
fn sleeper(directory: &Path) -> ConnectorRef {
    let path = directory.join("rdlt-connector-sleeper");
    std::fs::write(&path, "#!/bin/sh\nexec sleep 600\n").expect("the script writes");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))
        .expect("the script is executable");
    ConnectorRef::new(ConnectorId::parse("test.sleeper").expect("a valid id")).path(path)
}

/// Raises this process's limit on open descriptors to its hard limit: each connector holds four.
fn descriptors_raised() {
    use nix::sys::resource::{Resource, getrlimit, setrlimit};
    let (_, hard) = getrlimit(Resource::RLIMIT_NOFILE).expect("the limit reads");
    setrlimit(Resource::RLIMIT_NOFILE, hard, hard).expect("the limit rises");
}

/// Starts `count` connectors of `reference`, sixty-four at a time.
async fn started(local: &Local, reference: &ConnectorRef, count: usize) -> Vec<Wire> {
    let mut wires = Vec::with_capacity(count);
    while wires.len() < count {
        let mut starting = JoinSet::new();
        for _ in 0..(count - wires.len()).min(64) {
            let (local, reference) = (local.clone(), reference.clone());
            starting.spawn(async move { local.wire(&reference).await });
        }
        while let Some(joined) = starting.join_next().await {
            wires.push(joined.expect("a start ends").expect("a connector starts"));
        }
    }
    wires
}

/// Waits, for [`ENDING`] at most, until this process owns `count` groups.
async fn owning(count: usize) -> usize {
    let until = tokio::time::Instant::now() + ENDING;
    while rdlt_host::spawned().len() != count && tokio::time::Instant::now() < until {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    rdlt_host::spawned().len()
}

// The real clock: the race is between real threads, and the groups end as real processes do.
#[test]
#[ignore = "starts a thousand processes; run it with `just soak`"]
fn of_starts_racing_for_the_last_group_of_a_process_one_alone_starts() {
    descriptors_raised();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("a runtime");
    let directory = tempfile::tempdir().expect("a temporary directory");
    let (local, sleeper) = (local(), sleeper(directory.path()));
    runtime.block_on(async {
        let held = started(&local, &sleeper, OWNED - 1).await;
        for round in 0..ROUNDS {
            let mut racing = JoinSet::new();
            for _ in 0..RACERS {
                let (local, sleeper) = (local.clone(), sleeper.clone());
                racing.spawn(async move { local.wire(&sleeper).await });
            }
            let (mut won, mut refused) = (Vec::new(), 0);
            while let Some(joined) = racing.join_next().await {
                match joined.expect("a start ends") {
                    Ok(wire) => won.push(wire),
                    Err(ProviderError::SpawnFailed { .. }) => refused += 1,
                    Err(other) => panic!("round {round}: a start failed otherwise: {other}"),
                }
            }
            assert_eq!(
                won.len(),
                1,
                "round {round}: {} of {RACERS} starts started and {refused} were refused",
                won.len()
            );
            drop(won);
            // The winner's group ends before the next round races for its place.
            assert_eq!(owning(OWNED - 1).await, OWNED - 1, "round {round}");
        }
        drop(held);
    });
    assert_eq!(rdlt_host::stop_spawned(ENDING), Ok(()));
}
