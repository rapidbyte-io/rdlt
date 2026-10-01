//! Kills: a spawned connector killed outright, or a connection cut, is started or reached again
//! for the next call.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use rdlt_connector::serve::Served;
use rdlt_connector::{ConnectorId, Source, source_factory};
use rdlt_connector_reference::MemorySource;
use rdlt_host::{Connect, ConnectorRef, Kills, Placement, Provider as _};

use crate::process::{local, scripted, wait_gone};
use crate::support::served;

/// Calls `check` until it succeeds, as an engine's retries would, within a bound.
async fn checks_again(source: &dyn Source) {
    for _ in 0..50 {
        if source.check().await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the connector was never started again");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_killed_spawned_connector_is_spawned_again_for_the_next_call() {
    let kills = Kills::new();
    let dir = tempfile::tempdir().expect("a temporary directory");
    let pid_file = dir.path().join("pid");
    let script = serde_json::json!({ "pid_file": pid_file });
    let source = local()
        .kills(&kills)
        .source(&scripted(), &script)
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
    checks_again(source.as_ref()).await;
    let respawned: i32 = std::fs::read_to_string(&pid_file)
        .expect("the respawned connector wrote its id")
        .parse()
        .expect("a process id");
    assert_ne!(respawned, pid);
    assert_eq!(kills.count(), 1);
}

#[tokio::test]
async fn a_connector_reached_by_a_function_is_reached_again_once_its_stream_is_cut() {
    let kills = Kills::new();
    let opened = Arc::new(AtomicUsize::new(0));
    let connect = Connect::new({
        let opened = Arc::clone(&opened);
        move || {
            opened.fetch_add(1, Ordering::SeqCst);
            let stream = served(Served::new().with_source(source_factory::<MemorySource>()));
            Box::pin(async move { Ok(Box::new(stream) as Box<dyn rdlt_host::Stream>) })
        }
    })
    .kills(&kills);
    assert!(format!("{connect:?}").contains("heartbeat"), "{connect:?}");
    let reference = ConnectorRef::new(ConnectorId::parse("io.rapidbyte.memory").expect("valid"));
    let config = serde_json::json!({ "streams": { "items": [] } });
    let placed = connect
        .source(&reference, &config)
        .await
        .expect("the connector is reached");
    assert_eq!(placed.placement, Placement::Connected);
    placed
        .connector
        .check()
        .await
        .expect("the connector checks");
    kills.kill();
    checks_again(placed.connector.as_ref()).await;
    assert_eq!(opened.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn a_stream_that_never_opens_is_unreachable_within_the_connect_deadline() {
    let deadline = Duration::from_millis(50);
    let options = rdlt_host::Options {
        deadlines: rdlt_host::Deadlines {
            connect: deadline,
            ..rdlt_host::Deadlines::default()
        },
        ..rdlt_host::Options::default()
    };
    let connect = Connect::new(|| Box::pin(std::future::pending())).options(options);
    let reference = ConnectorRef::new(ConnectorId::parse("io.rapidbyte.memory").expect("valid"));
    let started = tokio::time::Instant::now();
    let unreachable = connect
        .source(&reference, &serde_json::json!({}))
        .await
        .err()
        .expect("the connector is never reached");
    assert!(
        matches!(unreachable, rdlt_host::ProviderError::Unreachable { .. }),
        "{unreachable}"
    );
    assert!(started.elapsed() < deadline * 2, "{:?}", started.elapsed());
}

/// A launcher at `directory`'s `launcher` that starts a sleeper, writes its process id to
/// `sleeper`, and becomes the scripted connector; `detached` starts the sleeper in a session of
/// its own, out of the launcher's process group.
fn launcher(directory: &std::path::Path, detached: bool) -> ConnectorRef {
    let path = directory.join("launcher");
    let sleeper = directory.join("sleeper");
    let connector = crate::process::example("scripted_connector");
    let session = if detached { "setsid " } else { "" };
    let script = format!(
        "#!/bin/sh\n{session}sleep 1000 &\necho $! > '{}'\nexec '{}' \"$@\"\n",
        sleeper.display(),
        connector.display()
    );
    std::fs::write(&path, script).expect("the launcher writes");
    std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o755))
        .expect("the launcher is executable");
    let id = ConnectorId::parse("test.scripted").expect("a valid id");
    ConnectorRef::new(id).path(path)
}

/// The process id the launcher at `directory` wrote for its sleeper.
fn sleeper(directory: &std::path::Path) -> i32 {
    let written = std::fs::read_to_string(directory.join("sleeper")).expect("the id was written");
    written.trim().parse().expect("a process id")
}

/// Waits for `kills` to have landed `landed` times, within a bound; whether they did.
async fn lands(kills: &Kills, landed: u64) -> bool {
    for _ in 0..250 {
        if kills.landed() >= landed {
            return kills.landed() == landed;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread")]
async fn a_kill_reaches_every_process_of_the_connectors_group_and_lands() {
    let kills = Kills::new();
    let directory = tempfile::tempdir().expect("a temporary directory");
    let launched = launcher(directory.path(), false);
    let source = local()
        .kills(&kills)
        .source(&launched, &serde_json::json!({}))
        .await
        .expect("the connector starts")
        .connector;
    let started = sleeper(directory.path());
    assert_eq!(kills.landed(), 0);
    kills.kill();
    assert!(
        wait_gone(started, Duration::from_secs(5)).await,
        "what the connector started was killed with it"
    );
    assert!(lands(&kills, 1).await, "{} landed", kills.landed());
    // Started again, it is killed again, and what it started with it.
    checks_again(source.as_ref()).await;
    let again = sleeper(directory.path());
    assert_ne!(again, started);
    kills.kill();
    assert!(wait_gone(again, Duration::from_secs(5)).await);
    assert!(lands(&kills, 2).await, "{} landed", kills.landed());
    assert_eq!(kills.count(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stopped_connector_takes_what_it_started_with_it() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let launched = launcher(directory.path(), false);
    let source = local()
        .grace(Duration::from_millis(200))
        .source(&launched, &serde_json::json!({}))
        .await
        .expect("the connector starts")
        .connector;
    let started = sleeper(directory.path());
    drop(source);
    assert!(
        wait_gone(started, Duration::from_secs(5)).await,
        "what the connector started stopped with it"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn a_kill_that_leaves_a_holder_of_the_connection_alive_has_not_landed() {
    let kills = Kills::new();
    let directory = tempfile::tempdir().expect("a temporary directory");
    let launched = launcher(directory.path(), true);
    let source = local()
        .kills(&kills)
        .source(&launched, &serde_json::json!({}))
        .await
        .expect("the connector starts")
        .connector;
    // In a session of its own, the sleeper holds the connector's end of the connection, and
    // outlives the kill of the connector's group.
    let holder = sleeper(directory.path());
    kills.kill();
    assert!(!lands(&kills, 1).await, "{} landed", kills.landed());
    assert_eq!((kills.count(), kills.landed()), (1, 0));
    assert!(!wait_gone(holder, Duration::from_millis(100)).await);
    // Once the last holder ends, the connection does, and the kill has landed.
    let holder = nix::unistd::Pid::from_raw(holder);
    nix::sys::signal::kill(holder, nix::sys::signal::Signal::SIGKILL).expect("it is signalled");
    assert!(lands(&kills, 1).await, "{} landed", kills.landed());
    drop(source);
}

#[tokio::test]
async fn a_cut_connection_lands_its_kill_once_and_a_kill_of_nothing_lands_nowhere() {
    let kills = Kills::new();
    kills.kill();
    assert_eq!((kills.count(), kills.landed()), (1, 0));
    let connect = Connect::new(|| {
        let stream = served(Served::new().with_source(source_factory::<MemorySource>()));
        Box::pin(async move { Ok(Box::new(stream) as Box<dyn rdlt_host::Stream>) })
    })
    .kills(&kills);
    let reference = ConnectorRef::new(ConnectorId::parse("io.rapidbyte.memory").expect("valid"));
    let config = serde_json::json!({ "streams": { "items": [] } });
    let placed = connect
        .source(&reference, &config)
        .await
        .expect("the connector is reached");
    assert_eq!(kills.landed(), 0);
    kills.kill();
    assert!(lands(&kills, 1).await, "{} landed", kills.landed());
    checks_again(placed.connector.as_ref()).await;
    // The connection that ended is counted once, however often it is asked of after.
    assert!(!lands(&kills, 2).await, "{} landed", kills.landed());
}
