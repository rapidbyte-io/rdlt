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
