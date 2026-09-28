//! Kills: a spawned connector killed outright is started again for the next call.

use std::time::Duration;

use rdlt_connector::Source;
use rdlt_host::{Kills, Provider as _};

use crate::process::{local, scripted, wait_gone};

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
