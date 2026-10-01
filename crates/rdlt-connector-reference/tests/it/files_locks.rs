//! Checks of one root never fail each other.

#![cfg(unix)]

use crate::fixtures::connect;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_checks_never_fail_each_other() {
    let root = tempfile::tempdir().unwrap();
    let destination = connect(root.path(), "jsonl").await;
    for _ in 0..50 {
        let checks = (0..16).map(|_| {
            let destination = std::sync::Arc::clone(&destination);
            tokio::spawn(async move { destination.check().await })
        });
        for check in checks.collect::<Vec<_>>() {
            check.await.unwrap().expect("the root is writable");
        }
    }
    // No probe is left behind.
    let left = crate::fixtures::files_under(root.path());
    assert!(left.is_empty(), "{left:?}");
}
