//! A table's lock is waited for a bounded time, and checks of one root never fail each other.

use std::time::{Duration, Instant};

use rdlt_connector::{ConnectorErrorKind, Field, LogicalType, TableChange};
use serde_json::json;

use crate::fixtures::{connect, connect_with, ids, open, stage, table};

#[tokio::test(flavor = "multi_thread")]
async fn a_lock_another_holds_is_waited_for_a_bounded_time() {
    let root = crate::fixtures::tempdir().unwrap();
    let (destination, _) = connect_with(root.path(), json!({ "lock_wait_ms": 300 })).await;
    let mut opened = open(destination.as_ref(), 1).await;
    let (schema, batch) = ids(&[1]);
    let orders = table("orders");
    stage(&mut opened, &orders, &schema, batch, 1).await;
    let path = root.path().join("_rdlt").join("locks").join("orders");
    let holder = std::fs::File::open(&path).expect("the lock file opens");
    holder.lock().expect("the lock is free");
    let add = TableChange::AddColumn {
        table: orders.clone(),
        field: Field::new("more", LogicalType::Int64, true),
    };
    let bound = Duration::from_secs(20);
    let started = Instant::now();
    let changed = tokio::time::timeout(bound, opened.session.apply_schema(&add))
        .await
        .expect("the schema change ends");
    let error = changed.expect_err("the lock is held");
    assert_eq!(error.kind(), ConnectorErrorKind::Transient);
    assert_eq!(error.code(), Some("lock_timeout"));
    let writer = tokio::time::timeout(bound, opened.session.writer(&orders))
        .await
        .expect("the writer's claim ends");
    let Err(error) = writer else {
        panic!("a writer opened under another's lock");
    };
    assert_eq!(error.code(), Some("lock_timeout"));
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_millis(600) && waited < Duration::from_secs(10),
        "{waited:?}"
    );
    drop(holder);
    opened
        .session
        .apply_schema(&add)
        .await
        .expect("the lock is free again");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_checks_never_fail_each_other() {
    let root = crate::fixtures::tempdir().unwrap();
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
