//! The old defects the public surface must never bring back, each a test of public behaviour.

use std::num::NonZeroUsize;

use rdlt::{Commit, Connector, Pipeline, Side, Stream};

use crate::support::{memory_host, within};

/// A subscriber that falls behind is told how many events it missed, and reads on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_subscriber_sees_lag() {
    let rows: Vec<serde_json::Value> = (0..40).map(|id| serde_json::json!({ "id": id })).collect();
    let source = Side::new(Connector::new("io.rapidbyte.memory"))
        .config(serde_json::json!({ "streams": { "users": rows }, "page_size": 5 }));
    let destination = Side::new(Connector::new("io.rapidbyte.memory"))
        .config(serde_json::json!({ "store": "ledger-lag" }));
    let pipeline = Pipeline::builder("lagging", source, destination)
        .stream(Stream::new("users"))
        .commit(Commit::new().rows(5))
        .build(&memory_host())
        .await
        .expect("built")
        .with_event_buffer(NonZeroUsize::new(2).unwrap());
    let run = pipeline.start();
    let mut slow = run.events();
    let outcome = within(run).await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    let lagged = within(slow.next()).await.expect("an item");
    let lagged = lagged.expect_err("the slow subscriber missed events");
    assert!(lagged.dropped > 0);
    let next = within(slow.next()).await.expect("an item");
    let next = next.expect("an event after the lag");
    assert_eq!(
        next.seq,
        lagged.dropped + 1,
        "it reads on from the first event kept"
    );
    let mut last = next.seq;
    while let Some(item) = within(slow.next()).await {
        let event = item.expect("no further lag");
        assert_eq!(event.seq, last + 1);
        last = event.seq;
    }
}
