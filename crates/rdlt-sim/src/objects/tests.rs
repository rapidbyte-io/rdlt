use std::sync::Arc;

use rdlt_connector::{LoadId, PipelineId};
use rdlt_engine::{Chunk, WalStore};
use rdlt_testkit::objects::{Call, Fault, Op};

use super::{ObjectLogs, fault};
use crate::rng::SplitMix64;

#[test]
fn no_listing_misses_an_object_and_puts_and_deletions_land_late() {
    let mut rng = SplitMix64::new(7);
    let call = |op: Op, key: &str| Call {
        op,
        key: key.to_owned(),
    };
    for listing in [
        call(Op::List, "logs/p.orders/open"),
        call(Op::List, "logs/p.orders/logs/0190"),
    ] {
        let drawn: Vec<Fault> = (0..400).map(|_| fault(&listing, &mut rng)).collect();
        assert!(!drawn.contains(&Fault::Stale), "{listing:?}");
    }
    let creates = call(Op::Put { create: true }, "logs/p.orders/open/x");
    let deletes = call(Op::Delete, "logs/p.orders/open/x");
    for (asked, expected) in [
        (
            &creates,
            vec![Fault::Fail, Fault::Hang, Fault::Raced, Fault::Answerless],
        ),
        (&deletes, vec![Fault::Fail, Fault::Hang, Fault::Answerless]),
    ] {
        let drawn: Vec<Fault> = (0..400).map(|_| fault(asked, &mut rng)).collect();
        for fault in expected {
            assert!(drawn.contains(&fault), "{fault:?}");
        }
        assert!(drawn.iter().any(|fault| matches!(fault, Fault::Late(_))));
    }
}

#[tokio::test(start_paused = true)]
async fn logs_in_the_object_store_are_held_until_removed() {
    let logs = ObjectLogs::open(SplitMix64::new(3)).await;
    assert!(!logs.holds_logs().await, "the probe leaves nothing");
    let wal: Arc<dyn WalStore> = Arc::clone(&logs.wal) as _;
    let pipeline = PipelineId::parse("held").expect("a valid pipeline");
    let load = LoadId::from_parts(std::time::UNIX_EPOCH, 1);
    wal.open_log(&pipeline, load).await.expect("opens");
    assert!(logs.holds_logs().await);
    let mut staged = wal
        .stage(&pipeline, Chunk { load, number: 0 })
        .await
        .expect("stages");
    staged
        .append(bytes::Bytes::from_static(b"chunk"))
        .await
        .expect("appends");
    staged.publish().await.expect("publishes");
    wal.remove_log(&pipeline, load).await.expect("removes");
    assert!(!logs.holds_logs().await);
}
