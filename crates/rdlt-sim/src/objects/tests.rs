use std::sync::Arc;

use rdlt_connector::{LoadId, PipelineId};
use rdlt_engine::{Chunk, WalStore};
use rdlt_testkit::objects::{Call, Fault, Op};

use super::{ObjectLogs, fault};
use crate::rng::SplitMix64;

#[test]
fn no_listing_of_the_open_logs_is_ever_stale() {
    let mut rng = SplitMix64::new(7);
    let marks = Call {
        op: Op::List,
        key: "logs/p.orders/open".to_owned(),
    };
    let chunks = Call {
        op: Op::List,
        key: "logs/p.orders/logs/0190".to_owned(),
    };
    let drawn: Vec<Fault> = (0..400).map(|_| fault(&marks, &mut rng)).collect();
    assert!(!drawn.contains(&Fault::Stale));
    let drawn: Vec<Fault> = (0..400).map(|_| fault(&chunks, &mut rng)).collect();
    assert!(drawn.contains(&Fault::Stale));
    let creates = Call {
        op: Op::Put { create: true },
        key: "logs/p.orders/open/x".to_owned(),
    };
    let drawn: Vec<Fault> = (0..400).map(|_| fault(&creates, &mut rng)).collect();
    for expected in [Fault::Fail, Fault::Hang, Fault::Raced, Fault::Answerless] {
        assert!(drawn.contains(&expected), "{expected:?}");
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
