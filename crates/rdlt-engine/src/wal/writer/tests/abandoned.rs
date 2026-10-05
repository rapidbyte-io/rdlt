//! Segments a partition abandons, and what the writer forgets as its chunks go.

use std::sync::Arc;

use rdlt_connector::SegmentId;

use super::super::super::memory::MemoryWal;
use super::super::{Command, Log, Shared};
use super::{drive, numbers, owner, table};

#[tokio::test]
async fn a_segment_its_partition_abandons_holds_no_chunk_back() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        log.batch(1, 0).await;
        // Segment 2's partition stopped before sealing it: no commit ever takes it.
        log.batch(2, 0).await;
        log.send(Command::Abandon {
            segment: SegmentId(2),
        })
        .await;
        log.commit(1, &[1]).await.expect("durable");
        log.committed(1).await;
        log.batch(3, 0).await;
        log.commit(2, &[3]).await.expect("durable");
        assert_eq!(
            numbers(&observed),
            [1],
            "chunk 0 holds only settled segments"
        );
    })
    .await
    .expect("the writer ends");
}

#[test]
fn settled_segments_are_forgotten_once_no_chunk_holds_them() {
    let shared = Arc::new(Shared::new(u64::MAX));
    let mut log = Log::new(Arc::new(MemoryWal::default()), owner(), shared);
    // Segment 1 has frames in chunk 0 alone, segment 2 in chunks 0 and 1, segment 3 in none.
    log.holds(SegmentId(1));
    log.holds(SegmentId(2));
    log.chunk = 1;
    log.holds(SegmentId(2));
    log.settle([SegmentId(1), SegmentId(2), SegmentId(3)]);
    let settled = |log: &Log| [1, 2, 3].map(|id| log.settled.contains(SegmentId(id)));
    assert_eq!(
        settled(&log),
        [true, true, false],
        "one no chunk holds is not kept"
    );
    assert!(log.forgotten(0).is_some());
    assert_eq!(settled(&log), [false, true, false]);
    assert!(log.forgotten(1).is_some());
    assert_eq!(settled(&log), [false, false, false]);
}
