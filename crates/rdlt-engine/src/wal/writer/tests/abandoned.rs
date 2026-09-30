//! Segments a partition abandons, and what the writer forgets as its chunks go.

use std::collections::BTreeMap;
use std::sync::Arc;

use rdlt_connector::SegmentId;

use super::super::super::memory::MemoryWal;
use super::super::{Command, Settled, Written};
use super::{batch, chunks, commit, committed, drive, send, table};

#[tokio::test]
async fn a_segment_its_partition_abandons_holds_no_chunk_back() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |writer| async move {
        send(&writer, table(0)).await;
        send(&writer, batch(1, 0)).await;
        // Segment 2's partition stopped before sealing it: no commit ever takes it.
        send(&writer, batch(2, 0)).await;
        send(
            &writer,
            Command::Abandon {
                segment: SegmentId(2),
            },
        )
        .await;
        let (command, answer) = commit(1, &[1]);
        send(&writer, command).await;
        answer.await.expect("the writer answers").expect("durable");
        send(&writer, committed(1)).await;
        send(&writer, batch(3, 0)).await;
        let (command, answer) = commit(2, &[3]);
        send(&writer, command).await;
        answer.await.expect("the writer answers").expect("durable");
        let numbers: Vec<u64> = chunks(&observed)
            .into_iter()
            .map(|(number, _)| number)
            .collect();
        assert_eq!(numbers, [1], "chunk 0 holds only settled segments");
    })
    .await
    .expect("the writer ends");
}

#[test]
fn settled_segments_are_forgotten_once_no_chunk_holds_them() {
    let mut settled = Settled::default();
    settled.settle([SegmentId(1), SegmentId(2), SegmentId(3)]);
    let mut written = BTreeMap::new();
    let mut chunk = Written::default();
    chunk.segments.insert(SegmentId(2));
    written.insert(4, chunk);
    settled.forget_unwritten(&written);
    let kept: Vec<bool> = [1, 2, 3]
        .map(|id| settled.contains(SegmentId(id)))
        .into_iter()
        .collect();
    assert_eq!(kept, [false, true, false]);
}
