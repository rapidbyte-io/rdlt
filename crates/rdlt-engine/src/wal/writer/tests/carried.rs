//! Segments still open when the chunks they began in are otherwise settled: their frames are
//! carried into the chunk being written, so they keep no settled frame of another segment.

use std::sync::Arc;

use super::super::super::frame::{Frame, Frames};
use super::super::super::memory::MemoryWal;
use super::{batch, chunks, commit, committed, drive, pipeline, send, table};

/// The segments of every batch frame the store holds, in order.
fn batches(store: &MemoryWal) -> Vec<u64> {
    store
        .stored(&pipeline())
        .iter()
        .flat_map(|(_, stored)| {
            Frames::new(&stored.bytes)
                .filter_map(|frame| match frame.expect("the frame decodes").1 {
                    Frame::Batch(batch) => Some(batch.segment.0),
                    _ => None,
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn an_open_segment_keeps_no_more_of_other_segments_back_than_itself() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |writer| async move {
        send(&writer, table(0)).await;
        // Segment 1000's partition writes a batch between every commit and never seals it;
        // the others write four a commit and are committed.
        for round in 1..=50_u64 {
            for segment in 0..4 {
                send(&writer, batch(round * 10 + segment, 0)).await;
            }
            send(&writer, batch(1000, 0)).await;
            let segments: Vec<u64> = (0..4).map(|segment| round * 10 + segment).collect();
            let (command, answer) = commit(round, &segments);
            send(&writer, command).await;
            answer.await.expect("the writer answers").expect("durable");
            send(&writer, committed(round)).await;
            // What is kept of committed segments is no more than the open segment holds, beside
            // the round being written.
            let kept = batches(&observed);
            let open = kept.iter().filter(|segment| **segment == 1000).count();
            let settled = kept.len() - open;
            assert!(
                settled <= open + 4,
                "round {round}: {settled} settled, {open} open"
            );
        }
    })
    .await
    .expect("the writer ends");
    // Every batch of the open segment is still logged, once each.
    let open = batches(&store)
        .into_iter()
        .filter(|segment| *segment == 1000)
        .count();
    assert_eq!(open, 50);
}

/// Logs a round: `settled` batches of segments `round * 10` onwards, which commit `round` takes
/// and receives, and `open` batches of segment 1000, which no commit takes.
async fn round(writer: &super::super::WalWriter, round: u64, settled: u64, open: u64) {
    for segment in 0..settled {
        send(writer, batch(round * 10 + segment, 0)).await;
    }
    for _ in 0..open {
        send(writer, batch(1000, 0)).await;
    }
    let segments: Vec<u64> = (0..settled).map(|segment| round * 10 + segment).collect();
    let (command, answer) = commit(round, &segments);
    send(writer, command).await;
    answer.await.expect("the writer answers").expect("durable");
    send(writer, committed(round)).await;
}

/// The seal of segment `segment`, of a partition its read finished.
fn sealed(segment: u64) -> super::super::Command {
    use rdlt_connector::{PartitionId, PartitionState, SegmentId, StreamName};

    use super::super::super::frame::Seal;
    let segment = SegmentId(segment);
    let seal = Frame::Seal(Seal {
        segment,
        stream: StreamName::new("orders").expect("a name"),
        partition: PartitionId::parse("p").expect("an id"),
        replayable: true,
        phase: 0,
        from: None,
        state: PartitionState::Done,
    });
    let frame = seal.encode().expect("the seal encodes");
    let held = Box::new(());
    super::super::Command::Seal {
        segment,
        frame,
        held,
    }
}

#[tokio::test]
async fn a_carried_segment_replays_once_after_the_commit_that_takes_it() {
    use rdlt_connector::SegmentId;

    use super::super::super::scan::scan;
    let store = Arc::new(MemoryWal::default());
    drive(Arc::clone(&store), |writer| async move {
        send(&writer, table(0)).await;
        for number in 1..=3 {
            round(&writer, number, 4, 1).await;
        }
        // The open segment is sealed and committed; the destination never answers.
        send(&writer, sealed(1000)).await;
        let (command, answer) = commit(4, &[1000]);
        send(&writer, command).await;
        answer.await.expect("the writer answers").expect("durable");
    })
    .await
    .expect("the writer ends");
    store.crash();
    let scanned = scan(store.as_ref(), &pipeline(), super::load())
        .await
        .expect("the log reads");
    let pending: Vec<_> = scanned.pending().collect();
    assert_eq!(pending.len(), 1);
    assert!(pending[0].meta.segments.contains(SegmentId(1000)));
    // Each of its three batches once, though each was carried.
    assert_eq!(scanned.batches[&SegmentId(1000)].len(), 3);
}

#[tokio::test]
async fn a_chunk_holding_more_of_open_segments_than_of_settled_ones_stays() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |writer| async move {
        send(&writer, table(0)).await;
        for number in 1..=3 {
            // Copying the open batches would cost more than freeing the settled one gives.
            round(&writer, number, 1, 2).await;
        }
        let (command, answer) = commit(4, &[]);
        send(&writer, command).await;
        answer.await.expect("the writer answers").expect("durable");
        let numbers: Vec<u64> = chunks(&observed)
            .into_iter()
            .map(|(number, _)| number)
            .collect();
        assert_eq!(numbers, [0, 1, 2, 3]);
    })
    .await
    .expect("the writer ends");
}

#[tokio::test]
async fn a_failure_between_carrying_and_removing_leaves_nothing_a_replay_stages_twice() {
    use rdlt_connector::SegmentId;

    use super::super::super::scan::scan;
    let store = Arc::new(MemoryWal::default());
    let failing = Arc::clone(&store);
    drive(Arc::clone(&store), |writer| async move {
        send(&writer, table(0)).await;
        round(&writer, 1, 4, 1).await;
        for segment in 20..24 {
            send(&writer, batch(segment, 0)).await;
        }
        send(&writer, batch(1000, 0)).await;
        let (command, answer) = commit(2, &[20, 21, 22, 23]);
        send(&writer, command).await;
        answer.await.expect("the writer answers").expect("durable");
        // The open segment's frames are carried at the receipt; removing the chunk they came
        // from fails, as a crash between the two leaves the log.
        *failing.unremovable.lock() = true;
        send(&writer, committed(2)).await;
        let (command, answer) = commit(3, &[]);
        send(&writer, command).await;
        assert!(answer.await.expect("the writer answers").is_err());
    })
    .await
    .expect("the writer ends");
    // Before a crash and after it, whatever copies of the open segment's frames are there, no
    // commit a replay makes takes it.
    let before = scan(store.as_ref(), &pipeline(), super::load())
        .await
        .expect("the log reads");
    assert_eq!(before.batches[&SegmentId(1000)].len(), 4);
    store.crash();
    let after = scan(store.as_ref(), &pipeline(), super::load())
        .await
        .expect("the log reads");
    for scanned in [before, after] {
        assert!(
            scanned
                .pending()
                .all(|logged| !logged.meta.segments.contains(SegmentId(1000)))
        );
    }
}

#[tokio::test]
async fn a_carried_segment_replays_its_batches_in_the_order_they_were_logged() {
    use rdlt_connector::SegmentId;

    use super::super::super::scan::scan;
    let store = Arc::new(MemoryWal::default());
    drive(Arc::clone(&store), |writer| async move {
        send(&writer, table(0)).await;
        send(&writer, batch(1000, 0)).await;
        for segment in 10..14 {
            send(&writer, batch(segment, 0)).await;
        }
        let (command, answer) = commit(1, &[10, 11, 12, 13]);
        send(&writer, command).await;
        answer.await.expect("the writer answers").expect("durable");
        // The open segment logs a batch in the next chunk before the receipt carries its first
        // one there after it.
        send(&writer, batch(1000, 0)).await;
        send(&writer, committed(1)).await;
        send(&writer, sealed(1000)).await;
        let (command, answer) = commit(2, &[1000]);
        send(&writer, command).await;
        answer.await.expect("the writer answers").expect("durable");
    })
    .await
    .expect("the writer ends");
    // In the log the carried batch follows the later one.
    let logged: Vec<u64> = store
        .stored(&pipeline())
        .iter()
        .flat_map(|(_, stored)| {
            Frames::new(&stored.bytes)
                .filter_map(|frame| match frame.expect("the frame decodes").1 {
                    Frame::Batch(batch) if batch.segment.0 == 1000 => Some(batch.ordinal),
                    _ => None,
                })
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(logged.len(), 2);
    assert!(logged[0] > logged[1], "{logged:?}");
    store.crash();
    let scanned = scan(store.as_ref(), &pipeline(), super::load())
        .await
        .expect("the log reads");
    let replayed: Vec<u64> = scanned.batches[&SegmentId(1000)]
        .iter()
        .map(|located| located.ordinal)
        .collect();
    assert_eq!(replayed, [logged[1], logged[0]]);
}
