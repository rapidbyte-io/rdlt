//! Segments still open when the chunks they began in are otherwise settled: their frames are
//! carried into the chunk being written, so they keep no settled frame of another segment.

use std::sync::Arc;

use rdlt_connector::SegmentId;

use super::super::super::frame::Frame;
use super::super::super::memory::MemoryWal;
use super::super::super::scan::scan;
use super::{Driving, drive, frames, load, numbers, pipeline, table};

/// The segment and ordinal of every batch frame the store holds, in order.
fn batches(store: &MemoryWal) -> Vec<(u64, u64)> {
    frames(store)
        .into_iter()
        .flat_map(|(_, frames)| frames)
        .filter_map(|frame| match frame {
            Frame::Batch(batch) => Some((batch.segment.0, batch.ordinal)),
            _ => None,
        })
        .collect()
}

/// What a scan reads frames of.
const FRAME_BYTES: u64 = 1 << 28;

#[tokio::test]
async fn an_open_segment_keeps_no_more_of_other_segments_back_than_itself() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        // Segment 1000's partition writes a batch between every commit and never seals it;
        // the others write four a commit and are committed.
        for number in 1..=50_u64 {
            round(&mut log, number, 4, 1).await;
            // What is kept of committed segments is no more than the open segment holds, beside
            // the round committed last.
            let kept = batches(&observed);
            let open = kept.iter().filter(|(segment, _)| *segment == 1000).count();
            let settled = kept.len() - open;
            assert!(
                settled <= open + 4,
                "round {number}: {settled} settled, {open} open"
            );
        }
    })
    .await
    .expect("the writer ends");
    // Every batch of the open segment is still logged, once each.
    let open = batches(&store)
        .into_iter()
        .filter(|(segment, _)| *segment == 1000)
        .count();
    assert_eq!(open, 50);
}

/// Logs a round: `settled` batches of segments `number * 10` onwards, which commit `number`
/// takes and receives, and `open` batches of segment 1000, which no commit takes.
async fn round(log: &mut Driving, number: u64, settled: u64, open: u64) {
    for segment in 0..settled {
        log.batch(number * 10 + segment, 0).await;
    }
    for _ in 0..open {
        log.batch(1000, 0).await;
    }
    let segments: Vec<u64> = (0..settled).map(|segment| number * 10 + segment).collect();
    log.commit(number, &segments).await.expect("durable");
    log.committed(number).await;
}

#[tokio::test]
async fn a_carried_segment_replays_once_after_the_commit_that_takes_it() {
    let store = Arc::new(MemoryWal::default());
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        for number in 1..=3 {
            round(&mut log, number, 4, 1).await;
        }
        // The open segment is sealed and committed; the destination never answers.
        log.commit(4, &[1000]).await.expect("durable");
    })
    .await
    .expect("the writer ends");
    let scanned = scan(store.as_ref(), &pipeline(), load(), FRAME_BYTES)
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
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        for number in 1..=3 {
            // Copying the open batches would cost more than freeing the settled one gives.
            round(&mut log, number, 1, 2).await;
        }
        log.commit(4, &[]).await.expect("durable");
        assert_eq!(numbers(&observed), [0, 1, 2, 3]);
    })
    .await
    .expect("the writer ends");
}

#[tokio::test]
async fn a_failed_deletion_after_a_carry_leaves_one_copy_a_replay_reads() {
    let store = Arc::new(MemoryWal::default());
    let failing = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        round(&mut log, 1, 4, 1).await;
        for segment in 20..24 {
            log.batch(segment, 0).await;
        }
        log.batch(1000, 0).await;
        log.commit(2, &[20, 21, 22, 23]).await.expect("durable");
        // The open segment's frames are carried at the receipt; deleting the chunk they came
        // from, once the next is published, fails, as a crash between the two leaves the log.
        log.committed(2).await;
        *failing.unremovable.lock() = true;
        assert!(log.commit(3, &[]).await.is_err());
    })
    .await
    .expect("the writer ends");
    assert_eq!(numbers(&store), [1, 2], "chunk 1 was to go");
    // The chunk the copies came from is needed no more: a replay reads the copies alone, and
    // no commit it makes takes the open segment.
    let scanned = scan(store.as_ref(), &pipeline(), load(), FRAME_BYTES)
        .await
        .expect("the log reads");
    assert_eq!(scanned.batches[&SegmentId(1000)].len(), 2);
    assert!(
        scanned
            .pending()
            .all(|logged| !logged.meta.segments.contains(SegmentId(1000)))
    );
}

#[tokio::test]
async fn a_carried_segment_replays_its_batches_in_the_order_they_were_logged() {
    let store = Arc::new(MemoryWal::default());
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        log.batch(1000, 0).await;
        for segment in 10..14 {
            log.batch(segment, 0).await;
        }
        log.commit(1, &[10, 11, 12, 13]).await.expect("durable");
        // The open segment logs a batch in the next chunk before the receipt carries its first
        // one there after it.
        log.batch(1000, 0).await;
        log.committed(1).await;
        log.commit(2, &[1000]).await.expect("durable");
    })
    .await
    .expect("the writer ends");
    // In the log the carried batch follows the later one.
    let logged: Vec<u64> = batches(&store)
        .into_iter()
        .filter(|(segment, _)| *segment == 1000)
        .map(|(_, ordinal)| ordinal)
        .collect();
    assert_eq!(logged.len(), 2);
    assert!(logged[0] > logged[1], "{logged:?}");
    let scanned = scan(store.as_ref(), &pipeline(), load(), FRAME_BYTES)
        .await
        .expect("the log reads");
    let replayed: Vec<u64> = scanned.batches[&SegmentId(1000)]
        .iter()
        .map(|located| located.ordinal)
        .collect();
    assert_eq!(replayed, [logged[1], logged[0]]);
}

#[tokio::test]
async fn a_segment_open_for_long_holds_back_no_commit_a_replay_never_repeats() {
    let store = Arc::new(MemoryWal::default());
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        // Segment 1000 stays open, and holds more of each chunk than any commit does, so every
        // chunk stays, commit 1's among them; commits go on.
        for number in 1..=4 {
            round(&mut log, number, 1, 2).await;
        }
        log.batch(50, 0).await;
        log.commit(5, &[50]).await.expect("durable");
        assert!(
            numbers(&store).contains(&0),
            "the open segment keeps chunk 0"
        );
        // Every receipt before commit 5 is in a published chunk: only commit 5 may be repeated.
        let oldest = *log.writer.shared().oldest.lock();
        assert_eq!(oldest.map(rdlt_connector::CommitSeq::get), Some(5));
    })
    .await
    .expect("the writer ends");
}
