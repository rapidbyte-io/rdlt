//! Segments still open when the chunks they began in are otherwise settled: a relief carries
//! their frames into the chunk it publishes, so they keep no settled frame of another segment.

use std::sync::Arc;

use rdlt_connector::SegmentId;
use tokio::sync::oneshot;

use super::super::super::frame::Frame;
use super::super::super::memory::MemoryWal;
use super::super::super::scan::scan;
use super::super::Command;
use super::relieved::relieve;
use super::{Driving, batch_frame, drive, frames, length, load, numbers, pipeline, table};

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
async fn a_relief_leaves_an_open_segment_no_settled_frame_of_another_beside_it() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        // Segment 1000's partition writes a batch between every commit and never seals it;
        // the others write four a commit and are committed. A relief follows each round.
        for number in 1..=50_u64 {
            round(&mut log, number, 4, 1).await;
            relieve(&log).await;
            let kept = batches(&observed);
            let open = kept.iter().filter(|(segment, _)| *segment == 1000).count();
            let settled = kept.len() - open;
            assert_eq!(
                (settled, u64::try_from(open).expect("few")),
                (0, number),
                "round {number}"
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
        relieve(&log).await;
        assert!(log.tally.counters().log.carried > 0, "the relief carried");
        // The open segment is sealed and committed; the destination never answers.
        log.commit(4, &[1000]).await.expect("durable");
    })
    .await
    .expect("the writer ends");
    let scanned = scan(store.as_ref(), &pipeline(), load(), FRAME_BYTES)
        .await
        .expect("the log reads");
    let pending: Vec<_> = scanned.commits.iter().collect();
    assert_eq!(pending.len(), 1);
    assert!(pending[0].meta.segments.contains(SegmentId(1000)));
    // Each of its three batches once, though each was carried.
    assert_eq!(scanned.batches[&SegmentId(1000)].len(), 3);
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
        log.committed(2).await;
        // A relief carries the open segment's frames out of chunks 0 and 1; deleting them once
        // the chunk it carried them to is published fails, as a crash between the two leaves
        // the log.
        *failing.unremovable.lock() = true;
        let (done, answer) = oneshot::channel();
        log.send(Command::Relieve { done }).await;
        let relieved = answer.await.expect("the writer answers");
        assert!(relieved.is_err(), "the deletion failed");
        assert!(
            log.commit(3, &[]).await.is_err(),
            "the failed deletion fails what follows"
        );
    })
    .await
    .expect("the writer ends");
    assert_eq!(numbers(&store), [0, 1, 2], "chunks 0 and 1 were to go");
    // Chunk 2 holds the copies once, and its end names neither chunk they came from: a replay
    // never reads the open segment's frames twice, and no commit it makes takes them.
    let (_, relieved) = frames(&store).pop().expect("chunk 2");
    let copies = relieved
        .iter()
        .filter(|frame| matches!(frame, Frame::Batch(batch) if batch.segment.0 == 1000))
        .count();
    assert_eq!(copies, 2);
    assert!(
        matches!(relieved.last(), Some(Frame::End(end)) if end.live.is_empty()),
        "{relieved:?}"
    );
    // A replay reads the log a failed deletion leaves.
    scan(store.as_ref(), &pipeline(), load(), FRAME_BYTES)
        .await
        .expect("the log reads");
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
        // The open segment logs a batch in the next chunk before a relief carries its first
        // one there after it.
        log.batch(1000, 0).await;
        log.committed(1).await;
        relieve(&log).await;
        log.commit(2, &[1000]).await.expect("durable");
        // Only the first batch was carried: its table's schema was in the chunk already.
        let first = length(&batch_frame(1000, 0, 0, 1));
        assert_eq!(log.tally.counters().log.carried, first);
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
        // Segment 1000 stays open, and no relief comes, so every chunk stays, commit 1's among
        // them; commits go on.
        for number in 1..=4 {
            round(&mut log, number, 1, 2).await;
        }
        log.batch(50, 0).await;
        log.commit(5, &[50]).await.expect("durable");
        // Every receipt before commit 5 is in a published chunk: only commit 5 may be repeated.
        let oldest = *log.writer.shared().oldest.lock();
        assert_eq!(oldest.map(rdlt_connector::CommitSeq::get), Some(5));
    })
    .await
    .expect("the writer ends");
    assert!(
        numbers(&store).contains(&0),
        "the open segment keeps chunk 0"
    );
}

#[tokio::test]
async fn a_segment_a_commit_waiting_for_its_receipt_takes_is_not_carried() {
    let store = Arc::new(MemoryWal::default());
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        // Chunk 0 holds a batch of segment 1 beside one of segment 2, which commit 2 takes in
        // chunk 1 and waits for its receipt.
        log.batch(1, 0).await;
        log.batch(2, 0).await;
        log.commit(1, &[1]).await.expect("durable");
        log.commit(2, &[2]).await.expect("durable");
        // Commit 1's receipt settles the rest of chunk 0, but segment 2 stays where commit 2
        // took it: a relief carries nothing.
        log.committed(1).await;
        relieve(&log).await;
        log.commit(3, &[]).await.expect("durable");
        assert_eq!(log.tally.counters().log.carried, 0);
    })
    .await
    .expect("the writer ends");
    assert_eq!(numbers(&store), [0, 1, 2]);
    assert_eq!(
        batches(&store),
        [(1, 0), (2, 1)],
        "chunk 0 is kept and segment 2's batch is not copied"
    );
    let scanned = scan(store.as_ref(), &pipeline(), load(), FRAME_BYTES)
        .await
        .expect("the log reads");
    let taking = scanned
        .commits
        .iter()
        .filter(|logged| logged.meta.segments.contains(SegmentId(2)))
        .count();
    assert_eq!(taking, 1, "commit 2 replays");
    assert_eq!(scanned.batches[&SegmentId(2)].len(), 1);
}

#[tokio::test]
async fn a_carry_that_would_take_the_log_past_what_it_may_hold_is_left_undone() {
    let store = Arc::new(MemoryWal::default());
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        // Four settled batches and one open: carrying would copy the open one.
        log.batch(1000, 0).await;
        for segment in 10..14 {
            log.batch(segment, 0).await;
        }
        log.commit(1, &[10, 11, 12, 13]).await.expect("durable");
        // The log may hold no more than it holds now.
        let shared = Arc::clone(log.writer.shared());
        let held = shared.held.load(std::sync::atomic::Ordering::Relaxed);
        shared
            .limit
            .store(held, std::sync::atomic::Ordering::Relaxed);
        log.committed(1).await;
        relieve(&log).await;
        log.commit(2, &[]).await.expect("durable");
    })
    .await
    .expect("the writer ends");
    assert!(
        numbers(&store).contains(&0),
        "chunk 0 was not carried out of"
    );
    let open = batches(&store)
        .into_iter()
        .filter(|(segment, _)| *segment == 1000)
        .count();
    assert_eq!(open, 1, "the open batch was never copied");
}
