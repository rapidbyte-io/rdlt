//! Chunks published between commits, where a batch finds the log full: they record the receipts
//! that arrived, carry what is open out of chunks holding settled frames, and let those go.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::sync::oneshot;

use super::super::super::frame::Frame;
use super::super::super::memory::MemoryWal;
use super::super::super::scan::scan;
use super::super::Command;
use super::{Driving, chunks, drive, frames, load, numbers, pipeline, table};

/// Has the writer publish chunks between commits while each frees anything.
pub(super) async fn relieve(log: &Driving) {
    let (done, answer) = oneshot::channel();
    log.send(Command::Relieve { done }).await;
    answer
        .await
        .expect("the writer answers")
        .expect("it relieves");
}

/// The segments of the batch frames chunk `number` holds.
pub(super) fn batches_of(store: &MemoryWal, number: u64) -> Vec<u64> {
    frames(store)
        .into_iter()
        .filter(|(chunk, _)| *chunk == number)
        .flat_map(|(_, frames)| frames)
        .filter_map(|frame| match frame {
            Frame::Batch(batch) => Some(batch.segment.0),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn a_chunk_published_between_commits_records_the_receipts_and_lets_go_of_what_they_settle() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        log.batch(1, 0).await;
        log.commit(1, &[1]).await.expect("durable");
        log.committed(1).await;
        log.batch(2, 0).await;
        relieve(&log).await;
        // The log holds what it published, and nothing else.
        let stored: usize = observed
            .stored(&pipeline())
            .iter()
            .map(|(_, bytes)| bytes.len())
            .sum();
        let stored = u64::try_from(stored).expect("a length");
        assert_eq!(log.writer.shared().held.load(Ordering::SeqCst), stored);
        let relieved = [
            "header 1",
            "schema 0",
            "batch 2 of 0",
            "relieved",
            "end [] []",
        ];
        assert_eq!(
            chunks(&observed),
            [(1, relieved.map(str::to_owned).to_vec())]
        );
        assert_eq!(log.tally.counters().log.reliefs, 1);
        // The commit after it takes the batch it holds, which a replay reads from it.
        log.commit(2, &[2]).await.expect("durable");
        assert_eq!(numbers(&observed), [1, 2]);
        let scanned = scan(observed.as_ref(), &pipeline(), load(), 1 << 28)
            .await
            .expect("it reads");
        assert_eq!(scanned.pending().count(), 1);
    })
    .await
    .expect("the writer ends");
}

#[tokio::test]
async fn nothing_is_published_between_a_commit_s_seals_and_the_commit() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        log.batch(1, 0).await;
        log.commit(1, &[1]).await.expect("durable");
        log.committed(1).await;
        log.batch(2, 0).await;
        log.seal(2).await;
        relieve(&log).await;
        assert_eq!(numbers(&observed), [0]);
        log.commit(2, &[]).await.expect("durable");
        assert_eq!(numbers(&observed), [1], "the commit records the receipt");
    })
    .await
    .expect("the writer ends");
}

#[tokio::test]
async fn a_relief_that_would_free_nothing_publishes_nothing() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        log.batch(1, 0).await;
        relieve(&log).await;
        assert!(observed.stored(&pipeline()).is_empty());
        log.commit(1, &[1]).await.expect("durable");
        // Commit 1 waits for its receipt: its chunk is needed. The chunk staged holds only a
        // segment its partition abandoned, and is published by no relief.
        log.batch(2, 0).await;
        log.send(Command::Abandon {
            segment: rdlt_connector::SegmentId(2),
        })
        .await;
        relieve(&log).await;
        assert_eq!(numbers(&observed), [0]);
    })
    .await
    .expect("the writer ends");
}

#[tokio::test]
async fn a_relief_carries_open_frames_out_of_the_chunks_it_has_room_to_copy() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        // Chunk 0 holds a batch of segment 1 beside five of segment 2; chunk 1 one of segment 3
        // beside three of segment 4.
        log.batch(1, 0).await;
        for _ in 0..5 {
            log.batch(2, 0).await;
        }
        log.commit(1, &[1]).await.expect("durable");
        log.batch(3, 0).await;
        for _ in 0..3 {
            log.batch(4, 0).await;
        }
        log.commit(2, &[3]).await.expect("durable");
        log.committed(1).await;
        log.committed(2).await;
        log.commit(3, &[]).await.expect("durable");
        log.committed(3).await;
        assert_eq!(numbers(&observed), [0, 1, 2]);
        let shared = Arc::clone(log.writer.shared());
        let sized = |kind: fn(&Frame) -> bool| {
            let frame = frames(&observed)[1]
                .1
                .iter()
                .find(|frame| kind(frame))
                .cloned();
            let frame = frame.expect("a frame").encode().expect("encodes");
            u64::try_from(frame.len()).expect("a length")
        };
        let frame = sized(|frame| matches!(frame, Frame::Batch(_)));
        let schema = sized(|frame| matches!(frame, Frame::Schema(_)));
        let closing = shared.closing.load(Ordering::SeqCst);
        assert!(2 * frame > closing, "a frame of {frame} beside {closing}");
        // No room to copy either chunk's open frames: only chunk 2, which the receipt of its
        // commit settles, goes.
        let held = shared.held.load(Ordering::SeqCst);
        shared.limit.store(held + frame, Ordering::SeqCst);
        relieve(&log).await;
        assert_eq!(numbers(&observed), [0, 1, 3]);
        // Room to copy chunk 1's three open frames and its schema, but not chunk 0's five: once
        // chunk 1 went, the room it freed copies chunk 0's.
        let held = shared.held.load(Ordering::SeqCst);
        let closing = shared.closing.load(Ordering::SeqCst);
        shared
            .limit
            .store(held + 3 * frame + schema + closing, Ordering::SeqCst);
        relieve(&log).await;
        assert_eq!(
            numbers(&observed),
            [4, 5],
            "chunks 0 and 1 went, their open frames carried"
        );
        assert_eq!(batches_of(&observed, 4), [4, 4, 4]);
        assert_eq!(batches_of(&observed, 5), [2, 2, 2, 2, 2]);
    })
    .await
    .expect("the writer ends");
}

#[tokio::test]
async fn a_relief_with_room_for_one_chunk_s_copies_carries_the_one_that_frees_most_first() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        // Chunks 0 and 1 each hold a batch of a settled segment beside three of an open one;
        // chunk 1's end names chunk 0, so deleting chunk 1 frees more.
        for (number, settled, open) in [(1, 1, 2), (2, 3, 4)] {
            log.batch(settled, 0).await;
            for _ in 0..3 {
                log.batch(open, 0).await;
            }
            log.commit(number, &[settled]).await.expect("durable");
        }
        log.committed(1).await;
        log.committed(2).await;
        // Chunk 2 holds a large batch whose commit waits for its receipt: it holds most of the
        // log, so a chunk may hold the open frames of chunks 0 and 1 together.
        log.rows(5, 0, 40_000).await;
        log.commit(3, &[5]).await.expect("durable");
        assert_eq!(numbers(&observed), [0, 1, 2]);
        let sized = |kind: fn(&Frame) -> bool| {
            let frame = frames(&observed)[1]
                .1
                .iter()
                .find(|frame| kind(frame))
                .cloned();
            let frame = frame.expect("a frame").encode().expect("encodes");
            u64::try_from(frame.len()).expect("a length")
        };
        let frame = sized(|frame| matches!(frame, Frame::Batch(_)));
        let schema = sized(|frame| matches!(frame, Frame::Schema(_)));
        // Room to copy one chunk's open frames and schema, not both chunks'.
        let shared = Arc::clone(log.writer.shared());
        let held = shared.held.load(Ordering::SeqCst);
        let closing = shared.closing.load(Ordering::SeqCst);
        shared
            .limit
            .store(held + 3 * frame + schema + closing, Ordering::SeqCst);
        let most = shared.limit.load(Ordering::SeqCst) / crate::limits::LOG_PARTS;
        assert!(6 * frame + 2 * schema <= most, "{frame} {schema} {most}");
        relieve(&log).await;
        // Chunk 1's open frames went first, and the room that freed copied chunk 0's; the
        // copies were then gathered into one chunk.
        assert_eq!(numbers(&observed), [2, 5]);
        assert_eq!(batches_of(&observed, 5), [4, 4, 4, 2, 2, 2]);
    })
    .await
    .expect("the writer ends");
}

#[tokio::test]
async fn a_relief_with_nothing_to_copy_frees_a_log_a_commit_took_past_its_bound() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        log.batch(1, 0).await;
        log.commit(1, &[1]).await.expect("durable");
        log.committed(1).await;
        // A commit larger than the room kept for it took the log past what it may hold.
        let shared = Arc::clone(log.writer.shared());
        let held = shared.held.load(Ordering::SeqCst);
        shared.limit.store(held - 1, Ordering::SeqCst);
        relieve(&log).await;
        assert!(!numbers(&observed).contains(&0), "chunk 0 went");
        assert!(shared.held.load(Ordering::SeqCst) < held);
    })
    .await
    .expect("the writer ends");
}

#[tokio::test]
async fn a_frame_larger_than_what_a_carry_reads_at_once_is_copied_whole() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        // Chunk 0 holds a frame of segment 1 of some 320 KB, beside a settled one of segment 2:
        // a relief copies it, more than a carry reads at once, and chunk 0 goes.
        log.rows(1, 0, 40_000).await;
        log.batch(2, 0).await;
        log.commit(1, &[2]).await.expect("durable");
        log.committed(1).await;
        relieve(&log).await;
        assert!(!numbers(&observed).contains(&0), "chunk 0 went");
        let copied = frames(&observed)
            .into_iter()
            .flat_map(|(_, frames)| frames)
            .find_map(|frame| match frame {
                Frame::Batch(batch) if batch.segment.0 == 1 => Some(batch.batch.num_rows()),
                _ => None,
            });
        assert_eq!(copied, Some(40_000), "the copy reads back whole");
        log.commit(2, &[1]).await.expect("durable");
        let scanned = scan(observed.as_ref(), &pipeline(), load(), 1 << 28)
            .await
            .expect("it reads");
        assert_eq!(scanned.pending().count(), 1);
    })
    .await
    .expect("the writer ends");
}

#[tokio::test]
async fn a_relief_whose_chunk_takes_no_copy_gathers_open_frames_into_the_next() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        // Chunk 0 holds a batch of segment 1 beside five of segment 2, chunk 1 one of segment 3
        // beside three of segment 4, chunk 2 one of segment 6 beside three of segment 7.
        log.batch(1, 0).await;
        for _ in 0..5 {
            log.batch(2, 0).await;
        }
        log.commit(1, &[1]).await.expect("durable");
        for (number, settled, open) in [(2, 3, 4), (3, 6, 7)] {
            log.batch(settled, 0).await;
            for _ in 0..3 {
                log.batch(open, 0).await;
            }
            log.commit(number, &[settled]).await.expect("durable");
        }
        for number in 1..=3 {
            log.committed(number).await;
        }
        let sized = |kind: fn(&Frame) -> bool| {
            let frame = frames(&observed)[1]
                .1
                .iter()
                .find(|frame| kind(frame))
                .cloned();
            let frame = frame.expect("a frame").encode().expect("encodes");
            u64::try_from(frame.len()).expect("a length")
        };
        let frame = sized(|frame| matches!(frame, Frame::Batch(_)));
        let schema = sized(|frame| matches!(frame, Frame::Schema(_)));
        // A chunk holds eight open frames, two schemas and a thousand bytes more: those of chunks
        // 1 and 2, or of 0 and 1, but not of all three, nor the chunk staged filled with segment
        // 5's frames beside any.
        let most = 8 * frame + 2 * schema + 1_000;
        let shared = Arc::clone(log.writer.shared());
        shared.limit.store(8 * most, Ordering::SeqCst);
        let header = sized(|frame| matches!(frame, Frame::Header(_)));
        let preamble = u64::try_from(super::super::super::frame::PREAMBLE).expect("small");
        // A batch counts its schema frame, which the chunk holds already, beside its own.
        let fits = (most - preamble - header - 2 * schema) / frame;
        for _ in 0..fits {
            log.batch(5, 0).await;
        }
        relieve(&log).await;
        let held = numbers(&observed);
        assert!(!held.contains(&0) && !held.contains(&1), "{held:?}");
        // The chunk staged is published first, and the open frames of the chunks that free the
        // most go to the next: chunks 2 and 1, whose ends name more chunks.
        assert_eq!(batches_of(&observed, 4), [7, 7, 7, 4, 4, 4]);
        let fives = held
            .iter()
            .flat_map(|number| batches_of(&observed, *number))
            .filter(|segment| *segment == 5)
            .count();
        assert_eq!(u64::try_from(fives).expect("few"), fits);
    })
    .await
    .expect("the writer ends");
}

#[tokio::test]
async fn a_relief_whose_deletion_fails_publishes_the_chunk_staged_and_carries_nothing() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        // Chunk 0 holds a settled segment alone; chunk 1 a batch of segment 1 beside five of
        // segment 2.
        log.batch(9, 0).await;
        log.commit(1, &[9]).await.expect("durable");
        log.batch(1, 0).await;
        for _ in 0..5 {
            log.batch(2, 0).await;
        }
        log.commit(2, &[1]).await.expect("durable");
        log.committed(1).await;
        log.committed(2).await;
        let sized = |kind: fn(&Frame) -> bool| {
            let frame = frames(&observed)[1]
                .1
                .iter()
                .find(|frame| kind(frame))
                .cloned();
            let frame = frame.expect("a frame").encode().expect("encodes");
            u64::try_from(frame.len()).expect("a length")
        };
        let frame = sized(|frame| matches!(frame, Frame::Batch(_)));
        let schema = sized(|frame| matches!(frame, Frame::Schema(_)));
        // A chunk holds the open frames of chunk 1 and a thousand bytes more, but not the chunk
        // staged filled with segment 5's frames beside them: the relief publishes it first.
        let most = 5 * frame + schema + 1_000;
        let shared = Arc::clone(log.writer.shared());
        shared.limit.store(8 * most, Ordering::SeqCst);
        let header = sized(|frame| matches!(frame, Frame::Header(_)));
        let preamble = u64::try_from(super::super::super::frame::PREAMBLE).expect("small");
        let fits = (most - preamble - header - 2 * schema) / frame;
        for _ in 0..fits {
            log.batch(5, 0).await;
        }
        *observed.unremovable.lock() = true;
        let (done, answer) = oneshot::channel();
        log.send(Command::Relieve { done }).await;
        let relieved = answer.await.expect("the writer answers");
        assert!(relieved.is_err(), "deleting chunk 0 failed");
    })
    .await
    .expect("the writer ends");
    // The chunk staged was published, recording the receipts; chunk 0 stayed, and nothing was
    // carried after the deletion failed.
    assert_eq!(numbers(&store), [0, 1, 2]);
}
