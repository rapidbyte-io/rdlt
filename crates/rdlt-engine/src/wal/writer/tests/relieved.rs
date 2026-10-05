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
async fn relieve(log: &Driving) {
    let (done, answer) = oneshot::channel();
    log.send(Command::Relieve { done }).await;
    answer
        .await
        .expect("the writer answers")
        .expect("it relieves");
}

/// The segments of the batch frames chunk `number` holds.
fn batches_of(store: &MemoryWal, number: u64) -> Vec<u64> {
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
        // Chunk 0 holds a batch of segment 1 beside three of segment 2, chunk 1 one of segment
        // 3 beside two of segment 4: each holds more open than settled once 1 and 3 settle, so
        // no carry follows their receipts.
        log.batch(1, 0).await;
        for _ in 0..3 {
            log.batch(2, 0).await;
        }
        log.commit(1, &[1]).await.expect("durable");
        log.batch(3, 0).await;
        log.batch(4, 0).await;
        log.batch(4, 0).await;
        log.commit(2, &[3]).await.expect("durable");
        log.committed(1).await;
        log.committed(2).await;
        log.commit(3, &[]).await.expect("durable");
        log.committed(3).await;
        assert_eq!(numbers(&observed), [0, 1, 2]);
        let shared = Arc::clone(log.writer.shared());
        let frame = frames(&observed)[1].1.iter().find_map(|frame| match frame {
            Frame::Batch(_) => Some(frame.encode().expect("encodes").len()),
            _ => None,
        });
        let frame = u64::try_from(frame.expect("a batch")).expect("a length");
        let schema = frames(&observed)[1].1.iter().find_map(|frame| match frame {
            Frame::Schema(_) => Some(frame.encode().expect("encodes").len()),
            _ => None,
        });
        let schema = u64::try_from(schema.expect("a schema")).expect("a length");
        // No room to copy either chunk's open frames: only chunk 2, which the receipt of its
        // commit settles, goes.
        let held = shared.held.load(Ordering::SeqCst);
        shared.limit.store(held + frame, Ordering::SeqCst);
        relieve(&log).await;
        assert_eq!(numbers(&observed), [0, 1, 3]);
        // Room to copy chunk 1's two open frames and its schema exactly, but not chunk 0's
        // three: once chunk 1 went, the room it freed copies chunk 0's.
        let held = shared.held.load(Ordering::SeqCst);
        shared
            .limit
            .store(held + 2 * frame + schema, Ordering::SeqCst);
        relieve(&log).await;
        assert_eq!(
            numbers(&observed),
            [4, 5],
            "chunks 0 and 1 went, their open frames carried"
        );
        assert_eq!(batches_of(&observed, 4), [4, 4]);
        assert_eq!(batches_of(&observed, 5), [2, 2, 2]);
    })
    .await
    .expect("the writer ends");
}

#[tokio::test]
async fn a_chunk_whose_open_frames_were_carried_needs_no_copy_of_them() {
    let store = Arc::new(MemoryWal::default());
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        log.batch(1, 0).await;
        log.batch(1, 0).await;
        log.batch(2, 0).await;
        log.commit(1, &[1]).await.expect("durable");
        let shared = Arc::clone(log.writer.shared());
        assert!(
            shared.copied.load(Ordering::SeqCst) > 0,
            "two open segments share chunk 0"
        );
        // Its receipt settles segment 1, and segment 2's frame is carried into the chunk
        // staged; a seal there keeps the relief from publishing, so it only answers.
        log.committed(1).await;
        log.seal(2).await;
        relieve(&log).await;
        assert_eq!(shared.copied.load(Ordering::SeqCst), 0);
        log.commit(2, &[2]).await.expect("durable");
    })
    .await
    .expect("the writer ends");
}
