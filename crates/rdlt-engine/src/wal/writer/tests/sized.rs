//! What a chunk holds at most, an eighth of what the log may hold, and the room a carry keeps.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::super::super::frame::{Frame, PREAMBLE};
use super::super::super::memory::MemoryWal;
use super::super::Command;
use super::{chunks, drive, frames, segments, table};

/// Bytes: what the first frame of chunk `number` for which `kind` holds takes.
fn sized(store: &MemoryWal, number: u64, kind: fn(&Frame) -> bool) -> u64 {
    let frame = frames(store)
        .into_iter()
        .filter(|(chunk, _)| *chunk == number)
        .flat_map(|(_, frames)| frames)
        .find(kind)
        .expect("the frame is there");
    let encoded = frame.encode().expect("it encodes");
    u64::try_from(encoded.len()).expect("a length")
}

/// The kinds of the frames of chunk `number`.
fn kinds(store: &MemoryWal, number: u64) -> Vec<String> {
    chunks(store)
        .into_iter()
        .find(|(chunk, _)| *chunk == number)
        .map(|(_, kinds)| kinds)
        .unwrap_or_default()
}

#[tokio::test]
async fn a_chunk_is_published_between_commits_only_once_a_batch_would_take_it_past_an_eighth() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        log.batch(1, 0).await;
        log.commit(1, &[1]).await.expect("durable");
        let header = sized(&observed, 0, |frame| matches!(frame, Frame::Header(_)));
        let schema = sized(&observed, 0, |frame| matches!(frame, Frame::Schema(_)));
        let batch = sized(&observed, 0, |frame| matches!(frame, Frame::Batch(_)));
        // A second batch, counted with its schema frame, takes the next chunk to an eighth
        // exactly: it stays.
        let first = u64::try_from(PREAMBLE).expect("small") + header + schema + batch;
        let shared = Arc::clone(log.writer.shared());
        shared
            .limit
            .store(8 * (first + schema + batch), Ordering::SeqCst);
        log.batch(2, 0).await;
        log.batch(2, 0).await;
        log.commit(2, &[2]).await.expect("durable");
        let batches = |number| {
            kinds(&observed, number)
                .iter()
                .filter(|kind| kind.starts_with("batch"))
                .count()
        };
        assert_eq!(batches(1), 2, "{:?}", kinds(&observed, 1));
        // A byte past it, the chunk is published before the second.
        shared
            .limit
            .store(8 * (first + schema + batch - 1), Ordering::SeqCst);
        log.batch(3, 0).await;
        log.batch(3, 0).await;
        log.commit(3, &[3]).await.expect("durable");
        assert!(kinds(&observed, 2).contains(&"relieved".to_owned()));
        assert_eq!((batches(2), batches(3)), (1, 1));
    })
    .await
    .expect("the writer ends");
}

#[tokio::test]
async fn a_chunk_holding_a_commit_s_seals_takes_no_other_frame_before_the_commit() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        log.batch(1, 0).await;
        // Segment 1's seal goes out; segment 2's batch, a receipt and an abandonment come
        // before the commit's frame.
        log.seal(1).await;
        log.batch(2, 0).await;
        log.committed(9).await;
        log.send(Command::Abandon {
            segment: rdlt_connector::SegmentId(3),
        })
        .await;
        log.commit(1, &[]).await.expect("durable");
        log.commit(2, &[2]).await.expect("durable");
        let commit = format!("commit of {:?}", segments(&[]));
        let first = ["header 0", "schema 0", "batch 1 of 0", "seal 1", &commit];
        assert_eq!(kinds(&observed, 0)[..5], first.map(str::to_owned));
        assert_eq!(kinds(&observed, 1)[2], "batch 2 of 0");
    })
    .await
    .expect("the writer ends");
}
