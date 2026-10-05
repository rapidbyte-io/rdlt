//! What a chunk holds at most, an eighth of what the log may hold, and the room a carry keeps.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::sync::oneshot;

use super::super::super::frame::{Frame, PREAMBLE};
use super::super::super::memory::MemoryWal;
use super::super::Command;
use super::{Driving, chunks, drive, frames, table};

/// Completes once the writer has handled every command sent before: a relief, in a log past
/// what it may hold, publishes nothing.
async fn written(log: &Driving) {
    let (done, answer) = oneshot::channel();
    log.send(Command::Relieve { done }).await;
    answer
        .await
        .expect("the writer answers")
        .expect("it relieves");
}

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
async fn a_carry_keeps_room_for_the_open_frames_of_a_chunk_holding_settled_ones_too() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        let limit = 1 << 20;
        let shared = Arc::clone(log.writer.shared());
        shared.limit.store(limit, Ordering::SeqCst);
        log.send(table(0)).await;
        // Segment 1 seals, and segment 2 logs past an eighth of the log beside it: the chunk,
        // holding a seal, goes with its commit.
        log.batch(1, 0).await;
        log.seal(1).await;
        log.rows(2, 0, 40_000).await;
        log.commit(1, &[1]).await.expect("durable");
        assert_eq!(shared.carry.load(Ordering::SeqCst), limit / 8);
        // Once segment 1 settles, a carry of chunk 0 copies segment 2's frame and the schema;
        // the log, full, has no room to carry it now.
        log.count(limit);
        log.committed(1).await;
        written(&log).await;
        let schema = sized(&observed, 0, |frame| matches!(frame, Frame::Schema(_)));
        let open = frames(&observed)
            .into_iter()
            .filter(|(chunk, _)| *chunk == 0)
            .flat_map(|(_, frames)| frames)
            .find_map(|frame| match frame {
                Frame::Batch(batch) if batch.segment.0 == 2 => Some(Frame::Batch(batch)),
                _ => None,
            })
            .map(|frame| frame.encode().expect("it encodes").len())
            .expect("segment 2's frame");
        let copies = schema + u64::try_from(open).expect("a length");
        assert!(copies > limit / 8);
        assert_eq!(shared.carry.load(Ordering::SeqCst), copies);
        // A larger chunk of open frames alone needs no carry: the room stays.
        log.rows(3, 0, 80_000).await;
        log.batch(3, 0).await;
        written(&log).await;
        assert!(kinds(&observed, 1).contains(&"relieved".to_owned()));
        assert_eq!(shared.carry.load(Ordering::SeqCst), copies);
    })
    .await
    .expect("the writer ends");
}
