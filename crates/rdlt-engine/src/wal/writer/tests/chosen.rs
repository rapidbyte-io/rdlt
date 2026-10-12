//! Which old chunks a relief carries: those that free the most beside their copy first, into
//! the chunk staged where they fit beside its frames, and into the next where they pay only
//! there.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::super::super::frame::{Frame, PREAMBLE};
use super::super::super::memory::MemoryWal;
use super::relieved::{batches_of, relieve};
use super::{batch_frame, drive, frames, length, numbers, table};

#[tokio::test]
async fn a_chunk_that_frees_little_beside_a_large_copy_takes_no_room_from_chunks_that_free_more() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        // Chunk 0 holds three large frames of segment 1000, which stays open, and nothing else.
        for _ in 0..3 {
            log.rows(1000, 0, 1_000).await;
        }
        // A relief with nothing to free answers once the writer has written them.
        relieve(&log).await;
        let large: u64 = (0..3)
            .map(|ordinal| length(&batch_frame(1000, 0, ordinal, 1_000)))
            .sum();
        let small = length(&batch_frame(1000, 0, 3, 1));
        let schema = log.schemas.lock()[&0];
        // A chunk holds chunk 0's copies, but no other chunk's beside them; the next batch
        // would take chunk 0 past that, so chunk 0 is published first.
        let most = large + schema + u64::midpoint(small, schema);
        log.writer.shared().limit.store(8 * most, Ordering::SeqCst);
        // Chunks 1 to 3 each hold a frame of segment 1000 beside a settled segment's.
        for number in 1..=3 {
            log.batch(1000, 0).await;
            log.batch(10 + number, 0).await;
            log.commit(number, &[10 + number]).await.expect("durable");
            log.committed(number).await;
        }
        assert_eq!(numbers(&observed), [0, 1, 2, 3]);
        relieve(&log).await;
        // Chunk 0 is the oldest but frees less than a relief writes beside its copy; chunks 1
        // to 3 free more, and go.
        assert_eq!(numbers(&observed), [0, 4]);
        assert_eq!(batches_of(&observed, 4), [1000, 1000, 1000]);
    })
    .await
    .expect("the writer ends");
}

#[tokio::test]
async fn a_relief_whose_chunk_staged_takes_only_a_chunk_that_does_not_pay_publishes_it_first() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        // Chunk 0 holds a frame of segment 1, which stays open, beside a large settled one.
        log.rows(1, 0, 500).await;
        log.rows(2, 0, 4_000).await;
        log.commit(1, &[2]).await.expect("durable");
        log.committed(1).await;
        let schema = log.schemas.lock()[&0];
        let small = length(&batch_frame(3, 0, 2, 1));
        // Chunk 1 holds a small frame of segment 3, which stays open, and nothing else: the
        // next batch would take it past what a chunk holds, so it is published first.
        let shared = Arc::clone(log.writer.shared());
        shared.limit.store(8 * (schema + small), Ordering::SeqCst);
        log.batch(3, 0).await;
        log.batch(4, 0).await;
        // Commits waiting for their receipts keep their chunks, whose numbers lengthen the end
        // a relief writes past what chunk 1 frees beside its copy.
        for number in 2..=101 {
            log.commit(number, &[]).await.expect("durable");
        }
        let header = frames(&observed)[0]
            .1
            .iter()
            .find(|frame| matches!(frame, Frame::Header(_)))
            .map(|frame| length(&frame.encode().expect("encodes")))
            .expect("a header");
        // The chunk staged holds a large open frame: chunk 1's copy fits beside it, chunk 0's
        // does not, but fits a chunk of its own.
        let open = length(&batch_frame(1, 0, 0, 500)) + schema;
        let staged = length(&batch_frame(5, 0, 4, 4_000));
        let preamble = u64::try_from(PREAMBLE).expect("small");
        let most = preamble + header + schema + staged + small + schema + open / 2;
        shared.limit.store(8 * most, Ordering::SeqCst);
        log.rows(5, 0, 4_000).await;
        relieve(&log).await;
        // The chunk staged was published first, and chunk 0's open frame carried to the next.
        assert!(!numbers(&observed).contains(&0), "{:?}", numbers(&observed));
        let carried = numbers(&observed)
            .into_iter()
            .any(|number| batches_of(&observed, number).contains(&1));
        assert!(carried, "chunk 0's open frame was carried");
    })
    .await
    .expect("the writer ends");
}
