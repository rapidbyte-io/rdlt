//! Which old chunks a relief carries: those that free the most beside their copy first, into
//! the chunk staged where they fit beside its frames, and into the next where they pay only
//! there.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::super::super::frame::{Frame, HEAD, PREAMBLE, end_len};
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

#[tokio::test]
async fn copies_a_relief_gathers_into_the_chunk_staged_fit_an_eighth_beside_its_header_and_end() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        log.send(table(1)).await;
        // Chunks 0 and 1 each hold an open frame of a table of their own beside a settled one.
        log.rows(1, 0, 200).await;
        log.rows(2, 0, 500).await;
        log.commit(1, &[2]).await.expect("durable");
        log.committed(1).await;
        log.rows(3, 1, 200).await;
        log.rows(4, 1, 500).await;
        log.commit(2, &[4]).await.expect("durable");
        log.committed(2).await;
        let schemas: u64 = log.schemas.lock().values().sum();
        let open = length(&batch_frame(1, 0, 0, 200)) + length(&batch_frame(3, 1, 2, 200));
        // Both copies fit an eighth, but not beside the header and end of a chunk.
        let most = open + schemas + 1;
        log.writer.shared().limit.store(8 * most, Ordering::SeqCst);
        relieve(&log).await;
        assert!(!numbers(&observed).contains(&0), "{:?}", numbers(&observed));
        assert!(!numbers(&observed).contains(&1), "{:?}", numbers(&observed));
        for (chunk, bytes) in observed.stored(&super::pipeline()) {
            let bytes = u64::try_from(bytes.len()).expect("a length");
            assert!(bytes <= most, "chunk {}: {bytes} of {most}", chunk.number);
        }
    })
    .await
    .expect("the writer ends");
}

#[tokio::test]
async fn a_relief_gathers_open_frames_whose_copies_fit_a_chunk_with_their_table_s_schema_once() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        // Chunks 0 and 1 each hold an open frame of one table beside a settled one.
        log.rows(1, 0, 200).await;
        log.rows(2, 0, 500).await;
        log.commit(1, &[2]).await.expect("durable");
        log.committed(1).await;
        log.rows(3, 0, 200).await;
        log.rows(4, 0, 500).await;
        log.commit(2, &[4]).await.expect("durable");
        log.committed(2).await;
        let schema = log.schemas.lock()[&0];
        let header = frames(&observed)[0]
            .1
            .iter()
            .find(|frame| matches!(frame, Frame::Header(_)))
            .map(|frame| length(&frame.encode().expect("encodes")))
            .expect("a header");
        // What staging and ending the chunk they go to writes at most: its preamble and
        // header, an end naming every chunk and commit the log holds, and a relief frame.
        let ending =
            u64::try_from(PREAMBLE + HEAD).expect("small") + header + end_len([0, 1], [1, 2]);
        let open = length(&batch_frame(1, 0, 0, 200)) + length(&batch_frame(3, 0, 2, 200));
        // Both open frames fit a chunk beside it, with their table's schema frame written once.
        let most = ending + open + schema;
        log.writer.shared().limit.store(8 * most, Ordering::SeqCst);
        relieve(&log).await;
        assert_eq!(numbers(&observed), [2]);
        let mut gathered = batches_of(&observed, 2);
        gathered.sort_unstable();
        assert_eq!(gathered, [1, 3]);
        for (_, bytes) in observed.stored(&super::pipeline()) {
            assert!(u64::try_from(bytes.len()).expect("a length") <= most);
        }
    })
    .await
    .expect("the writer ends");
}
