//! Which old chunks a relief carries: those that free the most beside their copy first, into
//! the chunk staged where they fit beside its frames, and into the next where they pay only
//! there.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use rdlt_wire::limits::count;

use super::super::super::frame::{Frame, HEAD, Header, PREAMBLE, end_len, header_len};
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

#[tokio::test]
async fn a_relief_gathers_chunks_of_open_frames_alone_until_no_two_fit_one() {
    // Chunks of one open frame each, then a chunk may hold `holds` of them: a relief leaves at
    // most one chunk of open frames alone holding half of that or less.
    for chunks in [3, 4, 5, 6, 7, 8, 9, 12] {
        for holds in [3_usize, 4, 5] {
            let store = Arc::new(MemoryWal::default());
            let observed = Arc::clone(&store);
            drive(Arc::clone(&store), |mut log| async move {
                log.send(table(0)).await;
                let schema = log.schemas.lock()[&0];
                let frame = length(&batch_frame(1, 0, 10, 100));
                let header = Header {
                    pipeline: super::pipeline(),
                    load: super::load(),
                    chunk: 10,
                    epoch: rdlt_connector::Epoch(1),
                    opened: None,
                    origin: super::load(),
                };
                let first = u64::try_from(PREAMBLE).expect("small")
                    + header_len(&header).expect("measures")
                    + schema;
                let spare = frame / 2;
                // A chunk holds one frame.
                let shared = Arc::clone(log.writer.shared());
                shared
                    .limit
                    .store(8 * (first + frame + spare), Ordering::SeqCst);
                for _ in 0..=chunks {
                    log.rows(1, 0, 100).await;
                }
                // Answered once the writer wrote them, as chunks of their own.
                relieve(&log).await;
                // A chunk holds `holds` frames, beside what ends it.
                let most = first + count(holds) * frame + spare;
                shared.limit.store(8 * most, Ordering::SeqCst);
                relieve(&log).await;
                let held: Vec<usize> = numbers(&observed)
                    .into_iter()
                    .map(|number| batches_of(&observed, number).len())
                    .collect();
                let small = held.iter().filter(|frames| 2 * **frames <= holds).count();
                assert!(small <= 1, "{chunks} chunks, {holds} a chunk: {held:?}");
            })
            .await
            .expect("the writer ends");
        }
    }
}

#[tokio::test]
async fn a_relief_counts_only_the_schema_frames_of_the_tables_a_chunk_holds_open_frames_of() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        log.send(table(1)).await;
        // Chunk 0 holds an open frame of table 0 beside a settled one of table 1; chunk 1 an
        // open frame of table 0 beside a settled one.
        log.rows(1, 0, 200).await;
        log.rows(2, 1, 500).await;
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
        let ending =
            u64::try_from(PREAMBLE + HEAD).expect("small") + header + end_len([0, 1], [1, 2]);
        let open = length(&batch_frame(1, 0, 0, 200)) + length(&batch_frame(3, 0, 2, 200));
        // Both open frames fit a chunk beside table 0's schema frame alone: no copy takes table
        // 1's.
        let most = ending + open + schema;
        log.writer.shared().limit.store(8 * most, Ordering::SeqCst);
        relieve(&log).await;
        assert_eq!(numbers(&observed), [2]);
        let mut gathered = batches_of(&observed, 2);
        gathered.sort_unstable();
        assert_eq!(gathered, [1, 3]);
    })
    .await
    .expect("the writer ends");
}

#[tokio::test]
async fn a_relief_takes_a_chunk_of_settled_frames_no_commit_holds_by_what_it_frees() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        // Chunk 0 holds an open frame beside a large one of segment 2, and is published between
        // commits; chunk 1 an open frame beside segment 2's seal and commit.
        log.rows(1, 0, 100).await;
        log.rows(2, 0, 1_000).await;
        relieve(&log).await;
        let shared = Arc::clone(log.writer.shared());
        shared.limit.store(8 * 1_000, Ordering::SeqCst);
        log.rows(5, 0, 100).await;
        relieve(&log).await;
        shared.limit.store(u64::MAX, Ordering::SeqCst);
        log.commit(1, &[2]).await.expect("durable");
        log.committed(1).await;
        relieve(&log).await;
        // Chunk 0, which frees segment 2's frame, goes first, though no commit is in it.
        assert_eq!(numbers(&observed), [2]);
        assert_eq!(batches_of(&observed, 2), [1, 5]);
    })
    .await
    .expect("the writer ends");
}

/// Bytes: what the store holds of chunk `number`.
fn stored(store: &MemoryWal, number: u64) -> u64 {
    store
        .stored(&super::pipeline())
        .into_iter()
        .find(|(chunk, _)| chunk.number == number)
        .map(|(_, bytes)| length(&bytes))
        .expect("the chunk is there")
}

/// Bytes: what staging and ending the next chunk writes at most, where `pending` commits
/// without receipts follow two chunks of a commit each: its preamble and header, an end naming
/// every chunk and commit the log holds, and a relief frame.
fn ending(pending: u64) -> u64 {
    let header = Header {
        pipeline: super::pipeline(),
        load: super::load(),
        chunk: pending + 2,
        epoch: rdlt_connector::Epoch(1),
        opened: None,
        origin: super::load(),
    };
    u64::try_from(PREAMBLE + HEAD).expect("small")
        + header_len(&header).expect("measures")
        + end_len(0..pending + 2, 1..=pending + 2)
}

#[tokio::test]
async fn a_relief_takes_first_the_chunk_that_frees_the_most_beside_what_its_copy_writes() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        let tables = 9_u32;
        for index in 0..tables {
            log.send(table(index)).await;
        }
        // Chunk 0 holds an open frame of table 0 beside settled frames of eight tables more,
        // whose schema frames no copy of it writes.
        log.rows(1, 0, 50).await;
        for index in 1..tables {
            log.rows(u64::from(index) + 1, index, 1).await;
        }
        let settled: Vec<u64> = (2..=u64::from(tables)).collect();
        log.commit(1, &settled).await.expect("durable");
        log.committed(1).await;
        let schemas = log.schemas.lock().clone();
        let open = length(&batch_frame(1, 0, 0, 50));
        let copy = open + schemas[&0];
        // What deleting chunk 0 frees beside its copy, and beside its copy and every schema
        // frame it holds.
        let frees = stored(&observed, 0) - copy;
        let beside_all = frees - schemas.values().skip(1).sum::<u64>();
        // Chunk 1 holds an open frame of table 0 beside a settled one that frees about as much
        // as chunk 0 beside all its schema frames.
        let ordinal = u64::from(tables) + 1;
        let target = u64::midpoint(beside_all, frees) - 600;
        let rows = (1..4_000)
            .find(|rows| length(&batch_frame(20, 0, ordinal, *rows)) >= target)
            .expect("rows enough");
        log.rows(10, 0, 50).await;
        log.rows(20, 0, rows).await;
        log.commit(2, &[20]).await.expect("durable");
        log.committed(2).await;
        let other = stored(&observed, 1) - copy;
        assert!(
            beside_all < other && other < frees,
            "{beside_all} {other} {frees}"
        );
        // Commits waiting for their receipts lengthen the end a relief writes past what chunk 1
        // frees, but not past what chunk 0 does.
        let pending = (0..2_000)
            .find(|pending| ending(*pending) >= other)
            .expect("an end long enough");
        assert!(ending(pending) < frees, "{} {frees}", ending(pending));
        for number in 0..pending {
            log.commit(number + 3, &[]).await.expect("durable");
        }
        // A chunk holds either copy beside what ends it, not both.
        let shared = Arc::clone(log.writer.shared());
        shared
            .limit
            .store(8 * (ending(pending) + copy), Ordering::SeqCst);
        // Room enough for either copy, whatever the pending commits' chunks hold.
        shared.held.store(0, Ordering::SeqCst);
        relieve(&log).await;
        // Chunk 0 went, as it frees more beside its copy than ending a chunk takes; chunk 1,
        // which frees more beside every schema frame it holds, would not have paid alone.
        assert!(!numbers(&observed).contains(&0), "{:?}", numbers(&observed));
    })
    .await
    .expect("the writer ends");
}
