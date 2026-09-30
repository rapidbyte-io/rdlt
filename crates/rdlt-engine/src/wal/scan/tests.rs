use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use bytes::{BufMut, Bytes, BytesMut};
use rdlt_connector::{
    CommitMeta, CommitSeq, Epoch, LoadId, PartitionId, PartitionState, PipelineId, Receipt,
    SegmentId, StreamName,
};

use super::{batch, scan};
use crate::budget::MemoryBudget;
use crate::compute::Inline;
use crate::error::ErrorKind;
use crate::table::testing::view;
use crate::wal::load::{LoadLog, Sealed};
use crate::wal::memory::MemoryWal;
use crate::wal::store::{Chunk, WalStore};

fn pipeline() -> PipelineId {
    PipelineId::parse("orders").expect("a valid pipeline")
}

fn load() -> LoadId {
    LoadId::from_parts(UNIX_EPOCH, 5)
}

fn ids(from: i64) -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(from..from + 2));
    RecordBatch::try_from_iter([("id", ids)]).expect("a valid batch")
}

fn seq(number: u64) -> CommitSeq {
    (1..number).fold(CommitSeq::FIRST, |seq, _| seq.next())
}

fn meta(number: u64, segments: &[u64]) -> CommitMeta {
    CommitMeta {
        load_id: load(),
        commit_seq: seq(number),
        epoch: Epoch(1),
        segments: segments.iter().copied().map(SegmentId).collect(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
    }
}

fn sealed(segment: u64) -> Sealed {
    Sealed {
        segment: SegmentId(segment),
        stream: StreamName::new("orders").expect("a valid stream"),
        partition: PartitionId::parse("p0").expect("a valid partition"),
        replayable: true,
        from: None,
        state: PartitionState::Done,
    }
}

/// A log of two commits of segments 1 and 2, the first `received` or not, as a load that crashed
/// after the second's frame was durable writes it.
async fn logged(received: bool) -> Arc<MemoryWal> {
    let store = Arc::new(MemoryWal::default());
    let wal: Arc<dyn WalStore> = Arc::clone(&store) as Arc<dyn WalStore>;
    let opened = Some((LoadId::from_parts(UNIX_EPOCH, 1), CommitSeq::FIRST));
    let (log, task) = LoadLog::start(wal, pipeline(), load(), opened)
        .await
        .expect("the log starts");
    let budget = MemoryBudget::new(1 << 20);
    let (orders, items) = (view("orders"), view("items"));
    let written = async {
        log.batch(&Inline, &budget, 0, &orders, SegmentId(1), &ids(0))
            .await
            .expect("logged");
        log.batch(&Inline, &budget, 1, &items, SegmentId(1), &ids(10))
            .await
            .expect("logged");
        log.commit(vec![sealed(1)], &meta(1, &[1]))
            .await
            .expect("durable");
        let receipt = Receipt {
            load_id: load(),
            commit_seq: seq(1),
            committed_at: UNIX_EPOCH,
            rows: 4,
            bytes: 32,
        };
        if received {
            log.committed(&receipt).await.expect("logged");
        }
        log.batch(&Inline, &budget, 0, &orders, SegmentId(2), &ids(20))
            .await
            .expect("logged");
        log.commit(vec![sealed(2)], &meta(2, &[2]))
            .await
            .expect("durable");
        drop(log);
    };
    let (ended, ()) = tokio::join!(task, written);
    ended.expect("the writer ends");
    store
}

#[tokio::test]
async fn a_log_scans_back_to_what_its_load_wrote() {
    let store = logged(false).await;
    let scanned = scan(store.as_ref(), &pipeline(), load())
        .await
        .expect("the log reads");
    let header = scanned.header.as_ref().expect("a header");
    assert_eq!(
        header.opened,
        Some((LoadId::from_parts(UNIX_EPOCH, 1), CommitSeq::FIRST))
    );
    let named: Vec<_> = scanned
        .tables
        .values()
        .map(|table| table.table.name.to_string())
        .collect();
    assert_eq!(named, ["orders", "items"]);
    let commits: Vec<_> = scanned
        .commits
        .iter()
        .map(|logged| {
            let sealed: Vec<_> = logged.seals.iter().map(|seal| seal.segment.0).collect();
            (logged.meta.clone(), sealed)
        })
        .collect();
    assert_eq!(
        commits,
        [(meta(1, &[1]), vec![1]), (meta(2, &[2]), vec![2])]
    );
    assert_eq!(scanned.pending().count(), 2);
    assert!(!scanned.closed);
    let mut read = Vec::new();
    for (segment, located) in &scanned.batches {
        for located in located {
            let batch = batch(store.as_ref(), &pipeline(), *located)
                .await
                .expect("the batch reads");
            read.push((segment.0, located.table, batch));
        }
    }
    assert_eq!(read, [(1, 0, ids(0)), (1, 1, ids(10)), (2, 0, ids(20))]);
}

#[tokio::test]
async fn a_received_commit_s_chunk_is_gone_and_its_receipt_reads_back() {
    let store = logged(true).await;
    let scanned = scan(store.as_ref(), &pipeline(), load())
        .await
        .expect("the log reads");
    // The second chunk restates the schema its batch names, and holds the first receipt.
    let named: Vec<_> = scanned
        .tables
        .values()
        .map(|table| table.table.name.to_string())
        .collect();
    assert_eq!(named, ["orders"]);
    let pending: Vec<_> = scanned
        .pending()
        .map(|logged| logged.meta.clone())
        .collect();
    assert_eq!(pending, [meta(2, &[2])]);
    assert_eq!(scanned.commits.len(), 1);
    assert!(scanned.received.contains(&seq(1)));
    assert_eq!(
        scanned.batches.keys().copied().collect::<Vec<_>>(),
        [SegmentId(2)]
    );
}

#[tokio::test]
async fn a_torn_chunk_reads_up_to_its_tear() {
    let whole = logged(false).await;
    let last = whole
        .stored(&pipeline())
        .last()
        .map(|(chunk, stored)| (*chunk, stored.bytes.clone()))
        .expect("a chunk");
    for cut in 0..last.1.len() {
        let torn = MemoryWal::default();
        for (chunk, stored) in whole.stored(&pipeline()) {
            let bytes = if chunk == last.0 {
                Bytes::copy_from_slice(&stored.bytes[..cut])
            } else {
                Bytes::from(stored.bytes)
            };
            torn.append(&pipeline(), chunk, bytes)
                .await
                .expect("appends");
        }
        let scanned = scan(&torn, &pipeline(), load())
            .await
            .expect("a torn log reads");
        // The last chunk holds the second commit's frame, whole only where nothing was cut.
        let commits: Vec<_> = scanned.commits.iter().map(|logged| &logged.meta).collect();
        assert_eq!(commits, [&meta(1, &[1])], "cut at {cut}");
        assert_eq!(scanned.pending().count(), 1, "cut at {cut}");
    }
}

/// A frame of `kind` whose checksum matches `payload`.
fn framed(kind: u8, payload: &[u8]) -> Bytes {
    let mut frame = BytesMut::new();
    frame.put_u8(kind);
    frame.put_u32_le(u32::try_from(payload.len()).expect("a short payload"));
    frame.put_u32_le(crc32c::crc32c(payload));
    frame.put_slice(payload);
    frame.freeze()
}

#[tokio::test]
async fn a_whole_frame_that_does_not_decode_makes_the_log_unreadable() {
    let header = |version: u16, load: LoadId| {
        let header = serde_json::json!({
            "version": version,
            "pipeline": "orders",
            "load": load.to_string(),
            "opened": null,
        });
        framed(1, header.to_string().as_bytes())
    };
    let chunk = Chunk {
        load: load(),
        number: 0,
    };
    for frames in [
        vec![header(1, load()), framed(99, b"")],
        vec![header(1, load()), framed(5, b"{\"not\":\"a commit\"}")],
        vec![header(2, load())],
        vec![header(1, LoadId::from_parts(UNIX_EPOCH, 6))],
    ] {
        let store = MemoryWal::default();
        for frame in frames {
            store
                .append(&pipeline(), chunk, frame)
                .await
                .expect("appends");
        }
        let error = scan(&store, &pipeline(), load())
            .await
            .expect_err("the log is not one this engine wrote");
        assert_eq!(error.kind(), ErrorKind::Wal);
        assert_eq!(error.code(), Some("wal_unreadable"));
        assert!(!error.is_retryable());
    }
}

#[tokio::test]
async fn a_chunk_before_the_last_that_ends_early_or_garbled_makes_the_log_unreadable() {
    // Every chunk but the last ends with a commit's frame, made durable: no crash tears it.
    let whole = logged(false).await;
    let stored = whole.stored(&pipeline());
    assert_eq!(stored.len(), 2);
    let first = stored[0].1.bytes.clone();
    for damage in ["cut", "garbled"] {
        let damaged = MemoryWal::default();
        for (chunk, stored) in whole.stored(&pipeline()) {
            let mut bytes = stored.bytes;
            if chunk.number == 0 {
                match damage {
                    "cut" => bytes.truncate(first.len() - 1),
                    _ => bytes[first.len() / 2] ^= 0x5a,
                }
            }
            damaged
                .append(&pipeline(), chunk, Bytes::from(bytes))
                .await
                .expect("appends");
        }
        let error = scan(&damaged, &pipeline(), load())
            .await
            .expect_err("a durable chunk damaged is not a crash");
        assert_eq!(error.code(), Some("wal_unreadable"), "{damage}");
    }
}

#[tokio::test]
async fn a_scan_indexes_batches_without_decoding_them_and_a_read_refuses_a_garbled_one() {
    let header = serde_json::json!({
        "version": 1,
        "pipeline": "orders",
        "load": load().to_string(),
        "opened": null,
    });
    let head = br#"{"segment":1,"table":0}"#;
    let mut payload = u32::try_from(head.len())
        .expect("short")
        .to_le_bytes()
        .to_vec();
    payload.extend_from_slice(head);
    payload.extend_from_slice(b"not an arrow stream");
    let chunk = Chunk {
        load: load(),
        number: 0,
    };
    let store = MemoryWal::default();
    for frame in [
        framed(1, header.to_string().as_bytes()),
        framed(3, &payload),
    ] {
        store
            .append(&pipeline(), chunk, frame)
            .await
            .expect("appends");
    }
    let scanned = scan(&store, &pipeline(), load())
        .await
        .expect("the scan reads only the batch's head");
    let located = scanned.batches[&SegmentId(1)][0];
    assert_eq!(located.table, 0);
    let error = batch(&store, &pipeline(), located)
        .await
        .expect_err("the batch does not decode");
    assert_eq!(error.code(), Some("wal_unreadable"));
}

#[tokio::test]
async fn a_batch_read_past_its_frame_is_refused() {
    let store = logged(false).await;
    let scanned = scan(store.as_ref(), &pipeline(), load())
        .await
        .expect("the log reads");
    let mut located = scanned.batches[&SegmentId(1)][0];
    batch(store.as_ref(), &pipeline(), located)
        .await
        .expect("the batch reads where the scan found it");
    // A location that runs on into the next frame is not the frame the scan found.
    located.len += 9;
    let error = batch(store.as_ref(), &pipeline(), located)
        .await
        .expect_err("more than the batch's frame");
    assert_eq!(error.code(), Some("wal_unreadable"));
}
