mod forged;

use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use bytes::Bytes;
use rdlt_connector::{
    CommitMeta, CommitSeq, Epoch, LoadId, PartitionId, PartitionState, PipelineId, Receipt,
    SegmentId, StateChange, StateEntry, StreamName,
};

use rdlt_connector::cost::Allocations;

use super::{batch, scan};
use crate::budget::MemoryBudget;
use crate::compute::Pool;
use crate::error::ErrorKind;
use crate::table::TableView;
use crate::table::testing::view;
use crate::wal::frame::{self, BegunPhase};
use crate::wal::load::{LoadLog, Owner, Sealed};
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
        abandoned: rdlt_connector::SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
        horizon: None,
    }
}

fn sealed(segment: u64) -> Sealed {
    Sealed {
        segment: SegmentId(segment),
        stream: StreamName::new("orders").expect("a valid stream"),
        partition: PartitionId::parse("p0").expect("a valid partition"),
        replayable: true,
        phase: 0,
        from: None,
        state: PartitionState::Done,
    }
}

/// Bytes: the most a frame read in these tests may take.
const FRAME_BYTES: u64 = 1 << 20;

/// What a log's batches may hold in these tests.
fn limits() -> rdlt_wire::Limits {
    frame::limits(1 << 30)
}

/// A log of two commits of segments 1 and 2, the first `received` or not, as a load that crashed
/// after the second's chunk was published writes it.
async fn logged(received: bool) -> Arc<MemoryWal> {
    logged_beginning(received, Vec::new()).await
}

/// A phase `orders` begins, as the second commit of [`logged_beginning`] logs it.
fn begun() -> BegunPhase {
    let changes = StateEntry::Phase {
        stream: StreamName::new("orders").expect("a valid stream"),
        phase: 1,
    };
    BegunPhase {
        stream: StreamName::new("orders").expect("a valid stream"),
        phase: 1,
        changes: vec![StateChange::Put(changes.to_record())],
    }
}

/// Logs `batch` of `segment` for `table` in `log`, its frame held by `budget`.
async fn log_batch(
    log: &LoadLog,
    budget: &MemoryBudget,
    table: (usize, &Arc<TableView>),
    segment: SegmentId,
    batch: &RecordBatch,
) -> Result<(), crate::Error> {
    log.batch(
        &Pool::inline(),
        budget,
        frame(budget),
        table,
        segment,
        batch,
    )
    .await
}

/// The owner of the logs these tests write.
fn owner() -> Owner {
    Owner {
        pipeline: pipeline(),
        load: load(),
        epoch: Epoch(1),
        opened: Some((LoadId::from_parts(UNIX_EPOCH, 1), CommitSeq::FIRST)),
        origin: load(),
    }
}

/// A log as [`logged`] writes it, whose second commit begins the phases `begun`.
async fn logged_beginning(received: bool, begun: Vec<BegunPhase>) -> Arc<MemoryWal> {
    let store = Arc::new(MemoryWal::default());
    store.open(&pipeline(), load());
    let wal: Arc<dyn WalStore> = Arc::clone(&store) as Arc<dyn WalStore>;
    let budget = crate::budget::budget(1 << 20);
    let (log, task) = LoadLog::start(wal, owner(), std::num::NonZeroU64::MAX, None);
    let (orders, items) = (view("orders"), view("items"));
    let written = async {
        log_batch(&log, &budget, (0, &orders), SegmentId(1), &ids(0))
            .await
            .expect("logged");
        log_batch(&log, &budget, (1, &items), SegmentId(1), &ids(10))
            .await
            .expect("logged");
        log.commit(&budget, vec![sealed(1)], Vec::new(), &meta(1, &[1]), 0)
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
        log_batch(&log, &budget, (0, &orders), SegmentId(2), &ids(20))
            .await
            .expect("logged");
        log.commit(&budget, vec![sealed(2)], begun, &meta(2, &[2]), 0)
            .await
            .expect("durable");
        drop(log);
    };
    let (ended, ()) = tokio::join!(task, written);
    ended.expect("the writer ends");
    store
}

/// The chunks of `from`, each changed as `change` says, in a store of their own.
async fn copied(from: &MemoryWal, change: impl Fn(Chunk, &mut Vec<u8>)) -> MemoryWal {
    let store = MemoryWal::default();
    for (chunk, bytes) in from.stored(&pipeline()) {
        let mut bytes = bytes.to_vec();
        change(chunk, &mut bytes);
        published(&store, &pipeline(), chunk, bytes).await;
    }
    store
}

/// Publishes `bytes` as `chunk` of `pipeline`'s log in `store`.
async fn published(store: &MemoryWal, pipeline: &PipelineId, chunk: Chunk, bytes: Vec<u8>) {
    store.open(pipeline, chunk.load);
    let mut staged = store.stage(pipeline, chunk).await.expect("stages");
    staged.append(Bytes::from(bytes)).await.expect("appends");
    staged.publish().await.expect("publishes");
}

#[tokio::test]
async fn a_log_scans_back_to_what_its_load_wrote() {
    let store = logged(false).await;
    let scanned = scan(store.as_ref(), &pipeline(), load(), FRAME_BYTES)
        .await
        .expect("the log reads");
    let header = scanned.header.as_ref().expect("a header");
    assert_eq!(
        (header.epoch, header.opened),
        (
            Epoch(1),
            Some((LoadId::from_parts(UNIX_EPOCH, 1), CommitSeq::FIRST))
        )
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
    let mut read = Vec::new();
    for (segment, located) in &scanned.batches {
        for located in located {
            let read_batch = batch(store.as_ref(), &pipeline(), *located, limits())
                .await
                .expect("the batch reads");
            let held = read_batch.held();
            let decoded = read_batch.decode().expect("the batch decodes");
            // What its buffers were measured to take before it was decoded, they take; what else
            // the batch holds is little.
            let holds = Allocations::of(&decoded).bytes();
            assert!(
                held <= holds && holds - held < 1 << 10,
                "{held} {holds}: {located:?}"
            );
            read.push((segment.0, located.table, decoded));
        }
    }
    assert_eq!(read, [(1, 0, ids(0)), (1, 1, ids(10)), (2, 0, ids(20))]);
}

#[tokio::test]
async fn a_received_commit_s_chunk_is_gone_and_only_what_is_needed_is_read() {
    let store = logged(true).await;
    assert_eq!(store.stored(&pipeline()).len(), 1, "chunk 0 is gone");
    let scanned = scan(store.as_ref(), &pipeline(), load(), FRAME_BYTES)
        .await
        .expect("the log reads");
    // The second chunk restates the schema its batch names.
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
    assert_eq!(
        scanned.batches.keys().copied().collect::<Vec<_>>(),
        [SegmentId(2)]
    );
}

#[tokio::test]
async fn a_commit_s_receipt_noted_in_a_later_chunk_s_end_settles_it() {
    // Commit 1 is received while segment 3 stays open: chunk 0 stays, its commit received.
    let store = Arc::new(MemoryWal::default());
    store.open(&pipeline(), load());
    let wal: Arc<dyn WalStore> = Arc::clone(&store) as Arc<dyn WalStore>;
    let budget = crate::budget::budget(1 << 20);
    let (log, task) = LoadLog::start(wal, owner(), std::num::NonZeroU64::MAX, None);
    let orders = view("orders");
    let written = async {
        log_batch(&log, &budget, (0, &orders), SegmentId(1), &ids(0))
            .await
            .expect("logged");
        for from in [100, 102] {
            log_batch(&log, &budget, (0, &orders), SegmentId(3), &ids(from))
                .await
                .expect("logged");
        }
        log.commit(&budget, vec![sealed(1)], Vec::new(), &meta(1, &[1]), 0)
            .await
            .expect("durable");
        let receipt = Receipt {
            load_id: load(),
            commit_seq: seq(1),
            committed_at: UNIX_EPOCH,
            rows: 2,
            bytes: 16,
        };
        log.committed(&receipt).await.expect("noted");
        log_batch(&log, &budget, (0, &orders), SegmentId(2), &ids(20))
            .await
            .expect("logged");
        log.commit(&budget, vec![sealed(2)], Vec::new(), &meta(2, &[2]), 0)
            .await
            .expect("durable");
        drop(log);
    };
    let (ended, ()) = tokio::join!(task, written);
    ended.expect("the writer ends");
    assert_eq!(store.stored(&pipeline()).len(), 2);
    let scanned = scan(store.as_ref(), &pipeline(), load(), FRAME_BYTES)
        .await
        .expect("the log reads");
    assert!(scanned.received.contains(&seq(1)));
    let pending: Vec<_> = scanned
        .pending()
        .map(|logged| logged.meta.clone())
        .collect();
    assert_eq!(pending, [meta(2, &[2])]);
}

#[tokio::test]
async fn a_phase_a_commit_began_scans_back_with_it() {
    let whole = logged_beginning(false, vec![begun()]).await;
    let scanned = scan(whole.as_ref(), &pipeline(), load(), FRAME_BYTES)
        .await
        .expect("the log reads");
    let begun_of: Vec<_> = scanned.commits.iter().map(|logged| &logged.begun).collect();
    assert_eq!(begun_of, [&Vec::new(), &vec![begun()]]);
}

#[tokio::test]
async fn a_chunk_the_log_needs_that_is_missing_is_refused() {
    let whole = logged(false).await;
    let first = Chunk {
        load: load(),
        number: 0,
    };
    whole.remove(&pipeline(), first).await.expect("removes");
    let error = scan(whole.as_ref(), &pipeline(), load(), FRAME_BYTES)
        .await
        .expect_err("chunk 0 holds commit 1 and its batches");
    assert_eq!(error.code(), Some("wal_unreadable"));
}

#[tokio::test]
async fn a_byte_changed_anywhere_in_any_chunk_or_a_chunk_cut_is_refused() {
    let whole = logged(false).await;
    let chunks = whole.stored(&pipeline());
    for (damaged, bytes) in &chunks {
        let len = bytes.len();
        // Every byte of the last chunk, which holds the second commit, and a stride of the first.
        let stride = if damaged.number == 1 { 1 } else { 7 };
        for at in (0..len).step_by(stride) {
            let store = copied(&whole, |chunk, bytes| {
                if chunk == *damaged {
                    bytes[at] ^= 0x01;
                }
            })
            .await;
            let error = scan(&store, &pipeline(), load(), FRAME_BYTES)
                .await
                .expect_err("a published chunk is whole: damage is never a tear");
            assert!(
                matches!(error.code(), Some("wal_unreadable" | "wal_foreign")),
                "chunk {} byte {at}: {error}",
                damaged.number
            );
        }
        for cut in [0, 1, 13, 14, len / 2, len - 1] {
            let store = copied(&whole, |chunk, bytes| {
                if chunk == *damaged {
                    bytes.truncate(cut);
                }
            })
            .await;
            let error = scan(&store, &pipeline(), load(), FRAME_BYTES)
                .await
                .expect_err("a chunk cut is refused");
            assert_eq!(error.code(), Some("wal_unreadable"), "cut at {cut}");
        }
    }
}

#[tokio::test]
async fn a_chunk_of_another_pipeline_or_load_is_refused_as_foreign() {
    let whole = logged(false).await;
    // The log of `orders`, found where `payments`' log, or another load's, is.
    let payments = PipelineId::parse("payments").expect("a valid pipeline");
    let other = LoadId::from_parts(UNIX_EPOCH, 6);
    for (pipeline, load) in [(payments.clone(), load()), (pipeline(), other)] {
        let store = MemoryWal::default();
        for (chunk, bytes) in whole.stored(&self::pipeline()) {
            let chunk = Chunk { load, ..chunk };
            published(&store, &pipeline, chunk, bytes.to_vec()).await;
        }
        let error = scan(&store, &pipeline, load, FRAME_BYTES)
            .await
            .expect_err("not this log's");
        assert_eq!(error.kind(), ErrorKind::Wal);
        assert_eq!(error.code(), Some("wal_foreign"));
        assert!(!error.is_retryable());
    }
}

#[tokio::test]
async fn a_log_of_another_format_is_refused() {
    let whole = logged(false).await;
    let store = copied(&whole, |_, bytes| {
        bytes[8..10].copy_from_slice(&(frame::VERSION - 1).to_le_bytes());
        let check = crc32c::crc32c(&bytes[..10]);
        bytes[10..14].copy_from_slice(&check.to_le_bytes());
    })
    .await;
    let error = scan(&store, &pipeline(), load(), FRAME_BYTES)
        .await
        .expect_err("the log is of the previous format");
    assert_eq!(error.code(), Some("wal_unreadable"));
}

#[tokio::test]
async fn a_batch_read_past_its_frame_is_refused() {
    let store = logged(false).await;
    let scanned = scan(store.as_ref(), &pipeline(), load(), FRAME_BYTES)
        .await
        .expect("the log reads");
    let mut located = scanned.batches[&SegmentId(1)][0];
    batch(store.as_ref(), &pipeline(), located, limits())
        .await
        .expect("the batch reads where the scan found it");
    // A location that runs on into the next frame is not the frame the scan found.
    located.len += 9;
    let error = batch(store.as_ref(), &pipeline(), located, limits())
        .await
        .err()
        .expect("more than the batch's frame");
    assert_eq!(error.code(), Some("wal_unreadable"));
    // Nor is the frame of another batch.
    let mut other = scanned.batches[&SegmentId(1)][0];
    other.ordinal += 1;
    let error = batch(store.as_ref(), &pipeline(), other, limits())
        .await
        .err()
        .expect("another batch");
    assert_eq!(error.code(), Some("wal_unreadable"));
}

/// What a piece reserved of `budget` for its frame in the log.
fn frame(budget: &MemoryBudget) -> rdlt_connector::Permit {
    Box::new(
        budget
            .try_acquire_working(4_096)
            .expect("the budget has room"),
    )
}
