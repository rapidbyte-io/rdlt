use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use rdlt_connector::{
    CommitMeta, CommitSeq, Cursor, Epoch, LoadId, PartitionId, PartitionState, PipelineId, Receipt,
    SchemaVersion, SegmentId, StreamName,
};

use super::{LoadLog, Owner, Sealed};
use crate::budget::MemoryBudget;
use crate::compute::Inline;
use crate::table::TableView;
use crate::table::testing::view;
use crate::wal::frame::{self, Frame};
use crate::wal::memory::MemoryWal;
use crate::wal::store::WalStore;

fn pipeline() -> PipelineId {
    PipelineId::parse("orders").expect("a valid pipeline")
}

fn load() -> LoadId {
    LoadId::from_parts(UNIX_EPOCH, 9)
}

fn ids(from: i64) -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(from..from + 3));
    RecordBatch::try_from_iter([("id", ids)]).expect("a valid batch")
}

fn meta(segments: &[u64]) -> CommitMeta {
    CommitMeta {
        load_id: load(),
        commit_seq: CommitSeq::FIRST,
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

/// Every frame the store holds for the load, in order.
fn frames(store: &MemoryWal) -> Vec<Frame> {
    store
        .stored(&pipeline())
        .iter()
        .flat_map(|(_, bytes)| frame::frames(bytes, frame::limits(1 << 30)).expect("it reads"))
        .collect()
}

/// The log of the load in `store`, and its writer's task.
fn start(
    store: &Arc<MemoryWal>,
) -> (
    LoadLog,
    impl Future<Output = Result<(), crate::Error>> + Send + 'static,
) {
    store.open(&pipeline(), load());
    let wal: Arc<dyn WalStore> = Arc::clone(store) as Arc<dyn WalStore>;
    let owner = Owner {
        pipeline: pipeline(),
        load: load(),
        epoch: Epoch(1),
        opened: None,
        origin: load(),
    };
    LoadLog::start(wal, owner, std::num::NonZeroU64::MAX, None)
}

/// The view of `table` at `version`.
fn at(table: &TableView, version: u32) -> Arc<TableView> {
    let mut versioned = table.clone();
    versioned.table.version = SchemaVersion(version);
    Arc::new(versioned)
}

#[tokio::test]
async fn each_table_version_is_described_once_before_its_first_batch() {
    let store = Arc::new(MemoryWal::default());
    let (log, task) = start(&store);
    let budget = MemoryBudget::new(1 << 20);
    let (orders, items) = (view("orders"), view("items"));
    let written = async {
        // Two partitions write the first version at once; the table then changes.
        let first = at(&orders, 1);
        let write = |segment| {
            let (log, budget, first) = (log.clone(), budget.clone(), first.clone());
            async move {
                logged(&log, &budget, 0, &first, SegmentId(segment), &ids(0))
                    .await
                    .expect("the batch is logged");
            }
        };
        tokio::join!(write(0), write(1), write(2), write(3));
        logged(&log, &budget, 0, &at(&orders, 2), SegmentId(4), &ids(10))
            .await
            .expect("the batch is logged");
        logged(&log, &budget, 1, &at(&items, 1), SegmentId(4), &ids(20))
            .await
            .expect("the batch is logged");
        log.commit(&budget, Vec::new(), Vec::new(), &meta(&[0, 1, 2, 3, 4]), 0)
            .await
            .expect("the commit is durable");
        drop(log);
    };
    let (ended, ()) = tokio::join!(task, written);
    ended.expect("the writer ends");
    let mut described = Vec::new();
    for frame in frames(&store) {
        match frame {
            Frame::Schema(table) => {
                assert!(!described.iter().any(|(index, _)| *index == table.index));
                described.push((table.index, (table.table.name.clone(), table.table.version)));
            }
            Frame::Batch(batch) => assert!(
                described.iter().any(|(index, _)| *index == batch.table),
                "batch of table {} before its schema",
                batch.table
            ),
            _ => {}
        }
    }
    let names: Vec<_> = described.into_iter().map(|(_, named)| named).collect();
    assert_eq!(
        names,
        [
            ("orders".into(), SchemaVersion(1)),
            ("orders".into(), SchemaVersion(2)),
            ("items".into(), SchemaVersion(1)),
        ]
    );
    assert_eq!(
        budget.reserved(),
        0,
        "each frame's bytes are released once appended"
    );
}

#[tokio::test]
async fn a_logged_load_reads_back_as_it_was_written() {
    let store = Arc::new(MemoryWal::default());
    let (log, task) = start(&store);
    let budget = MemoryBudget::new(1 << 20);
    let orders = view("orders");
    let observed = Arc::clone(&store);
    let state = PartitionState::Cursor(Cursor::new(1, b"{\"next\":3}").expect("a cursor"));
    let receipt = Receipt {
        load_id: load(),
        commit_seq: CommitSeq::FIRST,
        committed_at: UNIX_EPOCH,
        rows: 3,
        bytes: 24,
    };
    let written = async {
        for from in [0, 3] {
            logged(&log, &budget, 0, &orders, SegmentId(1), &ids(from))
                .await
                .expect("the batch is logged");
        }
        let sealed = vec![Sealed {
            segment: SegmentId(1),
            stream: StreamName::new("orders").expect("a valid stream"),
            partition: PartitionId::parse("p0").expect("a valid partition"),
            replayable: true,
            phase: 0,
            from: None,
            state: state.clone(),
        }];
        log.commit(&budget, sealed, Vec::new(), &meta(&[1]), 0)
            .await
            .expect("durable");
        let kinds: Vec<_> = frames(&observed).into_iter().skip(2).collect();
        let [
            Frame::Batch(first),
            Frame::Batch(second),
            Frame::Seal(seal),
            Frame::Commit(logged),
            Frame::End(_),
        ] = &kinds[..]
        else {
            panic!("batches, a seal, the commit and the end: {kinds:?}");
        };
        assert_eq!((first.segment, &first.batch), (SegmentId(1), &ids(0)));
        assert_eq!((second.segment, &second.batch), (SegmentId(1), &ids(3)));
        assert_eq!((seal.segment, &seal.state), (SegmentId(1), &state));
        assert_eq!((seal.batches, seal.rows), (2, 6));
        assert_eq!(
            (logged.meta.clone(), logged.seals, logged.phases),
            (meta(&[1]), 1, 0)
        );
        log.committed(&receipt).await.expect("logged");
        // Closed with every commit received, the log is gone.
        log.close().await.expect("closed");
    };
    let (ended, ()) = tokio::join!(task, written);
    ended.expect("the writer ends");
    assert!(frames(&store).is_empty());
}

#[tokio::test]
async fn a_second_writer_of_one_log_is_fenced_at_its_first_commit() {
    let store = Arc::new(MemoryWal::default());
    let (first, first_task) = start(&store);
    let (second, second_task) = start(&store);
    let budget = MemoryBudget::new(1 << 20);
    let written = async {
        first
            .commit(&budget, Vec::new(), Vec::new(), &meta(&[]), 0)
            .await
            .expect("durable");
        let refused = second
            .commit(&budget, Vec::new(), Vec::new(), &meta(&[]), 0)
            .await
            .expect_err("the chunk is taken");
        assert_eq!(refused.code(), Some("wal_fenced"));
        drop((first, second));
    };
    let (first_ended, second_ended, ()) = tokio::join!(first_task, second_task, written);
    first_ended.expect("the writer ends");
    second_ended.expect("the writer ends");
}

/// A seal of `segment` of partition `p0`, which cannot read again.
fn sealed_at(segment: u64) -> Sealed {
    Sealed {
        segment: SegmentId(segment),
        stream: StreamName::new("orders").expect("a valid stream"),
        partition: PartitionId::parse("p0").expect("a valid partition"),
        replayable: false,
        phase: 0,
        from: None,
        state: PartitionState::Done,
    }
}

#[tokio::test]
async fn a_long_load_keeps_only_the_chunks_its_receipts_do_not_cover_empty_segments_or_not() {
    let store = Arc::new(MemoryWal::default());
    let (log, task) = start(&store);
    let budget = MemoryBudget::new(1 << 20);
    let orders = view("orders");
    let observed = Arc::clone(&store);
    let written = async {
        let mut seq = CommitSeq::FIRST;
        for round in 0..5_u64 {
            let (full, empty) = (10 * round + 1, 10 * round + 2);
            logged(&log, &budget, 0, &orders, SegmentId(full), &ids(0))
                .await
                .expect("the batch is logged");
            // An idle partition seals an empty segment, which no commit publishes.
            let mut commit = meta(&[full]);
            commit.commit_seq = seq;
            log.commit(
                &budget,
                vec![sealed_at(full), sealed_at(empty)],
                Vec::new(),
                &commit,
                0,
            )
            .await
            .expect("durable");
            let receipt = Receipt {
                load_id: load(),
                commit_seq: seq,
                committed_at: UNIX_EPOCH,
                rows: 3,
                bytes: 24,
            };
            log.committed(&receipt).await.expect("logged");
            seq = seq.next();
        }
        // Every commit has its receipt: at most the chunk the last receipt went to is left.
        let kept = observed.stored(&pipeline()).len();
        assert!(kept <= 1, "{kept} chunks kept");
        drop(log);
    };
    let (ended, ()) = tokio::join!(task, written);
    ended.expect("the writer ends");
}

#[tokio::test]
async fn seal_and_commit_frames_are_charged_until_they_are_appended() {
    let store = Arc::new(MemoryWal::default());
    let (log, task) = start(&store);
    let observed = Arc::clone(&store);
    let written = async move {
        // A seal whose cursor makes its frame far larger than the commit's.
        let cursor = Cursor::new(1, &[b'c'; 10_000]).expect("a cursor");
        let large = Sealed {
            state: PartitionState::Cursor(cursor),
            ..sealed_at(1)
        };
        let sealing = MemoryBudget::new(1 << 20);
        log.commit(&sealing, vec![large], Vec::new(), &meta(&[1]), 0)
            .await
            .expect("durable");
        let lengths: Vec<u64> = frames(&observed)
            .iter()
            .filter(|frame| matches!(frame, Frame::Seal(_) | Frame::Commit(_)))
            .map(|frame| frame.encode().expect("the frame encodes").len() as u64)
            .collect();
        let [seal, commit] = lengths[..] else {
            panic!("a seal and the commit: {lengths:?}");
        };
        assert!(seal > commit && seal > 10_000);
        // It was charged before it was encoded, for its cursor twice over and what a frame takes
        // beside, which is more than the frame.
        assert!(seal < 20_000, "{seal}");
        assert_eq!(sealing.peak(), 2 * 10_000 + 4_096);
        assert_eq!(sealing.reserved(), 0, "released once appended");
        // A commit of no seals is charged what its frame may take, more than it takes.
        let committing = MemoryBudget::new(1 << 20);
        let mut second = meta(&[2]);
        second.commit_seq = CommitSeq::FIRST.next();
        log.commit(&committing, Vec::new(), Vec::new(), &second, 0)
            .await
            .expect("durable");
        assert_eq!(committing.peak(), super::commit_bytes(&[], &second));
        assert!(committing.peak() >= commit);
        assert_eq!(committing.reserved(), 0);
        drop(log);
    };
    let (ended, ()) = tokio::join!(task, written);
    ended.expect("the writer ends");
}

#[tokio::test]
async fn a_tables_schema_frame_is_charged_from_the_log_until_it_is_appended() {
    let store = Arc::new(MemoryWal::default());
    let (log, task) = start(&store);
    let budget = MemoryBudget::new(1 << 20);
    let orders = at(&view("orders"), 1);
    let written = async {
        logged(&log, &budget, 0, &orders, SegmentId(0), &ids(0))
            .await
            .expect("the batch is logged");
        drop(log);
    };
    let (ended, ()) = tokio::join!(task, written);
    ended.expect("the writer ends");
    // The schema's frame, held beside the batch's until the log appended it.
    let described = super::described(&orders.model.schema());
    assert!(described >= 4_096);
    assert!(budget.peak() >= 4_096 + described, "{}", budget.peak());
    assert_eq!(budget.reserved(), 0);
}

/// Logs `batch` of `segment`, lowered for `view` of table `table`, its frame held by `budget`.
async fn logged(
    log: &LoadLog,
    budget: &MemoryBudget,
    table: usize,
    view: &Arc<TableView>,
    segment: SegmentId,
    batch: &RecordBatch,
) -> Result<(), crate::Error> {
    let held = frame(budget);
    log.batch(&Inline, budget, held, (table, view), segment, batch)
        .await
}

/// What a piece reserved of `budget` for its frame in the log.
fn frame(budget: &MemoryBudget) -> rdlt_connector::Permit {
    Box::new(
        budget
            .try_acquire_working(4_096)
            .expect("the budget has room"),
    )
}

#[tokio::test]
async fn a_commit_frame_is_charged_for_the_state_it_records_and_refused_beyond_the_log_s_share() {
    let store = Arc::new(MemoryWal::default());
    let (log, task) = start(&store);
    let written = async move {
        // One that records state is charged for it before it is encoded, twice over, which is
        // more than its frame takes.
        let mut third = meta(&[3]);
        third.commit_seq = CommitSeq::FIRST;
        third.state_delta = vec![rdlt_connector::StateChange::Put(
            rdlt_connector::StateRecord {
                key: "k".repeat(1_000),
                value: vec![7; 9_000].into(),
            },
        )];
        let begun = vec![frame::BegunPhase {
            stream: StreamName::new("orders").expect("a valid stream"),
            phase: 1,
            changes: vec![rdlt_connector::StateChange::Delete("d".repeat(5_000))],
        }];
        let recording = MemoryBudget::new(1 << 20);
        let estimate = super::commit_bytes(&begun, &third);
        // Each record twice over and what a record takes beside, a segment, and two frames.
        assert_eq!(
            estimate,
            2 * 10_000 + 64 + 2 * 5_000 + 64 + 1_024 + 2 * 4_096
        );
        log.commit(&recording, Vec::new(), begun, &third, 0)
            .await
            .expect("durable");
        // Never more than was reserved: what the frames take is within the estimate.
        assert_eq!(recording.peak(), estimate);
        assert_eq!(recording.reserved(), 0);
        // What a commit records of tables, which their changes reserved, it does not again.
        let mut prepaid = third.clone();
        prepaid.commit_seq = third.commit_seq.next();
        let paid = MemoryBudget::new(1 << 20);
        log.commit(&paid, Vec::new(), Vec::new(), &prepaid, 20_000)
            .await
            .expect("durable");
        assert_eq!(paid.peak(), super::commit_bytes(&[], &prepaid) - 20_000);
        // A frame beyond the log's share of the budget is refused, and nothing is reserved.
        let small = MemoryBudget::new(64_000);
        let mut fourth = third.clone();
        fourth.commit_seq = prepaid.commit_seq.next();
        let refused = log.commit(&small, Vec::new(), Vec::new(), &fourth, 0).await;
        let refused = refused.expect_err("the frame passes the log's share");
        assert_eq!(
            (refused.kind(), refused.code()),
            (crate::ErrorKind::Wal, Some("log_frame_exceeds_budget"))
        );
        assert_eq!((small.reserved(), small.peak()), (0, 0));
        drop(log);
    };
    let (ended, ()) = tokio::join!(task, written);
    ended.expect("the writer ends");
}

#[tokio::test]
async fn a_frame_keeps_what_it_takes_of_its_reservation_and_reserves_what_it_takes_beyond() {
    let budget = MemoryBudget::new(1 << 20);
    let settle = |frame: usize| {
        let budget = budget.clone();
        async move {
            let held = budget.acquire_log(100).await.unwrap();
            super::settled(&budget, held, frame).await.unwrap()
        }
    };
    // A byte under, at and a byte over what was reserved; then far beyond it.
    for (frame, kept, more) in [(99, 99, None), (100, 100, None), (101, 100, Some(1))] {
        let (held, extra) = settle(frame).await;
        assert_eq!(
            (
                held.bytes(),
                extra.as_ref().map(crate::budget::Reservation::bytes)
            ),
            (kept, more),
            "a frame of {frame}"
        );
        assert_eq!(budget.reserved(), u64::try_from(frame).unwrap());
    }
    let (held, extra) = settle(350).await;
    assert_eq!(budget.reserved(), 350);
    assert_eq!(
        (held.bytes(), extra.map(|more| more.bytes())),
        (100, Some(250))
    );
    drop(held);
    assert_eq!(budget.reserved(), 0);
}

#[tokio::test]
async fn a_superseded_schema_frame_is_forgotten_once_no_batch_of_it_can_follow() {
    let store = Arc::new(MemoryWal::default());
    let (log, task) = start(&store);
    let budget = MemoryBudget::new(1 << 20);
    let orders = view("orders");
    let described = Arc::clone(&log.tables);
    let written = async {
        let current = at(&orders, 200);
        for version in 1..200_u32 {
            // Each version's view goes once its batch is logged, as a partition's plan does.
            let superseded = at(&orders, version);
            logged(
                &log,
                &budget,
                0,
                &superseded,
                SegmentId(u64::from(version)),
                &ids(0),
            )
            .await
            .expect("the batch is logged");
        }
        logged(&log, &budget, 0, &current, SegmentId(200), &ids(0))
            .await
            .expect("the batch is logged");
        log.commit(&budget, Vec::new(), Vec::new(), &meta(&[200]), 0)
            .await
            .expect("the commit is durable");
        // Only the version whose view is still alive is still described.
        let left: Vec<u32> = described
            .lock()
            .await
            .indexes
            .values()
            .map(|(index, _)| *index)
            .collect();
        assert_eq!(left, [199]);
        // A batch of a forgotten version is described again, under an index of its own.
        logged(&log, &budget, 0, &at(&orders, 1), SegmentId(201), &ids(0))
            .await
            .expect("the batch is logged");
        let mut next = meta(&[201]);
        next.commit_seq = CommitSeq::FIRST.next();
        log.commit(&budget, Vec::new(), Vec::new(), &next, 0)
            .await
            .expect("the commit is durable");
        drop(current);
        drop(log);
    };
    let (ended, ()) = tokio::join!(task, written);
    ended.expect("the writer ends");
    let indexes: Vec<(u32, SchemaVersion)> = frames(&store)
        .into_iter()
        .filter_map(|frame| match frame {
            Frame::Schema(table) => Some((table.index, table.table.version)),
            _ => None,
        })
        .collect();
    assert_eq!(indexes.last(), Some(&(200, SchemaVersion(1))));
}

#[tokio::test]
async fn a_version_stays_described_while_any_view_of_it_lives() {
    let store = Arc::new(MemoryWal::default());
    let (log, task) = start(&store);
    let budget = MemoryBudget::new(1 << 20);
    let orders = view("orders");
    let described = Arc::clone(&log.tables);
    let written = async {
        // Two views of one version, as a resolution that only rounds a column makes: the first
        // describes it, the second finds it described.
        let first = at(&orders, 1);
        let second = at(&orders, 1);
        logged(&log, &budget, 0, &first, SegmentId(1), &ids(0))
            .await
            .expect("the batch is logged");
        logged(&log, &budget, 0, &second, SegmentId(2), &ids(0))
            .await
            .expect("the batch is logged");
        drop(first);
        log.commit(&budget, Vec::new(), Vec::new(), &meta(&[1, 2]), 0)
            .await
            .expect("the commit is durable");
        // The second view lives: its version keeps its index, and a batch of it in flight finds
        // its schema in the chunk after the commit.
        let left: Vec<u32> = described
            .lock()
            .await
            .indexes
            .values()
            .map(|(index, _)| *index)
            .collect();
        assert_eq!(left, [0]);
        logged(&log, &budget, 0, &second, SegmentId(3), &ids(0))
            .await
            .expect("the batch is logged");
        // A view is noted once, however many of its batches are logged.
        let views: Vec<usize> = described
            .lock()
            .await
            .indexes
            .values()
            .map(|(_, views)| views.len())
            .collect();
        assert_eq!(views, [2]);
        log.commit(&budget, Vec::new(), Vec::new(), &meta(&[3]), 0)
            .await
            .expect("the commit is durable");
        drop(second);
        drop(log);
    };
    let (ended, ()) = tokio::join!(task, written);
    ended.expect("the writer ends");
    let schemas = frames(&store)
        .into_iter()
        .filter(|frame| matches!(frame, Frame::Schema(_)))
        .count();
    // Once in each chunk that holds its batches, under one index.
    assert_eq!(schemas, 2);
}

#[tokio::test]
async fn a_batch_that_would_take_the_log_past_what_it_may_hold_is_refused() {
    let store = Arc::new(MemoryWal::default());
    store.open(&pipeline(), load());
    let wal: Arc<dyn WalStore> = Arc::clone(&store) as Arc<dyn WalStore>;
    let owner = Owner {
        pipeline: pipeline(),
        load: load(),
        epoch: Epoch(1),
        opened: None,
        origin: load(),
    };
    let limit = 4_000;
    let (log, task) = LoadLog::start(wal, owner, std::num::NonZeroU64::new(limit).unwrap(), None);
    let budget = MemoryBudget::new(1 << 20);
    let orders = view("orders");
    let written = async {
        let mut refused = None;
        for segment in 0..100 {
            if let Err(error) = logged(&log, &budget, 0, &orders, SegmentId(segment), &ids(0)).await
            {
                refused = Some((segment, error));
                break;
            }
            assert!(log.held() <= limit, "{} bytes", log.held());
        }
        let (segment, error) = refused.expect("the log fills");
        assert!(segment > 2, "refused at batch {segment}");
        assert_eq!(error.code(), Some("log_bytes_exceeded"));
        assert!(!error.is_retryable());
        assert!(log.held() <= limit);
        drop(log);
    };
    let (ended, ()) = tokio::join!(task, written);
    ended.expect("the writer ends");
}

#[tokio::test]
async fn a_log_makes_a_commit_due_at_half_what_it_may_hold_and_at_each_eighth_after() {
    let store = Arc::new(MemoryWal::default());
    store.open(&pipeline(), load());
    let wal: Arc<dyn WalStore> = Arc::clone(&store) as Arc<dyn WalStore>;
    let owner = Owner {
        pipeline: pipeline(),
        load: load(),
        epoch: Epoch(1),
        opened: None,
        origin: load(),
    };
    let limit = 80_000;
    let (log, task) = LoadLog::start(wal, owner, std::num::NonZeroU64::new(limit).unwrap(), None);
    let budget = MemoryBudget::new(1 << 20);
    let orders = view("orders");
    let written = async {
        let segments = std::sync::atomic::AtomicU64::new(0);
        let fill = |until: u64| {
            let (log, budget, orders, segments) = (&log, &budget, &orders, &segments);
            async move {
                while log.held() < until {
                    let segment = segments.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    logged(log, budget, 0, orders, SegmentId(segment), &ids(0))
                        .await
                        .expect("logged");
                }
            }
        };
        fill(limit / 2 - 1_000).await;
        assert!(!log.due(), "{} bytes", log.held());
        fill(limit / 2).await;
        assert!(log.due());
        // A commit that lets nothing go makes the next due an eighth later.
        log.passed();
        assert!(!log.due());
        let held = log.held();
        fill(held + limit / 8 - 1_000).await;
        assert!(!log.due());
        fill(held + limit / 8).await;
        assert!(log.due());
        drop(log);
    };
    let (ended, ()) = tokio::join!(task, written);
    ended.expect("the writer ends");
}

#[tokio::test]
async fn a_failed_write_fails_the_batches_after_it_at_once() {
    let store = Arc::new(MemoryWal::default());
    let (log, task) = start(&store);
    let budget = MemoryBudget::new(1 << 20);
    let orders = view("orders");
    let failing = Arc::clone(&store);
    let written = async {
        *failing.failing.lock() = true;
        // The writer fails the first batch it writes, and the batches logged after it hear so.
        let mut refused = None;
        for segment in 0..64 {
            if let Err(error) = logged(&log, &budget, 0, &orders, SegmentId(segment), &ids(0)).await
            {
                refused = Some(error);
                break;
            }
            tokio::task::yield_now().await;
        }
        let error = refused.expect("a batch after the failure is refused");
        assert_eq!(error.kind(), crate::ErrorKind::Wal);
        drop(log);
    };
    let (ended, ()) = tokio::join!(task, written);
    ended.expect("the writer ends");
}

#[tokio::test]
async fn what_was_counted_of_a_segment_goes_with_its_seal_or_its_abandonment() {
    let store = Arc::new(MemoryWal::default());
    let (log, task) = start(&store);
    let budget = MemoryBudget::new(1 << 20);
    let orders = view("orders");
    let written = async {
        for segment in [1, 2] {
            logged(&log, &budget, 0, &orders, SegmentId(segment), &ids(0))
                .await
                .expect("the batch is logged");
        }
        assert_eq!(log.counts.lock().len(), 2);
        log.abandon(SegmentId(2)).await.expect("abandoned");
        let sealed = vec![Sealed {
            segment: SegmentId(1),
            stream: StreamName::new("orders").expect("a valid stream"),
            partition: PartitionId::parse("p0").expect("a valid partition"),
            replayable: true,
            phase: 0,
            from: None,
            state: PartitionState::Done,
        }];
        log.commit(&budget, sealed, Vec::new(), &meta(&[1]), 0)
            .await
            .expect("durable");
        assert!(log.counts.lock().is_empty());
        drop(log);
    };
    let (ended, ()) = tokio::join!(task, written);
    ended.expect("the writer ends");
}

#[tokio::test]
async fn a_batch_counts_as_unpublished_until_its_chunk_is_published() {
    let store = Arc::new(MemoryWal::default());
    let (log, task) = start(&store);
    let budget = MemoryBudget::new(1 << 20);
    let orders = view("orders");
    let shared = Arc::clone(log.writer.shared());
    let unpublished = || {
        shared
            .unpublished
            .load(std::sync::atomic::Ordering::Relaxed)
    };
    let written = async {
        for from in [0, 3] {
            logged(&log, &budget, 0, &orders, SegmentId(1), &ids(from))
                .await
                .expect("the batch is logged");
        }
        assert!(unpublished() > 0, "the batches wait for their chunk");
        log.commit(&budget, vec![sealed_at(1)], Vec::new(), &meta(&[1]), 0)
            .await
            .expect("durable");
        assert_eq!(unpublished(), 0, "the chunk holding them is published");
        drop(log);
    };
    let (ended, ()) = tokio::join!(task, written);
    ended.expect("the writer ends");
}

#[tokio::test(start_paused = true)]
async fn a_batch_finding_the_log_full_waits_only_while_a_commit_can_free_room() {
    let store = Arc::new(MemoryWal::default());
    store.open(&pipeline(), load());
    let wal: Arc<dyn WalStore> = Arc::clone(&store) as Arc<dyn WalStore>;
    let owner = Owner {
        pipeline: pipeline(),
        load: load(),
        epoch: Epoch(1),
        opened: None,
        origin: load(),
    };
    let limit = std::num::NonZeroU64::new(4_000).unwrap();
    let (log, task) = LoadLog::start(wal, owner, limit, None);
    let budget = MemoryBudget::new(1 << 20);
    let orders = view("orders");
    let written = async {
        // A checkpoint a commit took leaves nothing for the next commit to free.
        log.checkpointed();
        log.took(1);
        let mut segment = 0;
        while logged(&log, &budget, 0, &orders, SegmentId(segment), &ids(0))
            .await
            .is_ok()
        {
            segment += 1;
        }
        // A commit under way may free room: the next batch waits for it, and once it ended
        // having freed none, with no checkpoint left to take, the batch is refused.
        assert!(!log.waits(), "a batch refused waits for nothing");
        let committing = log.committing();
        let batch = ids(0);
        let waiting = logged(&log, &budget, 0, &orders, SegmentId(segment), &batch);
        let ended = async {
            while !log.waits() {
                tokio::task::yield_now().await;
            }
            drop(committing);
        };
        let (refused, ()) = tokio::join!(waiting, ended);
        let error = refused.expect_err("no room");
        assert_eq!(error.code(), Some("log_bytes_exceeded"));
        assert!(!log.waits());
        drop(log);
    };
    let deadline =
        crate::env::Clock::sleep(&crate::env::SystemClock, std::time::Duration::from_secs(60));
    tokio::select! {
        biased;
        (ended, ()) = async { tokio::join!(task, written) } => ended.expect("the writer ends"),
        () = deadline => panic!("a batch waits for a commit that cannot free room"),
    }
}
