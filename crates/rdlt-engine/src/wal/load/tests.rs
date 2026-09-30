use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use rdlt_connector::{
    CommitMeta, CommitSeq, Cursor, Epoch, LoadId, PartitionId, PartitionState, PipelineId, Receipt,
    SchemaVersion, SegmentId, StreamName,
};

use super::{LoadLog, Sealed};
use crate::budget::MemoryBudget;
use crate::compute::Inline;
use crate::table::TableView;
use crate::table::testing::view;
use crate::wal::frame::{Frame, Frames};
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
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
    }
}

/// Every frame the store holds for the load, in order.
fn frames(store: &MemoryWal) -> Vec<Frame> {
    store
        .stored(&pipeline())
        .iter()
        .flat_map(|(_, stored)| {
            Frames::new(&stored.bytes)
                .map(|frame| frame.expect("the frame decodes").1)
                .collect::<Vec<_>>()
        })
        .collect()
}

/// The view of `table` at `version`.
fn at(table: &TableView, version: u32) -> TableView {
    let mut versioned = table.clone();
    versioned.table.version = SchemaVersion(version);
    versioned
}

#[tokio::test]
async fn each_table_version_is_described_once_before_its_first_batch() {
    let store = Arc::new(MemoryWal::default());
    let wal: Arc<dyn WalStore> = Arc::clone(&store) as Arc<dyn WalStore>;
    let (log, task) = LoadLog::start(wal, pipeline(), load(), None)
        .await
        .expect("the log starts");
    let budget = MemoryBudget::new(1 << 20);
    let (orders, items) = (view("orders"), view("items"));
    let written = async {
        // Two partitions write the first version at once; the table then changes.
        let first = at(&orders, 1);
        let write = |segment| {
            let (log, budget, first) = (log.clone(), budget.clone(), first.clone());
            async move {
                log.batch(&Inline, &budget, 0, &first, SegmentId(segment), &ids(0))
                    .await
                    .expect("the batch is logged");
            }
        };
        tokio::join!(write(0), write(1), write(2), write(3));
        log.batch(&Inline, &budget, 0, &at(&orders, 2), SegmentId(4), &ids(10))
            .await
            .expect("the batch is logged");
        log.batch(&Inline, &budget, 1, &at(&items, 1), SegmentId(4), &ids(20))
            .await
            .expect("the batch is logged");
        log.commit(Vec::new(), &meta(&[0, 1, 2, 3, 4]))
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
    let wal: Arc<dyn WalStore> = Arc::clone(&store) as Arc<dyn WalStore>;
    let (log, task) = LoadLog::start(wal, pipeline(), load(), None)
        .await
        .expect("the log starts");
    let budget = MemoryBudget::new(1 << 20);
    let orders = view("orders");
    let observed = Arc::clone(&store);
    let state = PartitionState::Cursor(
        Cursor::new(1, bytes::Bytes::from_static(b"{\"next\":3}")).expect("a cursor"),
    );
    let receipt = Receipt {
        load_id: load(),
        commit_seq: CommitSeq::FIRST,
        committed_at: UNIX_EPOCH,
        rows: 3,
        bytes: 24,
    };
    let written = async {
        log.batch(&Inline, &budget, 0, &orders, SegmentId(1), &ids(0))
            .await
            .expect("the batch is logged");
        let sealed = vec![Sealed {
            segment: SegmentId(1),
            stream: StreamName::new("orders").expect("a valid stream"),
            partition: PartitionId::parse("p0").expect("a valid partition"),
            replayable: true,
            from: None,
            state: state.clone(),
        }];
        log.commit(sealed, &meta(&[1])).await.expect("durable");
        let kinds: Vec<_> = frames(&observed).into_iter().skip(2).collect();
        let [
            Frame::Batch(batch),
            Frame::Seal(seal),
            Frame::Commit(logged),
        ] = &kinds[..]
        else {
            panic!("a batch, its seal and the commit: {kinds:?}");
        };
        assert_eq!((batch.segment, &batch.batch), (SegmentId(1), &ids(0)));
        assert_eq!((seal.segment, &seal.state), (SegmentId(1), &state));
        assert_eq!(**logged, meta(&[1]));
        log.committed(&receipt).await.expect("logged");
        // Closed with every commit received, the log is gone.
        log.close().await.expect("closed");
    };
    let (ended, ()) = tokio::join!(task, written);
    ended.expect("the writer ends");
    assert!(frames(&store).is_empty());
}

#[tokio::test]
async fn a_load_holds_its_log_until_it_ends() {
    let store = Arc::new(MemoryWal::default());
    let wal: Arc<dyn WalStore> = Arc::clone(&store) as Arc<dyn WalStore>;
    let (log, task) = LoadLog::start(Arc::clone(&wal), pipeline(), load(), None)
        .await
        .expect("the log starts");
    assert!(
        LoadLog::start(Arc::clone(&wal), pipeline(), load(), None)
            .await
            .is_err(),
        "a second writer of one log is refused"
    );
    let replayer = wal.claim(&pipeline(), load()).await.expect("claims");
    assert!(replayer.is_none(), "a running load's log is not replayed");
    drop(log);
    task.await.expect("the writer ends");
    let replayer = wal.claim(&pipeline(), load()).await.expect("claims");
    assert!(replayer.is_some(), "an ended load's log is free");
}
