mod abandoned;
mod carried;
mod relieved;
mod taken;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field as ArrowField, Schema};
use bytes::Bytes;
use rdlt_connector::{
    CommitMeta, CommitSeq, Epoch, Field, LoadId, LogicalType, PartitionId, PartitionState,
    PipelineId, SchemaVersion, SegmentId, SegmentSet, StreamName, TablePath, TableRef, TableSchema,
};
use tokio::sync::oneshot;

use super::super::frame::{Batch, Committing, End, Frame, Seal, Table};
use super::super::memory::MemoryWal;
use super::super::store::WalStore;
use super::{Command, Owner, WalWriter};
use crate::budget::MemoryBudget;
use crate::error::{Error, ErrorKind};

fn pipeline() -> PipelineId {
    PipelineId::parse("orders").expect("a valid pipeline")
}

fn load() -> LoadId {
    LoadId::from_parts(UNIX_EPOCH, 7)
}

fn owner() -> Owner {
    Owner {
        pipeline: pipeline(),
        load: load(),
        epoch: Epoch(1),
        opened: None,
        origin: load(),
    }
}

fn encoded(frame: &Frame) -> Bytes {
    frame.encode().expect("the frame encodes")
}

/// Bytes: what `frame` takes.
fn length(frame: &Bytes) -> u64 {
    u64::try_from(frame.len()).expect("a length")
}

fn table(index: u32) -> Command {
    let schema = TableSchema::new(vec![Field::new("id", LogicalType::Int64, false)])
        .expect("a valid schema");
    let table = TableRef {
        path: TablePath::new([format!("t{index}")]).expect("a valid path"),
        name: format!("t{index}").into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    };
    let frame = encoded(&Frame::Schema(Table {
        index,
        table,
        schema,
    }));
    Command::Table {
        index,
        frame,
        held: Box::new(()),
    }
}

fn seq(number: u64) -> CommitSeq {
    (1..number).fold(CommitSeq::FIRST, |seq, _| seq.next())
}

fn segments(ids: &[u64]) -> SegmentSet {
    ids.iter().copied().map(SegmentId).collect()
}

/// A writer driven as a load drives one, counting the batches it logs of each segment, each of
/// one row, so its seals count them.
struct Driving {
    writer: WalWriter,
    logged: BTreeMap<u64, u64>,
    /// The ordinal the next batch takes.
    ordinal: u64,
    /// The bytes of each table's schema frame, by index.
    schemas: parking_lot::Mutex<BTreeMap<u32, u64>>,
}

impl Driving {
    fn new(writer: WalWriter) -> Self {
        Self {
            writer,
            logged: BTreeMap::new(),
            ordinal: 0,
            schemas: parking_lot::Mutex::default(),
        }
    }

    async fn send(&self, command: Command) {
        if let Command::Table { index, frame, .. } = &command {
            self.schemas.lock().insert(*index, length(frame));
        }
        self.writer.send(command).await.expect("the writer runs");
    }

    /// Counts `bytes` of frames on disk, as a load's log does before it sends them.
    fn count(&self, bytes: u64) {
        let held = &self.writer.shared().held;
        held.fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
    }

    /// Logs a batch of one row of `segment` for the table at `table`.
    async fn batch(&mut self, segment: u64, table: u32) {
        let schema = Schema::new(vec![ArrowField::new("id", DataType::Int64, false)]);
        let row = i64::try_from(segment).unwrap_or(0);
        let rows = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(vec![row]))],
        )
        .expect("a valid batch");
        let frame = encoded(&Frame::Batch(Batch {
            segment: SegmentId(segment),
            table,
            ordinal: self.ordinal,
            batch: rows,
        }));
        self.ordinal += 1;
        *self.logged.entry(segment).or_default() += 1;
        // Counted with its table's schema frame, as a load's log counts a batch it admits.
        let schema = self.schemas.lock().get(&table).copied().unwrap_or(0);
        self.count(length(&frame) + schema);
        let segment = SegmentId(segment);
        self.send(Command::Batch {
            segment,
            table,
            frame,
            schema,
            held: Box::new(()),
        })
        .await;
    }

    /// Seals `segment` with what was logged of it.
    async fn seal(&mut self, segment: u64) {
        let logged = self.logged.remove(&segment).unwrap_or_default();
        let seal = Frame::Seal(Seal {
            segment: SegmentId(segment),
            stream: StreamName::new("orders").expect("a name"),
            partition: PartitionId::parse(format!("p{segment}")).expect("an id"),
            replayable: true,
            phase: 0,
            from: None,
            state: PartitionState::Done,
            batches: logged,
            rows: logged,
        });
        let frame = encoded(&seal);
        self.count(length(&frame));
        self.send(Command::Seal {
            segment: SegmentId(segment),
            frame,
            held: Box::new(()),
        })
        .await;
    }

    /// Seals `ids` and logs commit `number` of them, which is answered once its chunk is
    /// published.
    async fn commit(&mut self, number: u64, ids: &[u64]) -> Result<(), Error> {
        for id in ids {
            self.seal(*id).await;
        }
        let meta = CommitMeta {
            abandoned: SegmentSet::new(),
            load_id: load(),
            commit_seq: seq(number),
            epoch: Epoch(1),
            segments: segments(ids),
            state_delta: Vec::new(),
            finish_generations: Vec::new(),
            child_tables: Vec::new(),
            drop_tables: Vec::new(),
            horizon: None,
        };
        let committing = Committing {
            meta,
            seals: u32::try_from(ids.len()).expect("few"),
            phases: 0,
        };
        let (durable, answer) = oneshot::channel();
        let frame = encoded(&Frame::Commit(Box::new(committing)));
        self.count(length(&frame));
        self.send(Command::Commit {
            seq: seq(number),
            segments: segments(ids),
            frame,
            held: Box::new(()),
            durable,
        })
        .await;
        answer.await.expect("the writer answers")
    }

    async fn committed(&self, number: u64) {
        self.send(Command::Committed { seq: seq(number) }).await;
    }

    async fn close(&self) -> Result<(), Error> {
        let (done, answer) = oneshot::channel();
        self.send(Command::Close { done }).await;
        answer.await.expect("the writer answers")
    }
}

/// A frame's kind, and the segment or table it names.
fn kind(frame: &Frame) -> String {
    match frame {
        Frame::Header(header) => format!("header {}", header.chunk),
        Frame::Schema(table) => format!("schema {}", table.index),
        Frame::Batch(batch) => format!("batch {} of {}", batch.segment.0, batch.table),
        Frame::Seal(seal) => format!("seal {}", seal.segment.0),
        Frame::Begun(begun) => format!("phase {} of {}", begun.phase, begun.stream),
        Frame::Commit(commit) => format!("commit of {:?}", commit.meta.segments),
        Frame::Closed => "closed".to_owned(),
        Frame::Relieved => "relieved".to_owned(),
        Frame::End(end) => format!("end {:?} {:?}", end.live, end.received),
        Frame::Fence(fence) => format!("fence {}", fence.chunk),
    }
}

/// The frames of each chunk `store` holds of the load, by number.
fn frames(store: &MemoryWal) -> Vec<(u64, Vec<Frame>)> {
    let limits = super::super::frame::limits(1 << 30);
    store
        .stored(&pipeline())
        .into_iter()
        .map(|(chunk, bytes)| {
            let frames = super::super::frame::frames(&bytes, limits).expect("the chunk reads");
            (chunk.number, frames)
        })
        .collect()
}

/// Each chunk the store holds for the load, by number, as the kinds of its frames.
fn chunks(store: &MemoryWal) -> Vec<(u64, Vec<String>)> {
    frames(store)
        .into_iter()
        .map(|(number, frames)| (number, frames.iter().map(kind).collect()))
        .collect()
}

/// The numbers of the chunks the store holds for the load.
fn numbers(store: &MemoryWal) -> Vec<u64> {
    chunks(store)
        .into_iter()
        .map(|(number, _)| number)
        .collect()
}

/// Runs a writer of `store` while `drive` sends it commands, until both end.
async fn drive<F, Fut>(store: Arc<MemoryWal>, drive: F) -> Result<(), Error>
where
    F: FnOnce(Driving) -> Fut,
    Fut: Future<Output = ()>,
{
    driven(store, MemoryBudget::new(1 << 20), drive).await
}

/// Runs a writer of `store` holding what it reads back in `budget` while `drive` sends it
/// commands, until both end.
async fn driven<F, Fut>(store: Arc<MemoryWal>, budget: MemoryBudget, drive: F) -> Result<(), Error>
where
    F: FnOnce(Driving) -> Fut,
    Fut: Future<Output = ()>,
{
    store.open(&pipeline(), load());
    let wal: Arc<dyn WalStore> = store;
    let (writer, task) = WalWriter::start(wal, owner(), budget);
    let (ended, ()) = tokio::join!(task, drive(Driving::new(writer)));
    ended
}

#[tokio::test]
async fn each_chunk_starts_with_its_header_and_the_schemas_its_batches_name() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        let commit = |ids: &[u64]| format!("commit of {:?}", segments(ids));
        for command in [table(0), table(1)] {
            log.send(command).await;
        }
        log.batch(1, 0).await;
        log.batch(1, 0).await;
        log.batch(2, 1).await;
        log.commit(1, &[1, 2]).await.expect("durable");
        let first = [
            "header 0",
            "schema 0",
            "batch 1 of 0",
            "batch 1 of 0",
            "schema 1",
            "batch 2 of 1",
            "seal 1",
            "seal 2",
            &commit(&[1, 2]),
            "end [] []",
        ];
        assert_eq!(chunks(&observed), [(0, first.map(str::to_owned).to_vec())]);
        log.committed(1).await;
        log.batch(3, 1).await;
        log.commit(2, &[3]).await.expect("durable");
        let second = [
            "header 1",
            "schema 1",
            "batch 3 of 1",
            "seal 3",
            &commit(&[3]),
            "end [] []",
        ];
        // Chunk 0 is gone once chunk 1, which records its receipt, is published.
        assert_eq!(chunks(&observed), [(1, second.map(str::to_owned).to_vec())]);
    })
    .await
    .expect("the writer ends");
}

#[tokio::test]
async fn nothing_is_seen_of_a_chunk_until_its_commit_publishes_it_whole() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        log.batch(1, 0).await;
        log.seal(1).await;
        assert!(
            observed.stored(&pipeline()).is_empty(),
            "a staged chunk is unseen"
        );
        log.commit(1, &[]).await.expect("durable");
        let kinds = &chunks(&observed)[0].1;
        assert_eq!(kinds.first().map(String::as_str), Some("header 0"));
        assert_eq!(
            kinds[kinds.len() - 2..],
            [
                format!("commit of {:?}", segments(&[])),
                "end [] []".to_owned()
            ]
        );
    })
    .await
    .expect("the writer ends");
}

#[tokio::test]
async fn a_chunk_s_end_names_the_chunks_still_needed_and_the_commits_received_in_them() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        // Segment 2 stays open across commits, so chunk 0 stays needed.
        log.batch(1, 0).await;
        log.batch(2, 0).await;
        log.batch(2, 0).await;
        log.commit(1, &[1]).await.expect("durable");
        log.committed(1).await;
        log.batch(3, 0).await;
        log.commit(2, &[3]).await.expect("durable");
        let ends: Vec<Option<End>> = frames(&observed)
            .into_iter()
            .map(|(_, frames)| match frames.last() {
                Some(Frame::End(end)) => Some(end.clone()),
                _ => None,
            })
            .collect();
        let end = |live: Vec<u64>, received: Vec<CommitSeq>| Some(End { live, received });
        assert_eq!(ends, [end(vec![], vec![]), end(vec![0], vec![seq(1)])]);
    })
    .await
    .expect("the writer ends");
}

#[tokio::test]
async fn a_chunk_goes_once_a_published_chunk_records_it_settled() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        // Segment 2's batch lands in chunk 0, but only commit 2, logged after it, covers it.
        log.batch(1, 0).await;
        log.batch(2, 0).await;
        log.commit(1, &[1]).await.expect("durable");
        log.committed(1).await;
        assert_eq!(numbers(&observed), [0], "a receipt alone deletes nothing");
        log.batch(3, 0).await;
        log.commit(2, &[2, 3]).await.expect("durable");
        // Segment 2 was open when commit 1's receipt settled the rest of chunk 0: its frame was
        // carried into chunk 1, and chunk 0 went once chunk 1 was published.
        assert_eq!(numbers(&observed), [1], "chunk 1 holds segment 2");
        log.committed(2).await;
        log.batch(4, 0).await;
        log.commit(3, &[4]).await.expect("durable");
        assert_eq!(numbers(&observed), [2], "chunks 0 and 1 are settled");
    })
    .await
    .expect("the writer ends");
}

#[tokio::test]
async fn closing_deletes_the_log_only_where_every_commit_has_its_receipt() {
    for receipts in [true, false] {
        let store = Arc::new(MemoryWal::default());
        drive(Arc::clone(&store), |mut log| async move {
            log.send(table(0)).await;
            log.batch(1, 0).await;
            log.commit(1, &[1]).await.expect("durable");
            if receipts {
                log.committed(1).await;
            }
            log.close().await.expect("closed");
        })
        .await
        .expect("the writer ends");
        if receipts {
            assert!(chunks(&store).is_empty());
        } else {
            let kept = chunks(&store);
            assert_eq!(kept.len(), 2, "{kept:?}");
            assert_eq!(kept[1].1, ["header 1", "closed", "end [0] []"]);
        }
    }
}

#[tokio::test]
async fn a_load_that_logged_nothing_closes_leaving_nothing() {
    let store = Arc::new(MemoryWal::default());
    drive(Arc::clone(&store), |log| async move {
        log.close().await.expect("closed");
    })
    .await
    .expect("the writer ends");
    assert!(chunks(&store).is_empty());
    // The log its load opened is gone too, so no replay is left to take it.
    assert_eq!(store.loads(&pipeline()).await.expect("lists"), []);
}

#[tokio::test]
async fn a_load_whose_next_chunk_a_replay_took_is_fenced_before_it_answers() {
    let store = Arc::new(MemoryWal::default());
    let replay = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        log.batch(1, 0).await;
        log.commit(1, &[1]).await.expect("durable");
        // A replay publishes chunk 1 first, as it fences the log.
        let chunk = super::Chunk {
            load: load(),
            number: 1,
        };
        let mut fence = replay.stage(&pipeline(), chunk).await.expect("stages");
        fence
            .append(Bytes::from_static(b"fence"))
            .await
            .expect("appends");
        fence.publish().await.expect("publishes");
        log.batch(2, 0).await;
        let error = log.commit(2, &[2]).await.expect_err("fenced");
        assert_eq!(error.kind(), ErrorKind::Fenced);
        assert_eq!(error.code(), Some("wal_fenced"));
        // So is every command after.
        let error = log.commit(3, &[]).await.expect_err("fenced");
        assert_eq!(error.code(), Some("wal_fenced"));
        assert_eq!(error.kind(), ErrorKind::Fenced);
    })
    .await
    .expect("the writer ends");
}

#[tokio::test]
async fn after_a_failed_write_every_commit_fails_though_the_store_recovers() {
    let store = Arc::new(MemoryWal::default());
    let failing = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        *failing.failing.lock() = true;
        log.batch(1, 0).await;
        let error = log.commit(1, &[1]).await.expect_err("the store fails");
        assert_eq!(error.kind(), ErrorKind::Wal);
        // Once the store recovers, the lost batch still fails every commit after it.
        *failing.failing.lock() = false;
        log.batch(2, 0).await;
        let error = log.commit(2, &[2]).await.expect_err("a batch was lost");
        assert_eq!(error.kind(), ErrorKind::Wal);
    })
    .await
    .expect("the writer ends");
    assert!(chunks(&store).is_empty(), "no chunk follows a lost batch");
}

#[tokio::test]
async fn a_batch_of_a_table_never_described_fails_the_commit_after_it() {
    let store = Arc::new(MemoryWal::default());
    drive(Arc::clone(&store), |mut log| async move {
        log.batch(1, 5).await;
        log.commit(1, &[1])
            .await
            .expect_err("the batch was not logged");
    })
    .await
    .expect("the writer ends");
}

#[tokio::test]
async fn a_write_the_disk_has_no_room_for_gives_back_what_its_chunk_staged() {
    let store = Arc::new(MemoryWal {
        disk: Arc::new(super::super::memory::Disk::of(2_000)),
        ..MemoryWal::default()
    });
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        for _ in 0..20 {
            log.batch(1, 0).await;
        }
        let error = log.commit(1, &[1]).await.expect_err("the disk is full");
        assert_eq!(error.code(), Some("wal_storage_full"), "{error}");
        assert!(error.is_retryable());
    })
    .await
    .expect("the writer ends");
    assert_eq!(
        store.disk.staged(),
        0,
        "the failed chunk's bytes are given back"
    );
    assert!(chunks(&store).is_empty());
}

#[tokio::test]
async fn a_failed_publish_fails_its_commit_and_every_one_after() {
    let store = Arc::new(MemoryWal::default());
    let failing = Arc::clone(&store);
    drive(Arc::clone(&store), |mut log| async move {
        log.send(table(0)).await;
        log.batch(1, 0).await;
        *failing.unpublishable.lock() = true;
        let error = log
            .commit(1, &[1])
            .await
            .expect_err("the chunk is not durable");
        assert_eq!(error.kind(), ErrorKind::Wal);
        // A publish that failed leaves the log unknown: nothing after it is trusted.
        *failing.unpublishable.lock() = false;
        log.batch(2, 0).await;
        log.commit(2, &[2])
            .await
            .expect_err("the log failed before");
    })
    .await
    .expect("the writer ends");
}

#[tokio::test]
async fn a_commit_after_a_failed_write_fails_as_retryably_as_the_write_did() {
    for transient in [true, false] {
        let store = Arc::new(MemoryWal::default());
        let failing = Arc::clone(&store);
        drive(Arc::clone(&store), |mut log| async move {
            log.send(table(0)).await;
            *(if transient {
                &failing.interrupted
            } else {
                &failing.failing
            })
            .lock() = true;
            log.batch(1, 0).await;
            log.commit(1, &[1]).await.expect_err("the store fails");
            *failing.interrupted.lock() = false;
            *failing.failing.lock() = false;
            // The log is lost for this load either way; a new attempt writes a new one.
            let error = log.commit(2, &[]).await.expect_err("failed before");
            assert_eq!(error.is_retryable(), transient);
        })
        .await
        .expect("the writer ends");
    }
}

#[tokio::test]
async fn a_retired_table_s_schema_frame_is_kept_no_more() {
    let store = Arc::new(MemoryWal::default());
    drive(Arc::clone(&store), |mut log| async move {
        for command in [table(0), table(1)] {
            log.send(command).await;
        }
        log.batch(1, 0).await;
        log.batch(1, 1).await;
        log.commit(1, &[1]).await.expect("durable");
        log.send(Command::Retire { tables: vec![0] }).await;
        // The next chunk still describes the table that stays; the retired one is unknown.
        log.batch(2, 1).await;
        log.commit(2, &[2]).await.expect("durable");
        log.batch(3, 0).await;
        // The batch failed: the writer answers every later command with its failure.
        let failed = log.commit(3, &[3]).await.unwrap_err();
        assert_eq!(failed.kind(), ErrorKind::Internal);
    })
    .await
    .expect("the writer ends");
    let second = chunks(&store)
        .into_iter()
        .find(|(number, _)| *number == 1)
        .expect("a second chunk");
    assert_eq!(second.1[..3], ["header 1", "schema 1", "batch 2 of 1"]);
}

#[tokio::test]
async fn a_retired_table_never_written_releases_what_its_frame_held() {
    let store = Arc::new(MemoryWal::default());
    let budget = MemoryBudget::new(1 << 20);
    let observed = budget.clone();
    drive(Arc::clone(&store), |mut log| async move {
        let Command::Table { index, frame, .. } = table(0) else {
            unreachable!("a table's command")
        };
        let held = budget
            .acquire_log(100)
            .await
            .expect("the log's share has room");
        let held = Box::new(held);
        log.send(Command::Table { index, frame, held }).await;
        log.send(Command::Retire { tables: vec![0] }).await;
        log.commit(1, &[]).await.expect("durable");
        assert_eq!(observed.reserved(), 0);
    })
    .await
    .expect("the writer ends");
}
