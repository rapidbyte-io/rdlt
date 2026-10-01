mod abandoned;

use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field as ArrowField, Schema};
use bytes::Bytes;
use rdlt_connector::{
    CommitMeta, CommitSeq, Epoch, Field, LoadId, LogicalType, PipelineId, Receipt, SchemaVersion,
    SegmentId, SegmentSet, TablePath, TableRef, TableSchema,
};
use tokio::sync::oneshot;

use super::super::frame::{Batch, Frame, Frames, Header, Table, VERSION};
use super::super::memory::MemoryWal;
use super::super::store::WalStore;
use super::{Command, WalWriter};
use crate::error::{Error, ErrorKind};

fn pipeline() -> PipelineId {
    PipelineId::parse("orders").expect("a valid pipeline")
}

fn load() -> LoadId {
    LoadId::from_parts(UNIX_EPOCH, 7)
}

fn encoded(frame: &Frame) -> Bytes {
    frame.encode().expect("the frame encodes")
}

fn header() -> Bytes {
    encoded(&Frame::Header(Header {
        version: VERSION,
        pipeline: pipeline(),
        load: load(),
        opened: None,
    }))
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
    Command::Table { index, frame }
}

fn batch(segment: u64, table: u32) -> Command {
    let schema = Schema::new(vec![ArrowField::new("id", DataType::Int64, false)]);
    let rows = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(Int64Array::from(vec![
            i64::try_from(segment).unwrap_or(0),
        ]))],
    )
    .expect("a valid batch");
    let segment = SegmentId(segment);
    let frame = encoded(&Frame::Batch(Batch {
        segment,
        table,
        batch: rows,
    }));
    Command::Batch {
        segment,
        table,
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

/// The commit `number` of `ids`, and where the writer answers once its frame is durable.
fn commit(number: u64, ids: &[u64]) -> (Command, oneshot::Receiver<Result<(), Error>>) {
    let meta = CommitMeta {
        load_id: load(),
        commit_seq: seq(number),
        epoch: Epoch(1),
        segments: segments(ids),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
    };
    let (durable, answer) = oneshot::channel();
    let command = Command::Commit {
        seq: seq(number),
        segments: segments(ids),
        frame: encoded(&Frame::Commit(Box::new(meta))),
        held: Box::new(()),
        durable,
    };
    (command, answer)
}

fn committed(number: u64) -> Command {
    let receipt = Receipt {
        load_id: load(),
        commit_seq: seq(number),
        committed_at: UNIX_EPOCH,
        rows: 1,
        bytes: 8,
    };
    Command::Committed {
        seq: seq(number),
        frame: encoded(&Frame::Committed(receipt)),
    }
}

fn close() -> (Command, oneshot::Receiver<Result<(), Error>>) {
    let (done, answer) = oneshot::channel();
    let frame = encoded(&Frame::Closed);
    (Command::Close { frame, done }, answer)
}

/// A frame's kind, and the segment or table it names.
fn kind(frame: &Frame) -> String {
    match frame {
        Frame::Header(_) => "header".to_owned(),
        Frame::Schema(table) => format!("schema {}", table.index),
        Frame::Batch(batch) => format!("batch {} of {}", batch.segment.0, batch.table),
        Frame::Seal(seal) => format!("seal {}", seal.segment.0),
        Frame::Begun(begun) => format!("phase {} of {}", begun.phase, begun.stream),
        Frame::Commit(meta) => format!("commit of {:?}", meta.segments),
        Frame::Committed(_) => "committed".to_owned(),
        Frame::Closed => "closed".to_owned(),
    }
}

/// Each chunk the store holds for the load, by number, as the kinds of its frames.
fn chunks(store: &MemoryWal) -> Vec<(u64, Vec<String>)> {
    store
        .stored(&pipeline())
        .into_iter()
        .map(|(chunk, stored)| {
            let frames = Frames::new(&stored.bytes)
                .map(|frame| kind(&frame.expect("the frame decodes").1))
                .collect();
            (chunk.number, frames)
        })
        .collect()
}

/// Runs a writer of `store` while `drive` sends it commands, until both end.
async fn drive<F, Fut>(store: Arc<MemoryWal>, drive: F) -> Result<(), Error>
where
    F: FnOnce(WalWriter) -> Fut,
    Fut: Future<Output = ()>,
{
    let wal: Arc<dyn WalStore> = store;
    let (writer, task) = WalWriter::start(wal, pipeline(), load(), header(), Box::new(()));
    let (ended, ()) = tokio::join!(task, drive(writer));
    ended
}

async fn send(writer: &WalWriter, command: Command) {
    writer.send(command).await.expect("the writer runs");
}

#[tokio::test]
async fn each_chunk_starts_with_the_header_and_the_schemas_its_batches_name() {
    let store = Arc::new(MemoryWal::default());
    drive(Arc::clone(&store), |writer| async move {
        for command in [table(0), table(1), batch(1, 0), batch(1, 0), batch(2, 1)] {
            send(&writer, command).await;
        }
        let (command, answer) = commit(1, &[1, 2]);
        send(&writer, command).await;
        answer.await.expect("the writer answers").expect("durable");
        send(&writer, batch(3, 1)).await;
    })
    .await
    .expect("the writer ends");
    assert_eq!(
        chunks(&store),
        [
            (
                0,
                vec![
                    "header".to_owned(),
                    "schema 0".to_owned(),
                    "batch 1 of 0".to_owned(),
                    "batch 1 of 0".to_owned(),
                    "schema 1".to_owned(),
                    "batch 2 of 1".to_owned(),
                    format!("commit of {:?}", segments(&[1, 2])),
                ]
            ),
            (
                1,
                vec![
                    "header".to_owned(),
                    "schema 1".to_owned(),
                    "batch 3 of 1".to_owned()
                ]
            ),
        ]
    );
}

#[tokio::test]
async fn a_commit_is_answered_only_once_its_frame_is_durable() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |writer| async move {
        send(&writer, table(0)).await;
        send(&writer, batch(1, 0)).await;
        let (command, answer) = commit(1, &[1]);
        send(&writer, command).await;
        answer.await.expect("the writer answers").expect("durable");
        let stored = observed.stored(&pipeline());
        let (chunk, first) = &stored[0];
        assert_eq!(chunk.number, 0);
        assert_eq!(
            first.synced,
            first.bytes.len(),
            "the commit frame is durable"
        );
        // A crash now keeps the commit frame whole.
        observed.crash();
        assert_eq!(
            chunks(&observed)[0].1.last().map(String::as_str),
            Some(format!("commit of {:?}", segments(&[1])).as_str())
        );
    })
    .await
    .expect("the writer ends");
}

#[tokio::test]
async fn a_chunk_goes_once_every_segment_and_commit_in_it_is_committed() {
    let store = Arc::new(MemoryWal::default());
    let observed = Arc::clone(&store);
    drive(Arc::clone(&store), |writer| async move {
        let numbers = |store: &MemoryWal| -> Vec<u64> {
            chunks(store)
                .into_iter()
                .map(|(number, _)| number)
                .collect()
        };
        send(&writer, table(0)).await;
        // Segment 2's batch lands in chunk 0, but only commit 2, in chunk 1, covers it.
        send(&writer, batch(1, 0)).await;
        send(&writer, batch(2, 0)).await;
        let (command, answer) = commit(1, &[1]);
        send(&writer, command).await;
        answer.await.expect("the writer answers").expect("durable");
        send(&writer, committed(1)).await;
        send(&writer, batch(3, 0)).await;
        let (command, answer) = commit(2, &[2, 3]);
        send(&writer, command).await;
        answer.await.expect("the writer answers").expect("durable");
        assert_eq!(numbers(&observed), [0, 1], "chunk 0 holds segment 2");
        send(&writer, committed(2)).await;
        send(&writer, batch(4, 0)).await;
        let (command, answer) = commit(3, &[4]);
        send(&writer, command).await;
        answer.await.expect("the writer answers").expect("durable");
        assert_eq!(numbers(&observed), [2], "chunks 0 and 1 are committed");
    })
    .await
    .expect("the writer ends");
}

#[tokio::test]
async fn closing_removes_the_log_only_where_every_commit_has_its_receipt() {
    for receipts in [true, false] {
        let store = Arc::new(MemoryWal::default());
        drive(Arc::clone(&store), |writer| async move {
            send(&writer, table(0)).await;
            send(&writer, batch(1, 0)).await;
            let (command, answer) = commit(1, &[1]);
            send(&writer, command).await;
            answer.await.expect("the writer answers").expect("durable");
            if receipts {
                send(&writer, committed(1)).await;
            }
            let (command, done) = close();
            send(&writer, command).await;
            done.await.expect("the writer answers").expect("closed");
        })
        .await
        .expect("the writer ends");
        if receipts {
            assert!(chunks(&store).is_empty());
        } else {
            let kept = chunks(&store);
            assert_eq!(kept.len(), 2, "{kept:?}");
            assert_eq!(kept[1].1, ["header", "closed"]);
            store.crash();
            assert_eq!(chunks(&store), kept, "the closing frame is durable");
        }
    }
}

#[tokio::test]
async fn after_a_failed_append_every_commit_fails_though_the_store_recovers() {
    let store = Arc::new(MemoryWal::default());
    let failing = Arc::clone(&store);
    drive(Arc::clone(&store), |writer| async move {
        send(&writer, table(0)).await;
        *failing.failing.lock() = true;
        send(&writer, batch(1, 0)).await;
        let (command, answer) = commit(1, &[1]);
        send(&writer, command).await;
        let error = answer
            .await
            .expect("the writer answers")
            .expect_err("the store fails");
        assert_eq!(error.kind(), ErrorKind::Wal);
        // Once the store recovers, the lost batch still fails every commit after it.
        *failing.failing.lock() = false;
        send(&writer, batch(2, 0)).await;
        let (command, answer) = commit(2, &[1, 2]);
        send(&writer, command).await;
        let error = answer
            .await
            .expect("the writer answers")
            .expect_err("a batch was lost");
        assert_eq!(error.kind(), ErrorKind::Wal);
    })
    .await
    .expect("the writer ends");
    assert!(
        chunks(&store)
            .iter()
            .all(|(_, frames)| !frames.iter().any(|frame| frame.starts_with("commit"))),
        "no commit frame follows a lost batch"
    );
}

#[tokio::test]
async fn a_batch_of_a_table_never_described_fails_the_commit_after_it() {
    let store = Arc::new(MemoryWal::default());
    drive(Arc::clone(&store), |writer| async move {
        send(&writer, batch(1, 5)).await;
        let (command, answer) = commit(1, &[1]);
        send(&writer, command).await;
        answer
            .await
            .expect("the writer answers")
            .expect_err("the batch was not logged");
    })
    .await
    .expect("the writer ends");
}

#[tokio::test]
async fn a_failed_sync_fails_its_commit_and_every_one_after() {
    let store = Arc::new(MemoryWal::default());
    let failing = Arc::clone(&store);
    drive(Arc::clone(&store), |writer| async move {
        send(&writer, table(0)).await;
        send(&writer, batch(1, 0)).await;
        *failing.unsyncable.lock() = true;
        let (command, answer) = commit(1, &[1]);
        send(&writer, command).await;
        let error = answer
            .await
            .expect("the writer answers")
            .expect_err("the commit frame is not durable");
        assert_eq!(error.kind(), ErrorKind::Wal);
        // A flush that failed leaves the chunk unknown: nothing after it is trusted.
        *failing.unsyncable.lock() = false;
        send(&writer, batch(2, 0)).await;
        let (command, answer) = commit(2, &[2]);
        send(&writer, command).await;
        answer
            .await
            .expect("the writer answers")
            .expect_err("the log failed before");
    })
    .await
    .expect("the writer ends");
}

#[tokio::test]
async fn a_commit_after_a_failed_append_fails_as_retryably_as_the_append_did() {
    for transient in [true, false] {
        let store = Arc::new(MemoryWal::default());
        let failing = Arc::clone(&store);
        drive(Arc::clone(&store), |writer| async move {
            send(&writer, table(0)).await;
            *(if transient {
                &failing.interrupted
            } else {
                &failing.failing
            })
            .lock() = true;
            send(&writer, batch(1, 0)).await;
            let (command, answer) = commit(1, &[1]);
            send(&writer, command).await;
            answer
                .await
                .expect("the writer answers")
                .expect_err("the store fails");
            *failing.interrupted.lock() = false;
            *failing.failing.lock() = false;
            // The log is lost for this load either way; a new attempt writes a new one.
            let (command, answer) = commit(2, &[1]);
            send(&writer, command).await;
            let error = answer
                .await
                .expect("the writer answers")
                .expect_err("failed before");
            assert_eq!(error.is_retryable(), transient);
        })
        .await
        .expect("the writer ends");
    }
}
