use std::sync::Arc;
use std::time::SystemTime;

use arrow_array::{Int64Array, RecordBatch};
use rdlt_connector::{
    BoxFuture, Capabilities, CommitMeta, CommitSeq, Destination, DestinationSession,
    DestinationWriter, Epoch, LoadId, OpenContext, OpenedSession, PipelineId, Receipt, Result,
    SchemaVersion, SegmentId, SegmentSet, TableChange, TablePath, TableRef, WriteStats,
};
use rdlt_host::{CONNECTOR_LOST, Kills};

use super::{Killing, Schedule};

/// A destination that takes every call and publishes nothing.
struct Taking(Capabilities);

struct TakingSession;

struct TakingWriter;

impl Destination for Taking {
    fn capabilities(&self) -> &Capabilities {
        &self.0
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn open<'a>(&'a self, _: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        Box::pin(async {
            Ok(OpenedSession {
                session: Box::new(TakingSession),
                epoch: Epoch(1),
                state: Vec::new(),
            })
        })
    }
}

impl DestinationSession for TakingSession {
    fn apply_schema<'a>(&'a mut self, _: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn writer<'a>(
        &'a mut self,
        _: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        Box::pin(async { Ok(Box::new(TakingWriter) as Box<dyn DestinationWriter>) })
    }

    fn commit<'a>(&'a mut self, meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        Box::pin(async move {
            Ok(Receipt {
                load_id: meta.load_id,
                commit_seq: meta.commit_seq,
                committed_at: SystemTime::now(),
                rows: 0,
                bytes: 0,
            })
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

impl DestinationWriter for TakingWriter {
    fn write(&mut self, _: SegmentId, _: RecordBatch) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<WriteStats>> {
        Box::pin(async { Ok(WriteStats::default()) })
    }
}

fn table(name: &str) -> TableRef {
    TableRef {
        path: TablePath::new([name]).expect("a valid path"),
        name: Arc::from(name),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    }
}

fn meta() -> CommitMeta {
    CommitMeta {
        load_id: LoadId::from_parts(SystemTime::now(), 7),
        commit_seq: CommitSeq::FIRST,
        epoch: Epoch(1),
        segments: SegmentSet::new(),
        abandoned: SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
        horizon: None,
    }
}

fn batch() -> RecordBatch {
    RecordBatch::try_from_iter([(
        "id",
        Arc::new(Int64Array::from(vec![1])) as arrow_array::ArrayRef,
    )])
    .expect("a valid batch")
}

#[tokio::test]
async fn kills_land_after_the_settled_commit_before_the_scheduled_one_and_losing_an_answer() {
    let kills = Kills::new();
    let schedule = Schedule {
        settled: 2,
        commit: 3,
        answer: Some(1),
    };
    let killing = Killing::new(Arc::new(Taking(Capabilities::minimal())), &kills, schedule);
    let context = OpenContext {
        pipeline: PipelineId::parse("killing").expect("a valid id"),
        load_id: meta().load_id,
    };
    let mut session = killing.open(&context).await.expect("opens").session;
    let mut writer = session.writer(&table("rows")).await.expect("a writer");
    writer.write(SegmentId(1), batch()).await.expect("written");
    assert_eq!(kills.count(), 0, "no commit yet");
    let lost = session
        .commit(&meta())
        .await
        .expect_err("its answer is lost");
    assert_eq!(lost.code(), Some(CONNECTOR_LOST));
    assert_eq!(kills.count(), 1, "killed after the first commit");
    writer.write(SegmentId(2), batch()).await.expect("written");
    session
        .commit(&meta())
        .await
        .expect("the second commit answers");
    assert_eq!(kills.count(), 1, "nothing before the second commit settles");
    writer.write(SegmentId(3), batch()).await.expect("written");
    assert_eq!(kills.count(), 2, "killed at the first write after it");
    writer.write(SegmentId(4), batch()).await.expect("written");
    assert_eq!(kills.count(), 2, "and at no write after");
    session
        .commit(&meta())
        .await
        .expect("the stub still commits");
    assert_eq!(kills.count(), 3, "killed before the third commit");
    session
        .commit(&meta())
        .await
        .expect("the fourth commit answers");
    assert_eq!(kills.count(), 3, "and before no other");
}

#[tokio::test]
async fn the_tables_written_are_named_once_each() {
    let killing = Killing::new(
        Arc::new(Taking(Capabilities::minimal())),
        &Kills::new(),
        Schedule::seeded(0, false),
    );
    let context = OpenContext {
        pipeline: PipelineId::parse("tables").expect("a valid id"),
        load_id: meta().load_id,
    };
    let mut session = killing.open(&context).await.expect("opens").session;
    for name in ["a", "b", "a"] {
        session.writer(&table(name)).await.expect("a writer");
    }
    let names: Vec<String> = killing
        .tables()
        .iter()
        .map(|table| table.name.to_string())
        .collect();
    assert_eq!(names, ["b", "a"]);
}
