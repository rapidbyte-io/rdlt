use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use arrow_array::{Int64Array, RecordBatch};
use rdlt_connector::{
    BoxFuture, Capabilities, CommitKey, CommitMeta, ConnectorError, ConnectorErrorKind,
    Destination, DestinationSession, DestinationWriter, Epoch, OpenContext, OpenedSession,
    PipelineId, Receipt, Result, SchemaVersion, SegmentId, SegmentSet, TableChange, TablePath,
    TableRef, WriteStats,
};
use rdlt_host::{CONNECTOR_LOST, Kills};

use super::{Killing, Schedule};

/// A destination that takes every call and publishes nothing; it carries seven calls at once.
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
                landed: None,
                state: Vec::new(),
            })
        })
    }

    fn max_calls(&self) -> Option<std::num::NonZeroUsize> {
        std::num::NonZeroUsize::new(7)
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
                key: meta.key,
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

/// A destination that takes every call but fails its `fails`th commit, as a connector killed
/// before it answers it does.
struct Failing {
    capabilities: Capabilities,
    fails: Option<u64>,
    commits: Arc<AtomicU64>,
}

struct FailingSession {
    fails: Option<u64>,
    commits: Arc<AtomicU64>,
}

impl Destination for Failing {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn open<'a>(&'a self, _: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        Box::pin(async {
            let session = FailingSession {
                fails: self.fails,
                commits: Arc::clone(&self.commits),
            };
            Ok(OpenedSession {
                session: Box::new(session),
                epoch: Epoch(1),
                landed: None,
                state: Vec::new(),
            })
        })
    }
}

impl DestinationSession for FailingSession {
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
            let commit = self.commits.fetch_add(1, Ordering::SeqCst) + 1;
            if self.fails == Some(commit) {
                let message = "the connector was killed before it answered";
                return Err(ConnectorError::new(ConnectorErrorKind::Transient, message));
            }
            Ok(Receipt {
                key: meta.key,
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

/// What became of a commit through a killing destination.
#[derive(Debug, PartialEq, Eq)]
enum Answer {
    /// It was answered.
    Answered,
    /// The destination failed it.
    Failed,
    /// It landed and its answer was lost, with a kill.
    Lost,
}

/// What becomes of `commits` commits through a killing destination that loses the answer the
/// schedule's `answer` says, of a destination that fails its `fails`th commit; each lost answer
/// kills once, and nothing else does.
async fn answered(answer: u64, fails: Option<u64>, commits: u64) -> Vec<Answer> {
    let kills = Kills::new();
    let schedule = Schedule {
        settled: u64::MAX,
        commit: u64::MAX,
        answer: Some(answer),
    };
    let failing = Failing {
        capabilities: Capabilities::minimal(),
        fails,
        commits: Arc::default(),
    };
    let killing = Killing::new(Arc::new(failing), &kills, schedule);
    let context = OpenContext {
        pipeline: PipelineId::parse("answers").expect("a valid id"),
    };
    let mut session = killing.open(&context).await.expect("opens").session;
    let mut answers = Vec::new();
    for _ in 0..commits {
        let answer = match session.commit(&meta()).await {
            Ok(_) => Answer::Answered,
            Err(lost) if lost.code() == Some(CONNECTOR_LOST) => Answer::Lost,
            Err(_) => Answer::Failed,
        };
        answers.push(answer);
    }
    let lost = answers.iter().filter(|answer| **answer == Answer::Lost);
    assert_eq!(kills.count(), u64::try_from(lost.count()).expect("few"));
    answers
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
        key: CommitKey {
            epoch: Epoch(1),
            seq: 1,
        },
        after: None,
        segments: SegmentSet::new(),
        abandoned: SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
    }
}

fn batch() -> RecordBatch {
    RecordBatch::try_from_iter([(
        "id",
        Arc::new(Int64Array::from(vec![1])) as arrow_array::ArrayRef,
    )])
    .expect("a valid batch")
}

#[test]
fn a_killing_destination_carries_the_calls_its_destination_carries() {
    let schedule = Schedule {
        settled: 0,
        commit: 1,
        answer: None,
    };
    let taking = Arc::new(Taking(Capabilities::minimal()));
    let killing = Killing::new(
        Arc::clone(&taking) as Arc<dyn Destination>,
        &Kills::new(),
        schedule,
    );
    assert_eq!(killing.max_calls(), taking.max_calls());
    assert!(killing.max_calls().is_some());
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
    };
    let mut session = killing.open(&context).await.expect("opens").session;
    let mut writer = session
        .writer(&table("rows"))
        .await
        .expect("a table writer");
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
async fn an_answer_whose_commit_fails_is_lost_from_the_next_commit_that_lands() {
    use Answer::{Answered, Failed, Lost};
    let answers = answered(2, Some(2), 4).await;
    assert_eq!(answers, [Answered, Failed, Lost, Answered]);
}

#[tokio::test]
async fn an_answer_is_lost_once() {
    use Answer::{Answered, Lost};
    let answers = answered(1, None, 4).await;
    assert_eq!(answers, [Lost, Answered, Answered, Answered]);
}

#[tokio::test]
async fn no_answer_is_lost_before_its_commit() {
    use Answer::{Answered, Lost};
    let answers = answered(3, None, 4).await;
    assert_eq!(answers, [Answered, Answered, Lost, Answered]);
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
    };
    let mut session = killing.open(&context).await.expect("opens").session;
    for name in ["a", "b", "a"] {
        session.writer(&table(name)).await.expect("a table writer");
    }
    let names: Vec<String> = killing
        .tables()
        .iter()
        .map(|table| table.name.to_string())
        .collect();
    assert_eq!(names, ["b", "a"]);
}
