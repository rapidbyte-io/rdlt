//! How a partition's read ends: what lands of the rows after its last checkpoint, and whether a
//! later run reads the partition again.

use std::sync::{Arc, LazyLock};
use std::time::Duration;

use parking_lot::Mutex;
use rdlt_connector::{
    ConnectContext, Emitter, Partition, PartitionId, ReadMode, ReadStream, Result, SourceConnector,
    StreamName, StreamSpec, StreamState, Streams, source_factory,
};
use rdlt_connector_reference::LogSource;
use rdlt_engine::{
    EngineConfigBuilder, ErrorKind, PipelinePlan, RetryPolicy, RunOutcome, RunStatus, Until,
    WriteMode, codes,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::support::{
    commit_every, engine, memory, pipeline, published_ids, published_json, stream,
};

/// Where each read of the test's source started.
static STARTS: LazyLock<Mutex<Vec<u64>>> = LazyLock::new(Mutex::default);

/// How the source's streams are planned and read.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Shape {
    /// The streams: `rows`, then `rows_1` and on.
    #[serde(default = "one")]
    streams: usize,
    /// The rows each stream holds, ids `0..rows`.
    rows: u64,
    /// How its partition is planned.
    partition: Planned,
    /// Where each read checkpoints among the rows after its cursor.
    checkpoints: Checkpoints,
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum Planned {
    /// The stream plans no partitions of its own.
    Unsplit,
    /// One partition it declares bounded.
    Bounded,
    /// One partition it declares unbounded.
    Unbounded,
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum Checkpoints {
    /// After the first half of them, so the read ends on the rest.
    Middle,
    /// After the first half of them and after the last.
    MiddleAndEnd,
    /// After the first and the second third of them, so the read ends on the last.
    Thirds,
    /// Only before the first of them, where the read starts.
    Leading,
    /// Never.
    Never,
}

fn one() -> usize {
    1
}

/// A source of streams read as its shape says.
struct Ending {
    shape: Shape,
}

impl SourceConnector for Ending {
    const ID: &'static str = "io.test.ending";
    const VERSION: &'static str = "0.0.0";
    type Config = Shape;

    async fn connect(shape: Shape, _: &ConnectContext) -> Result<Self> {
        Ok(Self { shape })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        let named = |index| match index {
            0 => "rows".to_owned(),
            _ => format!("rows_{index}"),
        };
        (0..self.shape.streams).fold(Streams::new(), |streams, index| {
            streams.with(Rows(named(index), self.shape.partition))
        })
    }
}

struct Rows(String, Planned);

impl ReadStream<Ending> for Rows {
    type Cursor = u64;

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(StreamName::new(&self.0).expect("a name"))
            .with_read_modes([ReadMode::Full, ReadMode::Incremental])
    }

    async fn partitions(&self, _: &Ending, _: &StreamState) -> Result<Vec<Partition>> {
        let declared = || Partition::new(PartitionId::parse("p0").expect("an id"));
        Ok(vec![match self.1 {
            Planned::Unsplit => Partition::single(),
            Planned::Bounded => declared(),
            Planned::Unbounded => declared().unbounded(),
        }])
    }

    async fn read(
        &self,
        source: &Ending,
        _: &Partition,
        next: u64,
        out: &mut Emitter<u64>,
    ) -> Result<()> {
        STARTS.lock().push(next);
        let held = source.shape.rows;
        if next < held {
            push(out, source.shape.checkpoints, next, held).await?;
        }
        // A read asked to follow waits for more until it is stopped.
        if out.follows() {
            out.stopped().await;
        }
        Ok(())
    }
}

/// Pushes the rows `next..held`, checkpointed as `checkpoints` says.
async fn push(
    out: &mut Emitter<u64>,
    checkpoints: Checkpoints,
    next: u64,
    held: u64,
) -> Result<()> {
    let span = held - next;
    let marks = match checkpoints {
        Checkpoints::Middle => vec![next + span.div_ceil(2)],
        Checkpoints::MiddleAndEnd => vec![next + span.div_ceil(2), held],
        Checkpoints::Thirds => vec![next + span.div_ceil(3), next + (2 * span).div_ceil(3)],
        Checkpoints::Leading => {
            out.checkpoint(&next).await?;
            Vec::new()
        }
        Checkpoints::Never => Vec::new(),
    };
    let mut from = next;
    for mark in marks {
        out.rows(&ids(from..mark)).await?;
        out.checkpoint(&mark).await?;
        from = mark;
    }
    out.rows(&ids(from..held)).await?;
    Ok(())
}

fn ids(range: std::ops::Range<u64>) -> Vec<Value> {
    range.map(|id| json!({ "id": id })).collect()
}

/// Runs `plan` once over the source `shape` describes, into `store`, with one attempt.
async fn run(plan: PipelinePlan, shape: Value, store: &str) -> RunOutcome {
    run_by(commit_every(1_000), plan, shape, store).await
}

/// Runs `plan` as [`run`] does, by an engine `config` makes.
async fn run_by(
    config: EngineConfigBuilder,
    plan: PipelinePlan,
    shape: Value,
    store: &str,
) -> RunOutcome {
    let source = source_factory::<Ending>()
        .connect(shape, ConnectContext::new())
        .await
        .expect("it connects");
    engine(config.retry(RetryPolicy::default().max_attempts(1)))
        .run(plan, Arc::from(source), memory(store).await)
        .await
}

fn reading(mode: ReadMode, write: WriteMode) -> PipelinePlan {
    pipeline("ending", [stream("rows").read(mode).write(write)])
}

#[tokio::test(start_paused = true)]
async fn a_full_read_lands_every_row_of_a_partition_that_ends_on_rows_after_a_checkpoint() {
    for (planned, write) in [
        ("unbounded", WriteMode::Append),
        ("unbounded", WriteMode::Replace),
        ("unsplit", WriteMode::Append),
        ("bounded", WriteMode::Append),
    ] {
        let store = format!("ending_full_{planned}_{write:?}");
        for rows in [6, 9] {
            let shape = json!({ "rows": rows, "partition": planned, "checkpoints": "middle" });
            let outcome = run(reading(ReadMode::Full, write), shape, &store).await;
            assert_eq!(outcome.report.status, RunStatus::Succeeded, "{outcome:?}");
        }
        let mut expected: Vec<i64> = (0..9).collect();
        if write == WriteMode::Append {
            expected.extend(0..6);
            expected.sort_unstable();
        }
        assert_eq!(
            published_ids(&store, "rows"),
            expected,
            "{planned} {write:?}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_full_read_that_never_checkpoints_lands_everything_it_read() {
    for planned in ["unsplit", "unbounded"] {
        let store = format!("ending_full_never_{planned}");
        let shape = json!({ "rows": 5, "partition": planned, "checkpoints": "never" });
        let outcome = run(reading(ReadMode::Full, WriteMode::Append), shape, &store).await;
        assert_eq!(outcome.report.status, RunStatus::Succeeded, "{outcome:?}");
        assert_eq!(published_ids(&store, "rows"), (0..5).collect::<Vec<_>>());
    }
}

#[tokio::test(start_paused = true)]
async fn an_unsplit_stream_read_incrementally_is_read_again_from_its_committed_cursor() {
    let store = "ending_unsplit";
    STARTS.lock().clear();
    for (rows, checkpoints) in [
        (6, "middle"),
        (10, "middle"),
        (10, "middle_and_end"),
        (10, "middle_and_end"),
    ] {
        let shape = json!({ "rows": rows, "partition": "unsplit", "checkpoints": checkpoints });
        let outcome = run(
            reading(ReadMode::Incremental, WriteMode::Append),
            shape,
            store,
        )
        .await;
        assert_eq!(outcome.report.status, RunStatus::Succeeded, "{outcome:?}");
    }
    // Each run reads from where the last committed: the rows after a checkpoint are read again.
    assert_eq!(*STARTS.lock(), [0, 3, 7, 10]);
    assert_eq!(published_ids(store, "rows"), (0..10).collect::<Vec<_>>());
}

#[tokio::test(start_paused = true)]
async fn an_incremental_read_whose_checkpoints_followed_rows_ends_on_rows_and_reads_them_again() {
    let store = "ending_thirds";
    STARTS.lock().clear();
    for checkpoints in ["thirds", "middle_and_end"] {
        let shape = json!({ "rows": 6, "partition": "unsplit", "checkpoints": checkpoints });
        let outcome = run(
            reading(ReadMode::Incremental, WriteMode::Append),
            shape,
            store,
        )
        .await;
        assert_eq!(outcome.report.status, RunStatus::Succeeded, "{outcome:?}");
    }
    assert_eq!(*STARTS.lock(), [0, 4]);
    assert_eq!(published_ids(store, "rows"), (0..6).collect::<Vec<_>>());
}

#[tokio::test(start_paused = true)]
async fn a_declared_bounded_partition_an_incremental_read_finishes_is_not_read_again() {
    let store = "ending_bounded";
    STARTS.lock().clear();
    for rows in [6, 10] {
        let shape = json!({ "rows": rows, "partition": "bounded", "checkpoints": "middle" });
        let outcome = run(
            reading(ReadMode::Incremental, WriteMode::Append),
            shape,
            store,
        )
        .await;
        assert_eq!(outcome.report.status, RunStatus::Succeeded, "{outcome:?}");
    }
    assert_eq!(*STARTS.lock(), [0], "the second run reads nothing");
    assert_eq!(published_ids(store, "rows"), (0..6).collect::<Vec<_>>());
}

#[tokio::test(start_paused = true)]
async fn an_incremental_read_of_an_unbounded_partition_with_no_checkpoint_after_a_row_fails() {
    for (planned, checkpoints) in [
        ("unsplit", "never"),
        ("unbounded", "never"),
        ("unbounded", "leading"),
    ] {
        let store = format!("ending_unmarked_{planned}_{checkpoints}");
        let shape = json!({ "rows": 4, "partition": planned, "checkpoints": checkpoints });
        let outcome = run(
            reading(ReadMode::Incremental, WriteMode::Append),
            shape,
            &store,
        )
        .await;
        assert_eq!(outcome.report.status, RunStatus::Failed, "{outcome:?}");
        let error = outcome.error.expect("the run failed");
        assert_eq!(
            (error.kind(), error.code(), error.connector_code()),
            (ErrorKind::Source, Some(codes::CHECKPOINT_MISSING), None),
            "{planned} {checkpoints}: {error:?}"
        );
        assert!(!error.is_retryable(), "{error:?}");
        assert_eq!(published_ids(&store, "rows"), Vec::<i64>::new());
    }
}

#[tokio::test(start_paused = true)]
async fn an_incremental_read_of_an_unbounded_partition_that_pushes_nothing_ends_clean() {
    let shape = json!({ "rows": 0, "partition": "unsplit", "checkpoints": "never" });
    let plan = reading(ReadMode::Incremental, WriteMode::Append);
    let outcome = run(plan, shape, "ending_nothing").await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded, "{outcome:?}");
}

#[tokio::test(start_paused = true)]
async fn each_unsplit_stream_a_following_run_reads_incrementally_takes_a_followed_read_s_place() {
    // A run's partitions leave room for one followed read fewer than they are.
    for (partitions, status) in [(2, RunStatus::Failed), (3, RunStatus::Succeeded)] {
        let store = format!("ending_following_{partitions}");
        let shape = json!({
            "streams": 2, "rows": 4, "partition": "unsplit", "checkpoints": "middle_and_end",
        });
        let streams = ["rows", "rows_1"].map(|name| stream(name).read(ReadMode::Incremental));
        let plan = pipeline("ending", streams).with_until(Until::For(Duration::from_secs(1)));
        let config = commit_every(1_000).partitions(partitions);
        let outcome = run_by(config, plan, shape, &store).await;
        assert_eq!(outcome.report.status, status, "{outcome:?}");
        match outcome.error {
            Some(error) => assert_eq!(error.code(), Some(codes::PARTITIONS_TOO_FEW), "{error:?}"),
            None => assert_eq!(published_ids(&store, "rows_1"), [0, 1, 2, 3]),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_stopped_read_that_pushed_rows_and_no_checkpoint_after_them_is_not_refused() {
    let store = "ending_stopped";
    let shape = json!({ "rows": 4, "partition": "unsplit", "checkpoints": "never" });
    let plan = reading(ReadMode::Incremental, WriteMode::Append)
        .with_until(Until::For(Duration::from_secs(1)));
    let outcome = run(plan, shape, store).await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded, "{outcome:?}");
    assert_eq!(published_ids(store, "rows"), Vec::<i64>::new());
}

/// The reference log source: one stream, `events`, of one partition that never ends, holding
/// `messages` messages in batches of ten and checkpointing every other batch, its offsets kept in
/// `group`.
async fn log(group: &str, messages: u64) -> Arc<dyn rdlt_connector::Source> {
    let stream = json!({
        "name": "events", "partitions": 1, "messages": messages, "checkpoint_batches": 2,
    });
    let config = json!({ "seed": 5, "group": group, "streams": [stream] });
    let source = source_factory::<LogSource>()
        .connect(config, ConnectContext::new())
        .await
        .expect("the log connects");
    Arc::from(source)
}

#[tokio::test(start_paused = true)]
async fn a_log_checkpointing_every_other_batch_lands_each_message_once_and_then_reads_nothing() {
    // One batch past the cursor, and a batch after the last checkpoint every other batch makes.
    for messages in [10, 30] {
        let store = format!("ending_log_{messages}");
        for run in 1..=2 {
            let plan = pipeline("ending", [stream("events").read(ReadMode::Incremental)]);
            let outcome = engine(commit_every(1_000).retry(RetryPolicy::default().max_attempts(1)))
                .run(plan, log(&store, messages).await, memory(&store).await)
                .await;
            let case = format!("{messages} messages, run {run}");
            assert_eq!(
                outcome.report.status,
                RunStatus::Succeeded,
                "{case}: {outcome:?}"
            );
        }
        let mut offsets: Vec<u64> = published_json(&store, "events")
            .iter()
            .map(|row| row["offset"].as_u64().expect("an offset"))
            .collect();
        offsets.sort_unstable();
        assert_eq!(
            offsets,
            (0..messages).collect::<Vec<_>>(),
            "{messages} messages"
        );
    }
}
