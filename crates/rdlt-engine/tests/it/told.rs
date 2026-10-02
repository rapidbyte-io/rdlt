//! A source that keeps to the limits it is told is refused nothing and stalls nothing: cursors
//! answering barriers, rows as long as a frame, and tables as wide as a schema may be.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use arrow_array::{ArrayRef, Int8Array, Int64Array, RecordBatch, StringArray};
use parking_lot::Mutex;
use rdlt_connector::{
    Checkpointing, ConnectContext, ConnectorError, Emitter, Partition, ReadMode, ReadStream,
    Result, SourceConnector, StreamName, StreamSpec, StreamState, Streams, source_factory,
};
use rdlt_engine::{
    CommitPolicy, EngineConfig, EngineConfigBuilder, ErrorKind, LocalWal, RunOutcome, RunStatus,
    WalStore,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use crate::support::making::{Step, Steps, making};
use crate::support::{engine, logging_engine, memory, pipeline, retrying, stream};

/// Partitions, batches each reads, rows a batch, and the bytes of a cursor answering a barrier.
type Shape = (usize, usize, usize, usize);

static SHAPES: LazyLock<Mutex<BTreeMap<String, Shape>>> = LazyLock::new(Mutex::default);

#[derive(Debug, Deserialize, JsonSchema)]
struct AskedConfig {
    name: String,
}

/// A source whose partitions checkpoint only when the engine asks.
struct Asked {
    shape: Shape,
}

impl SourceConnector for Asked {
    const ID: &'static str = "io.test.asked";
    const VERSION: &'static str = "0.0.0";
    type Config = AskedConfig;

    async fn connect(config: AskedConfig, _: &ConnectContext) -> Result<Self> {
        let shape = SHAPES.lock().get(&config.name).copied();
        let shape = shape.ok_or_else(|| ConnectorError::config("no such shape"))?;
        Ok(Self { shape })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new().with(AskedStream)
    }
}

struct AskedStream;

impl ReadStream<Asked> for AskedStream {
    type Cursor = String;

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(StreamName::new("events").expect("a name"))
            .with_read_modes([ReadMode::Full, ReadMode::Incremental])
            .with_checkpointing(Checkpointing::OnDemand)
    }

    async fn partitions(&self, source: &Asked, _: &StreamState) -> Result<Vec<Partition>> {
        let part = |index| {
            let id = rdlt_connector::PartitionId::parse(format!("p{index}"));
            Partition::new(id.expect("an id"))
        };
        Ok((0..source.shape.0).map(part).collect())
    }

    async fn read(
        &self,
        source: &Asked,
        _: &Partition,
        _: String,
        out: &mut Emitter<String>,
    ) -> Result<()> {
        let (_, batches, rows, cursor) = source.shape;
        for batch in 0..batches {
            let id = i64::try_from(batch).expect("an id");
            let ids: ArrayRef = Arc::new(Int64Array::from(vec![id; rows]));
            out.batch(RecordBatch::try_from_iter([("id", ids)]).expect("a batch"))
                .await?;
            // A read that takes its time, so barriers arrive while it reads.
            tokio::time::sleep(Duration::from_millis(100)).await;
            if out.checkpoint_due() {
                // The cursor's JSON text is its characters and their two quotes.
                let text = format!("{batch:08}{}", "c".repeat(cursor.saturating_sub(10)));
                out.checkpoint(&text).await?;
            }
        }
        Ok(())
    }
}

/// Loads sixteen partitions answering barriers with cursors of `cursor` bytes under the
/// default budget, committing every sixteen batches, with the default barrier wait.
async fn on_demand(name: &str, cursor: usize) -> RunOutcome {
    let (parts, batches, rows) = (16, 20, 100);
    SHAPES
        .lock()
        .insert(name.to_owned(), (parts, batches, rows, cursor));
    let source = source_factory::<Asked>()
        .connect(json!({ "name": name }), ConnectContext::new())
        .await
        .expect("registered");
    let every = u64::try_from(parts * rows).expect("a count");
    let policy = CommitPolicy::new(None, Some(every), None).expect("a policy");
    let config = EngineConfig::builder()
        .commit(policy)
        .lanes(2)
        .partitions(parts)
        .barrier_wait(Duration::from_secs(5));
    engine(config)
        .run(
            pipeline(name, [stream("events")]),
            Arc::from(source),
            memory(name).await,
        )
        .await
}

#[tokio::test(start_paused = true)]
async fn barriers_answered_with_cursors_as_large_as_told_never_wait_on_the_budget() {
    let limit = EngineConfig::default().limits().cursor_bytes;
    let limit = usize::try_from(limit).expect("a size");
    let small = on_demand("told_small", 1_024).await;
    let large = on_demand("told_large", limit).await;
    for outcome in [&small, &large] {
        assert_eq!(
            outcome.report.status,
            RunStatus::Succeeded,
            "{:?}",
            outcome.error
        );
    }
    // Every barrier's answers fit beside the cursors waiting: no cursor waits, and the load
    // takes no barrier's wait longer than one of small cursors.
    assert_eq!(large.report.cursor_waits, 0);
    assert_eq!(large.report.commits, small.report.commits);
    assert!(
        large.report.elapsed < small.report.elapsed + Duration::from_secs(1),
        "{:?} against {:?}",
        large.report.elapsed,
        small.report.elapsed
    );
}

/// Loads two partitions, each reading for two seconds, through one slot, a read waiting for a
/// slot or for bytes `wait` at most.
async fn one_slot(name: &str, wait: Duration) -> RunOutcome {
    SHAPES.lock().insert(name.to_owned(), (2, 20, 10, 64));
    let source = source_factory::<Asked>()
        .connect(json!({ "name": name }), ConnectContext::new())
        .await
        .expect("registered");
    let config = EngineConfig::builder()
        .partitions(1)
        .lanes(1)
        .memory_wait(wait)
        .retry(rdlt_engine::RetryPolicy::default().max_attempts(1));
    engine(config)
        .run(
            pipeline(name, [stream("events")]),
            Arc::from(source),
            memory(name).await,
        )
        .await
}

#[tokio::test(start_paused = true)]
async fn a_read_waits_for_a_slot_as_long_as_a_request_waits_for_bytes() {
    // Within the wait, the second read takes the slot once the first ends.
    let waited = one_slot("told_slot", Duration::from_secs(60)).await;
    assert_eq!(
        waited.report.status,
        RunStatus::Succeeded,
        "{:?}",
        waited.error
    );
    assert_eq!(waited.report.rows, 400);
    // Beyond it, the read waiting fails the attempt, saying what the reads kept.
    let started = tokio::time::Instant::now();
    let beyond = one_slot("told_slot_beyond", Duration::from_secs(1)).await;
    let error = beyond.error.expect("the run fails");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Memory, Some("memory_budget_wait_exceeded"))
    );
    let said = format!("{:?}", error.report());
    assert!(said.contains("what reads keep"), "{said}");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
}

/// One row of one text column of `bytes` bytes, then a checkpoint.
fn one_long_row(bytes: usize) -> Steps {
    Arc::new(move |step| match step {
        0 => {
            let text: ArrayRef = Arc::new(StringArray::from(vec!["t".repeat(bytes)]));
            let batch = RecordBatch::try_from_iter([("text", text)]).expect("a batch");
            Some(Step::Batch(batch))
        }
        1 => Some(Step::Checkpoint(8)),
        _ => None,
    })
}

/// Loads what `steps` makes as `name` under `config`, through a log where `logged`.
async fn loaded(name: &str, config: EngineConfigBuilder, steps: Steps, logged: bool) -> RunOutcome {
    let source = making(name, steps).await;
    let destination = memory(name).await;
    let plan = pipeline(name, [stream("events").read(ReadMode::Incremental)]);
    if !logged {
        return engine(config).run(plan, source, destination).await;
    }
    let base = tempfile::tempdir().expect("a directory");
    let store: Arc<dyn WalStore> = Arc::new(LocalWal::new(base.path()));
    let outcome = logging_engine(config, store)
        .run(plan.with_wal(true), source, destination)
        .await;
    drop(base);
    outcome
}

/// The bytes of a text value a one-row batch may hold within `frame`: its offsets, validity and
/// the frame's own bytes take the rest.
fn value_within(frame: u64) -> usize {
    usize::try_from(frame).expect("a size") - 4_096
}

#[tokio::test(start_paused = true)]
async fn a_row_as_long_as_a_frame_loads_at_the_least_memory_and_at_the_default_with_a_log() {
    let least = EngineConfig::least_memory(16);
    let config = retrying(1).memory(least).partitions(16);
    let frame = config.clone().build().expect("valid").limits().frame_bytes;
    let outcome = loaded(
        "told_row_least",
        config,
        one_long_row(value_within(frame)),
        true,
    )
    .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let config = retrying(1);
    let frame = config.clone().build().expect("valid").limits().frame_bytes;
    let outcome = loaded(
        "told_row_default",
        config,
        one_long_row(value_within(frame)),
        true,
    )
    .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
}

/// One row of `columns` columns of 8-bit integers, each named in six bytes.
fn wide_row(columns: usize) -> Steps {
    Arc::new(move |step| match step {
        0 => {
            let one: ArrayRef = Arc::new(Int8Array::from(vec![1_i8]));
            let named = (0..columns).map(|index| {
                let name = format!("c{index:05}");
                (name, Arc::clone(&one))
            });
            Some(Step::Batch(
                RecordBatch::try_from_iter(named).expect("a batch"),
            ))
        }
        1 => Some(Step::Checkpoint(8)),
        _ => None,
    })
}

#[tokio::test(start_paused = true)]
async fn a_table_as_wide_as_a_schema_may_be_commits_through_a_log_and_a_wider_is_refused_first() {
    let least = EngineConfig::least_memory(16);
    let config = || retrying(3).memory(least);
    let limits = config().build().expect("valid").limits();
    let columns = usize::try_from(limits.schema_columns).expect("a count");
    // As wide as a schema may be: the commit records its schema and names, which its change
    // reserved, in its frame.
    let within = loaded("told_wide", config(), wide_row(columns), true).await;
    assert_eq!(
        within.report.status,
        RunStatus::Succeeded,
        "{:?}",
        within.error
    );
    // Four thousand columns pass what a schema may hold: refused where the source pushes them,
    // before any table changes or any commit.
    let beyond = loaded("told_wider", config(), wide_row(4_000), true).await;
    let error = beyond.error.expect("the run fails");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Source, Some("limit_exceeded"))
    );
    let said = format!("{:?}", error.report());
    assert!(said.contains("batch columns"), "{said}");
    assert_eq!(beyond.report.commits, 0);
}

/// A source of two streams: `asked`, whose one partition checkpoints only when asked and never
/// looks whether it is, and `own`, whose partitions checkpoint on their own with cursors as large
/// as they are told they may be.
struct Mixed {
    cursor: usize,
    /// The partitions of `own` that read to their end.
    ended: std::sync::atomic::AtomicUsize,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct MixedConfig {
    cursor: usize,
}

impl SourceConnector for Mixed {
    const ID: &'static str = "io.test.mixed";
    const VERSION: &'static str = "0.0.0";
    type Config = MixedConfig;

    async fn connect(config: MixedConfig, _: &ConnectContext) -> Result<Self> {
        Ok(Self {
            cursor: config.cursor,
            ended: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new().with(Quiet).with(Own)
    }
}

/// One partition that pushes a row, then reads nothing until `own` has read to its end.
struct Quiet;

impl ReadStream<Mixed> for Quiet {
    type Cursor = String;

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(StreamName::new("asked").expect("a name"))
            .with_read_modes([ReadMode::Full])
            .with_checkpointing(Checkpointing::OnDemand)
    }

    async fn partitions(&self, _: &Mixed, _: &StreamState) -> Result<Vec<Partition>> {
        Ok(vec![Partition::new(
            rdlt_connector::PartitionId::parse("q").expect("an id"),
        )])
    }

    async fn read(
        &self,
        source: &Mixed,
        _: &Partition,
        _: String,
        out: &mut Emitter<String>,
    ) -> Result<()> {
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![0_i64]));
        out.batch(RecordBatch::try_from_iter([("id", ids)]).expect("a batch"))
            .await?;
        while source.ended.load(std::sync::atomic::Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    }
}

/// Two partitions, each pushing a row then checkpointing, forty times.
struct Own;

impl ReadStream<Mixed> for Own {
    type Cursor = String;

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(StreamName::new("own").expect("a name"))
            .with_read_modes([ReadMode::Full, ReadMode::Incremental])
    }

    async fn partitions(&self, _: &Mixed, _: &StreamState) -> Result<Vec<Partition>> {
        let part = |index| {
            Partition::new(rdlt_connector::PartitionId::parse(format!("o{index}")).expect("an id"))
        };
        Ok((0..2).map(part).collect())
    }

    async fn read(
        &self,
        source: &Mixed,
        _: &Partition,
        _: String,
        out: &mut Emitter<String>,
    ) -> Result<()> {
        for step in 0..40_i64 {
            let ids: ArrayRef = Arc::new(Int64Array::from(vec![step]));
            out.batch(RecordBatch::try_from_iter([("id", ids)]).expect("a batch"))
                .await?;
            let text = format!("{step:08}{}", "c".repeat(source.cursor.saturating_sub(10)));
            out.checkpoint(&text).await?;
        }
        source
            .ended
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn a_cursor_waiting_for_room_commits_without_waiting_on_a_barrier_slow_to_be_answered() {
    // Three reads at once, at about the least memory: cursors may take eight cursors of the
    // limit, so the forty of each partition of `own` wait on commits, which a policy of rows
    // never makes due.
    let config = || {
        EngineConfig::builder()
            .commit(CommitPolicy::new(None, Some(1_000_000), None).expect("a policy"))
            .memory(34 << 20)
            .partitions(3)
            .lanes(1)
            .barrier_wait(Duration::from_secs(20))
    };
    let limit = config().build().expect("valid").limits().cursor_bytes;
    let cursor = usize::try_from(limit).expect("a size");
    let source = source_factory::<Mixed>()
        .connect(json!({ "cursor": cursor }), ConnectContext::new())
        .await
        .expect("connects");
    let started = tokio::time::Instant::now();
    let outcome = engine(config())
        .run(
            pipeline("told_mixed", [stream("asked"), stream("own")]),
            Arc::from(source),
            memory("told_mixed").await,
        )
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert!(outcome.report.cursor_waits > 0);
    // Each commit a waiting cursor makes due takes what is sealed at once, rather than after the
    // barrier's twenty seconds: the partitions of `own` read to their end in less than one.
    let elapsed = started.elapsed();
    assert!(elapsed < Duration::from_secs(20), "{elapsed:?}");
}
