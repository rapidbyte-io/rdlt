//! Connectors and helpers the engine tests share.

pub(crate) mod batches;
pub(crate) mod destinations;
pub(crate) mod listening;
pub(crate) mod logs;
pub(crate) mod script;
pub(crate) mod targets;

use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use arrow_array::{Array, Int64Array, RecordBatch};
use rdlt_connector::{
    ConnectContext, Destination, PipelineId, Source, StreamName, destination_factory,
    source_factory,
};
use rdlt_connector_reference::{GeneratorSource, MemoryDestination, published};
use rdlt_engine::{
    CommitPolicy, ComputePool, Engine, EngineConfig, EngineConfigBuilder, Env, Job, PipelinePlan,
    RayonPool, RunControl, RunOutcome, Sleep, StreamPlan, SystemEnv, WalStore,
};
use serde_json::{Value, json};

/// The example `name`, which the test build builds beside the tests: in the first directory above
/// the test binary that holds an `examples` directory with it, whichever layout the build uses.
pub(crate) fn example(name: &str) -> std::path::PathBuf {
    let tests = std::env::current_exe().expect("the test binary has a path");
    tests
        .ancestors()
        .map(|dir| dir.join("examples").join(name))
        .find(|example| example.is_file())
        .expect("the test build builds the examples")
}

/// Runs `scenario` against each of `targets`, in turn.
pub(crate) async fn each<Run>(
    targets: impl IntoIterator<Item = targets::Target>,
    scenario: impl Fn(targets::Target) -> Run,
) where
    Run: std::future::Future<Output = ()>,
{
    for target in targets {
        scenario(target).await;
    }
}

/// An engine on the system environment with `config`, running compute jobs inline.
pub(crate) fn engine(config: EngineConfigBuilder) -> TestEngine {
    counting_engine(config).0
}

/// An engine as [`engine`] makes it, and how many compute jobs it has run.
pub(crate) fn counting_engine(config: EngineConfigBuilder) -> (TestEngine, Arc<AtomicUsize>) {
    let pool = RayonPool::new(NonZeroUsize::MIN).expect("a one-thread pool starts");
    engine_on(config, SystemEnv::new(pool))
}

/// An engine as [`engine`] makes it, keeping write-ahead logs in `store`.
pub(crate) fn logging_engine(config: EngineConfigBuilder, store: Arc<dyn WalStore>) -> TestEngine {
    let pool = RayonPool::new(NonZeroUsize::MIN).expect("a one-thread pool starts");
    engine_on(config, SystemEnv::new(pool).with_wal(store)).0
}

fn engine_on(config: EngineConfigBuilder, system: SystemEnv) -> (TestEngine, Arc<AtomicUsize>) {
    let config = config.build().expect("the test configuration is valid");
    let jobs = Arc::new(AtomicUsize::new(0));
    let env = InlineEnv(system, Inline(Arc::clone(&jobs)));
    (TestEngine(Engine::new(config, Arc::new(env))), jobs)
}

/// The system's clock and randomness, with compute jobs run on the calling thread: the paused
/// test runtime would otherwise advance its clock while a job runs on another thread.
struct InlineEnv(SystemEnv, Inline);

impl Env for InlineEnv {
    fn now(&self) -> std::time::SystemTime {
        self.0.now()
    }

    fn instant(&self) -> std::time::Instant {
        self.0.instant()
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        self.0.sleep(duration)
    }

    fn random(&self) -> u64 {
        self.0.random()
    }

    fn compute(&self) -> &dyn ComputePool {
        &self.1
    }

    fn wal(&self) -> Option<Arc<dyn WalStore>> {
        self.0.wal()
    }
}

/// A [`ComputePool`] that runs each job at once, on the calling thread, counting them.
struct Inline(Arc<AtomicUsize>);

impl ComputePool for Inline {
    fn execute(&self, job: Job) {
        self.0.fetch_add(1, Ordering::SeqCst);
        job();
    }
}

/// How long a test waits, in the runtime's paused time, before it calls something hung; the
/// longest wait any test needs is a 90-second rate limit.
const LIMIT: Duration = Duration::from_secs(600);

/// An engine whose runs fail the test instead of hanging it.
pub(crate) struct TestEngine(Engine);

impl TestEngine {
    /// Starts a run that panics if it has not ended within [`LIMIT`].
    pub(crate) fn run(
        &self,
        plan: PipelinePlan,
        source: Arc<dyn Source>,
        destination: Arc<dyn Destination>,
    ) -> Guarded {
        let handle = self.0.run(plan, source, destination);
        let control = handle.control();
        let future = async move {
            tokio::time::timeout(LIMIT, handle)
                .await
                .expect("the run ends within the test's limit")
        };
        Guarded {
            control,
            future: Box::pin(future),
        }
    }
}

/// A run awaited under a time limit.
pub(crate) struct Guarded {
    control: RunControl,
    future: Pin<Box<dyn Future<Output = RunOutcome> + Send>>,
}

impl Guarded {
    pub(crate) fn control(&self) -> RunControl {
        self.control.clone()
    }
}

impl Future for Guarded {
    type Output = RunOutcome;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<RunOutcome> {
        self.future.as_mut().poll(context)
    }
}

/// A configuration that commits every `rows` rows and never waits for barriers long.
pub(crate) fn commit_every(rows: u64) -> EngineConfigBuilder {
    let policy = CommitPolicy::new(None, Some(rows), None).expect("a row threshold is valid");
    EngineConfig::builder()
        .commit(policy)
        .lanes(2)
        .barrier_wait(Duration::from_millis(100))
}

pub(crate) fn pipeline(name: &str, streams: impl IntoIterator<Item = StreamPlan>) -> PipelinePlan {
    let pipeline = PipelineId::parse(name).expect("valid pipeline id");
    PipelinePlan::new(pipeline, streams).expect("the test plan is valid")
}

pub(crate) fn stream(name: &str) -> StreamPlan {
    StreamPlan::new(StreamName::new(name).expect("valid stream name"))
}

/// A generator source of `streams`, each `(name, rows, partitions, batch_rows)`.
pub(crate) async fn generator(streams: &[(&str, u64, u64, u64)]) -> Arc<dyn Source> {
    let source = source_factory::<GeneratorSource>()
        .connect(generated(streams), ConnectContext::new())
        .await
        .expect("the generator connects");
    Arc::from(source)
}

/// The generator's configuration for `streams`: each a name, rows, partitions and batch rows.
fn generated(streams: &[(&str, u64, u64, u64)]) -> Value {
    let streams: Vec<Value> = streams
        .iter()
        .map(|(name, rows, partitions, batch_rows)| {
            json!({ "name": name, "rows": rows, "partitions": partitions, "batch_rows": batch_rows })
        })
        .collect();
    json!({ "seed": 7, "streams": streams })
}

/// Places connectors in processes of their own, whose coverage, when measured, is kept.
pub(crate) fn local() -> rdlt_host::Local {
    rdlt_host::Local::new().env_passthrough("LLVM_PROFILE_FILE")
}

/// The generator, as [`generator`], spawned in a process of its own.
pub(crate) async fn spawned_generator(streams: &[(&str, u64, u64, u64)]) -> Arc<dyn Source> {
    use rdlt_host::Provider as _;
    let id = rdlt_connector::ConnectorId::parse("io.rapidbyte.generator").expect("a valid id");
    let reference = rdlt_host::ConnectorRef::new(id).path(example("serve_generator"));
    let placed = local()
        .source(&reference, &generated(streams))
        .await
        .expect("the generator starts");
    Arc::from(placed.connector)
}

/// A memory destination writing to `store`.
pub(crate) async fn memory(store: &str) -> Arc<dyn Destination> {
    let destination = destination_factory::<MemoryDestination>()
        .connect(json!({ "store": store }), ConnectContext::new())
        .await
        .expect("the memory destination connects");
    Arc::from(destination)
}

/// The sorted ids published to `table` in `store`.
pub(crate) fn published_ids(store: &str, table: &str) -> Vec<i64> {
    let mut ids: Vec<i64> = published(store, table)
        .iter()
        .flat_map(|batch: &RecordBatch| {
            let column = batch
                .column_by_name("id")
                .expect("tables have an id column");
            let ids = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("ids are Int64");
            (0..ids.len()).map(|row| ids.value(row)).collect::<Vec<_>>()
        })
        .collect();
    ids.sort_unstable();
    ids
}

/// `0..rows` as ids.
pub(crate) fn every_id(rows: i64) -> Vec<i64> {
    (0..rows).collect()
}

/// Waits, in the runtime's time, until `condition` holds.
pub(crate) async fn until(condition: impl Fn() -> bool) {
    let waiting = async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(LIMIT, waiting)
        .await
        .expect("the condition holds within the test's limit");
}

/// The number of rows published to `table` in `store`.
pub(crate) fn published_rows(store: &str, table: &str) -> usize {
    published(store, table)
        .iter()
        .map(RecordBatch::num_rows)
        .sum()
}

/// An engine configuration that commits every 10 rows and makes `attempts` attempts, quickly.
pub(crate) fn retrying(attempts: u32) -> EngineConfigBuilder {
    let retry = rdlt_engine::RetryPolicy::default()
        .max_attempts(attempts)
        .initial(Duration::from_millis(10))
        .max_delay(Duration::from_millis(100));
    commit_every(10).retry(retry)
}

/// Every published row of `table` in `store` as a JSON object, without the metadata columns,
/// sorted by their rendering.
pub(crate) fn published_json(store: &str, table: &str) -> Vec<Value> {
    let mut rows = Vec::new();
    for batch in published(store, table) {
        let mut writer = arrow_json::ArrayWriter::new(Vec::new());
        writer
            .write(&batch)
            .expect("published batches render as JSON");
        writer.finish().expect("the JSON array closes");
        let rendered: Vec<Value> =
            serde_json::from_slice(&writer.into_inner()).expect("arrow writes valid JSON");
        for mut row in rendered {
            if let Value::Object(columns) = &mut row {
                columns.retain(|name, _| !name.starts_with("_rdlt_"));
            }
            rows.push(row);
        }
    }
    rows.sort_by_key(ToString::to_string);
    rows
}
