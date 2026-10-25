//! What a served connection carries at once: a run reads, and holds writers open, within the
//! calls its connector's connection carries, so no read or writer waits for the transport.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::prelude::*;
use rdlt_connector::serve::Served;
use rdlt_connector::{Destination, PipelineId, Role, Source};
use rdlt_connector_reference::{MemoryDestination, MemorySource, published};
use rdlt_engine::{
    EngineConfig, EngineConfigBuilder, ErrorKind, GrowthLimits, PipelinePlan, RunOutcome,
    RunStatus, StreamPlan, Until, codes,
};
use rdlt_host::{Connection, Options, RemoteDestination, RemoteSource};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::support::{paused_engine, piped};

/// How long a run may take, in the runtime's paused time, before the test calls it hung: longer
/// than every retry of an attempt that waits out a transport's deadline.
const HUNG: Duration = Duration::from_hours(2);

/// Configuration of [`Held`].
#[derive(Debug, Deserialize, JsonSchema)]
struct HeldConfig {
    /// Partitions of its one stream, `held`.
    partitions: u32,
    /// Whether they never end.
    unbounded: bool,
    /// Seconds a read of a bounded partition holds its call before it pushes its row and ends.
    seconds: u64,
}

/// A source of one stream, `held`, whose reads each hold their call: a bounded partition's for
/// its seconds, then it pushes one row; a followed unbounded partition's until it is stopped.
#[derive(Debug)]
struct Held {
    config: HeldConfig,
}

#[source(id = "test.held")]
impl SourceConnector for Held {
    type Config = HeldConfig;

    async fn connect(config: HeldConfig, _context: &ConnectContext) -> Result<Self> {
        Ok(Self { config })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new().with(HeldStream)
    }
}

struct HeldStream;

impl ReadStream<Held> for HeldStream {
    type Cursor = u64;

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(StreamName::new("held").expect("a valid name"))
            .with_read_modes([ReadMode::Full, ReadMode::Incremental])
    }

    async fn partitions(&self, source: &Held, _state: &StreamState) -> Result<Vec<Partition>> {
        let config = &source.config;
        let partitions = (0..config.partitions).map(|index| {
            let id = PartitionId::parse(format!("p{index}")).expect("a valid partition id");
            if config.unbounded {
                Partition::new(id).unbounded()
            } else {
                Partition::new(id)
            }
        });
        Ok(partitions.collect())
    }

    async fn read(
        &self,
        source: &Held,
        partition: &Partition,
        _cursor: u64,
        out: &mut Emitter<u64>,
    ) -> Result<()> {
        if out.follows() && partition.is_unbounded() {
            out.stopped().await;
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(source.config.seconds)).await;
        out.rows(&[serde_json::json!({ "partition": partition.id().as_str() })])
            .await
    }
}

/// The held source, served over a pipe, with `config`.
async fn held(config: serde_json::Value) -> Arc<dyn Source> {
    let io = piped(Served::new().with_source(source_factory::<Held>()));
    let connection = Connection::connect(io, Role::Source, &config, Options::default())
        .await
        .expect("the source handshakes");
    Arc::new(RemoteSource::new(connection))
}

/// The memory destination over `store`, served over a pipe.
async fn served_memory(store: &str) -> RemoteDestination {
    let io = piped(Served::new().with_destination(destination_factory::<MemoryDestination>()));
    let config = serde_json::json!({ "store": store });
    let connection = Connection::connect(io, Role::Destination, &config, Options::default())
        .await
        .expect("the destination handshakes");
    RemoteDestination::new(connection).expect("the destination declares its capabilities")
}

/// The memory destination over `store`, in process.
async fn in_process(store: &str) -> Arc<dyn Destination> {
    let config = serde_json::json!({ "store": store });
    let destination = destination_factory::<MemoryDestination>()
        .connect(config, ConnectContext::new())
        .await
        .expect("the destination connects");
    Arc::from(destination)
}

/// A configuration reading `partitions` partitions at once, within memory that admits them.
fn reading(partitions: usize) -> EngineConfigBuilder {
    EngineConfig::builder()
        .memory(1 << 30)
        .partitions(partitions)
}

/// A pipeline named `name` reading `held` incrementally, following it `until`.
fn followed(name: &str, until: Until) -> PipelinePlan {
    let stream =
        StreamPlan::new(StreamName::new("held").expect("a valid name")).read(ReadMode::Incremental);
    PipelinePlan::new(PipelineId::parse(name).expect("a valid id"), [stream])
        .expect("a valid plan")
        .with_until(until)
}

/// What `run` ends with, unless it takes longer than [`HUNG`].
async fn bounded(run: impl Future<Output = RunOutcome>) -> RunOutcome {
    tokio::time::timeout(HUNG, run)
        .await
        .expect("the run ends within its bound")
}

#[tokio::test(start_paused = true)]
async fn a_served_source_and_destination_carry_every_call_of_their_connection_but_the_heartbeats() {
    let calls = usize::try_from(rdlt_wire::limits::MAX_CALLS).expect("calls fit a usize");
    let carried = NonZeroUsize::new(calls - 1);
    let config = serde_json::json!({ "partitions": 1, "unbounded": false, "seconds": 0 });
    assert_eq!(held(config).await.max_calls(), carried);
    assert_eq!(served_memory("carried").await.max_calls(), carried);
}

#[tokio::test(start_paused = true)]
async fn a_following_run_of_more_unbounded_partitions_than_its_served_source_carries_is_refused() {
    // A connection carries 200 calls: the heartbeat's, the coordinator's other call, a slot left
    // for every other read, and 197 followed reads.
    for partitions in [200, 198] {
        let config =
            serde_json::json!({ "partitions": partitions, "unbounded": true, "seconds": 0 });
        let source = held(config).await;
        let name = format!("too_many_{partitions}");
        let started = tokio::time::Instant::now();
        let run = paused_engine(reading(partitions + 1)).run(
            followed(&name, Until::Forever),
            source,
            in_process(&name).await,
        );
        let outcome = bounded(run).await;
        let error = outcome.error.expect("the run fails");
        assert_eq!(
            (error.kind(), error.code()),
            (ErrorKind::Config, Some(codes::SOURCE_CALLS_EXCEEDED)),
            "{partitions} partitions: {error}"
        );
        assert!(!error.is_retryable());
        assert_eq!(outcome.report.attempted, 1);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "refused after {:?}",
            started.elapsed()
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_following_run_reads_as_many_unbounded_partitions_as_its_served_source_leaves_room_for() {
    let config = serde_json::json!({ "partitions": 197, "unbounded": true, "seconds": 0 });
    let source = held(config).await;
    let until = Until::For(Duration::from_secs(1));
    let run =
        paused_engine(reading(199)).run(followed("room", until), source, in_process("room").await);
    let outcome = bounded(run).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
}

#[tokio::test(start_paused = true)]
async fn bounded_reads_beyond_what_a_served_source_carries_wait_their_turn_and_load() {
    // Each read holds its call longer than a read's start may wait for the transport.
    let config = serde_json::json!({ "partitions": 300, "unbounded": false, "seconds": 61 });
    let source = held(config).await;
    let plan = PipelinePlan::new(
        PipelineId::parse("bounded").expect("a valid id"),
        [StreamPlan::new(
            StreamName::new("held").expect("a valid name"),
        )],
    )
    .expect("a valid plan");
    let run = paused_engine(reading(300)).run(plan, source, in_process("bounded").await);
    let outcome = bounded(run).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let rows: usize = published("bounded", "held")
        .iter()
        .map(arrow_array::RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 300);
    assert_eq!(outcome.report.attempted, 1);
}

#[tokio::test(start_paused = true)]
async fn a_served_destination_takes_more_tables_than_its_connection_carries() {
    let tables = 250;
    let streams: serde_json::Map<String, serde_json::Value> = (0..tables)
        .map(|table| (format!("t{table}"), serde_json::json!([{ "id": table }])))
        .collect();
    let source = source_factory::<MemorySource>()
        .connect(
            serde_json::json!({ "streams": streams }),
            ConnectContext::new(),
        )
        .await
        .expect("the source connects");
    let destination = served_memory("tables").await;
    let plan = PipelinePlan::new(
        PipelineId::parse("tables").expect("a valid id"),
        (0..tables).map(|table| {
            StreamPlan::new(StreamName::new(format!("t{table}")).expect("a valid name"))
        }),
    )
    .expect("a valid plan");
    // More writers than the connection carries calls, every table's open at once.
    let growth = GrowthLimits::new(1024, 300).expect("valid limits");
    let run = paused_engine(EngineConfig::builder().growth(growth)).run(
        plan,
        Arc::from(source),
        Arc::new(destination),
    );
    let outcome = bounded(run).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    for table in 0..tables {
        let rows: usize = published("tables", &format!("t{table}"))
            .iter()
            .map(arrow_array::RecordBatch::num_rows)
            .sum();
        assert_eq!(rows, 1, "table t{table}");
    }
    assert_eq!(outcome.report.attempted, 1);
}
