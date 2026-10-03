//! What a destination stores of a pipeline's state: never more than an open's answer, a commit's
//! request, a plan's request and a report of committed positions carry.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use parking_lot::Mutex;
use rdlt_connector::wire::record_bytes;
use rdlt_connector::{
    ConnectContext, Destination, Emitter, OpenContext, Partition, PartitionId, PipelineId,
    PipelineState, ReadMode, ReadStream, Result, SourceConnector, StreamName, StreamSpec,
    StreamState, Streams, source_factory,
};
use rdlt_engine::{EngineConfig, EngineConfigBuilder, ErrorKind, LocalWal, RunOutcome, RunStatus};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use crate::support::{commit_every, engine, logging_engine, memory, pipeline, stream};

/// What a test's source plans and sends: the ids of the partitions its plans name, the bytes of
/// the cursor each checkpoints after its one row, and whether it can read again what it
/// acknowledged.
#[derive(Clone, Debug)]
struct Shape {
    partitions: Vec<String>,
    cursor: usize,
    replayable: bool,
}

static SHAPES: LazyLock<Mutex<BTreeMap<String, Shape>>> = LazyLock::new(Mutex::default);

/// Acknowledgements each test's source heard, by its name.
static ACKS: LazyLock<Mutex<BTreeMap<String, Arc<AtomicUsize>>>> = LazyLock::new(Mutex::default);

#[derive(Debug, Deserialize, JsonSchema)]
struct PlannedConfig {
    name: String,
}

/// A source of one stream, `events`, whose partitions each push one row and checkpoint once.
struct Planned {
    name: String,
}

impl SourceConnector for Planned {
    const ID: &'static str = "io.test.planned";
    const VERSION: &'static str = "0.0.0";
    type Config = PlannedConfig;

    async fn connect(config: PlannedConfig, _: &ConnectContext) -> Result<Self> {
        Ok(Self { name: config.name })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        let replayable = SHAPES.lock()[&self.name].replayable;
        Streams::new().with(Events { replayable })
    }
}

struct Events {
    replayable: bool,
}

impl ReadStream<Planned> for Events {
    type Cursor = String;

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(StreamName::new("events").expect("a name"))
            .with_read_modes([ReadMode::Incremental])
            .with_replayable(self.replayable)
    }

    async fn partitions(&self, source: &Planned, _: &StreamState) -> Result<Vec<Partition>> {
        let shape = SHAPES.lock()[&source.name].clone();
        let partition = |id: &String| Partition::new(PartitionId::parse(id).expect("an id"));
        Ok(shape.partitions.iter().map(partition).collect())
    }

    async fn read(
        &self,
        source: &Planned,
        partition: &Partition,
        cursor: String,
        out: &mut Emitter<String>,
    ) -> Result<()> {
        if !cursor.is_empty() {
            return Ok(());
        }
        let size = SHAPES.lock()[&source.name].cursor;
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![1_i64]));
        out.batch(RecordBatch::try_from_iter([("id", ids)]).expect("a batch"))
            .await?;
        let text = format!("{}:{}", partition.id(), "c".repeat(size));
        out.checkpoint(&text).await
    }

    async fn committed(&self, source: &Planned, cursors: &[(PartitionId, String)]) -> Result<()> {
        let acks = ACKS.lock().entry(source.name.clone()).or_default().clone();
        acks.fetch_add(cursors.len(), Ordering::SeqCst);
        Ok(())
    }
}

/// A source named `name` of `shape`.
async fn source(name: &str, shape: Shape) -> Arc<dyn rdlt_connector::Source> {
    SHAPES.lock().insert(name.to_owned(), shape);
    let connected = source_factory::<Planned>()
        .connect(json!({ "name": name }), ConnectContext::new())
        .await
        .expect("the source connects");
    Arc::from(connected)
}

/// The acknowledgements `name`'s source heard.
fn acks(name: &str) -> usize {
    ACKS.lock()
        .get(name)
        .map_or(0, |acks| acks.load(Ordering::SeqCst))
}

/// `count` partition ids, `p0` onwards.
fn ids(count: usize) -> Vec<String> {
    (0..count).map(|index| format!("p{index}")).collect()
}

/// The state `destination` holds for pipeline `name`.
///
/// What it holds must be what one message may carry: an open's answer within the default limit,
/// beside its session's handle and epoch.
async fn stored(destination: &dyn Destination, name: &str) -> PipelineState {
    let context = OpenContext {
        pipeline: PipelineId::parse(name).expect("a valid pipeline"),
        load_id: rdlt_connector::LoadId::from_parts(std::time::UNIX_EPOCH, 1),
    };
    let opened = destination
        .open(&context)
        .await
        .expect("the destination opens");
    let carried: u64 = opened.state.iter().map(record_bytes).sum();
    let limit = EngineConfig::default().growth().state_bytes().get();
    assert!(carried + 22 <= limit, "{carried} bytes of state");
    PipelineState::from_records(&opened.state).expect("the state reads")
}

/// The kinds of the frames `name`'s log holds in `store`, of every load.
async fn logged(store: &dyn rdlt_engine::WalStore, name: &str) -> Vec<u8> {
    let pipeline = PipelineId::parse(name).expect("a valid pipeline");
    let mut kinds = Vec::new();
    for load in store.loads(&pipeline).await.expect("loads list") {
        for (number, len) in store.chunks(&pipeline, load).await.expect("chunks list") {
            let chunk = rdlt_engine::Chunk { load, number };
            let bytes = store
                .read(&pipeline, chunk, 0, len)
                .await
                .expect("the chunk reads");
            let mut at = 0;
            while at + 9 <= bytes.len() {
                kinds.push(bytes[at]);
                let len = u32::from_le_bytes([
                    bytes[at + 1],
                    bytes[at + 2],
                    bytes[at + 3],
                    bytes[at + 4],
                ]);
                at += 9 + usize::try_from(len).expect("a length");
            }
        }
    }
    kinds
}

/// Cursors of `bytes` fit a budget this large: a 64th of it is the cursors' share.
fn roomy() -> EngineConfigBuilder {
    commit_every(1_000_000).memory(4 << 30)
}

/// Checks that `outcome` failed for state that would pass what a message carries.
fn refused(outcome: &RunOutcome) {
    assert_eq!(outcome.report.status, RunStatus::Failed);
    let error = outcome.error.as_ref().expect("the run fails");
    assert_eq!(error.kind(), ErrorKind::Config, "{error:?}");
    assert_eq!(error.code(), Some("state_bytes_exceeded"), "{error:?}");
    assert!(!error.is_retryable());
}

#[tokio::test(start_paused = true)]
async fn a_commit_whose_state_would_pass_the_limit_is_refused_before_it_is_logged() {
    let name = "stored-one-commit";
    let cursor = usize::try_from(roomy().build().expect("valid").limits().cursor_bytes)
        .expect("a size")
        - 64;
    let shape = Shape {
        partitions: ids(10),
        cursor,
        replayable: false,
    };
    let base = tempfile::tempdir().expect("a temporary directory");
    let store: Arc<dyn rdlt_engine::WalStore> = Arc::new(LocalWal::new(base.path()));
    let outcome = logging_engine(roomy(), Arc::clone(&store))
        .run(
            pipeline(name, [stream("events").read(ReadMode::Incremental)]),
            source(name, shape).await,
            memory(name).await,
        )
        .await;
    refused(&outcome);
    // Nothing was logged as committed, and the source heard of no position.
    assert_eq!(acks(name), 0);
    let kinds = logged(store.as_ref(), name).await;
    assert!(kinds.contains(&3), "batches were logged: {kinds:?}");
    assert!(!kinds.contains(&5), "a commit was logged: {kinds:?}");
    let state = stored(memory(name).await.as_ref(), name).await;
    assert!(state.streams.is_empty(), "{state:?}");
}

#[tokio::test(start_paused = true)]
async fn state_grown_by_small_commits_stops_at_the_limit_and_still_opens() {
    let name = "stored-small-commits";
    let config = || commit_every(1).memory(4 << 30).partitions(1);
    let cursor = usize::try_from(config().build().expect("valid").limits().cursor_bytes)
        .expect("a size")
        - 64;
    let shape = Shape {
        partitions: ids(12),
        cursor,
        replayable: true,
    };
    let outcome = engine(config())
        .run(
            pipeline(name, [stream("events").read(ReadMode::Incremental)]),
            source(name, shape).await,
            memory(name).await,
        )
        .await;
    refused(&outcome);
    // The commits before the refused one landed, and their state opens.
    let state = stored(memory(name).await.as_ref(), name).await;
    let partitions = state.streams[&StreamName::new("events").expect("a name")]
        .partitions
        .len();
    assert!((1..12).contains(&partitions), "{partitions} partitions");
}

#[tokio::test(start_paused = true)]
async fn long_column_names_cannot_make_state_unopenable() {
    use crate::support::batches::{BatchStream, batches};
    let name = "stored-long-names";
    let streams: Vec<String> = (0..6).map(|index| format!("s{index}")).collect();
    let key = "k".repeat(3 << 20);
    let document = json!({ "id": 1, key: 1 }).to_string();
    let pushes: Vec<BatchStream> = streams
        .iter()
        .map(|stream| BatchStream::json(stream, &[&document]))
        .collect();
    let source = batches(name, pushes).await;
    let plan = pipeline(name, streams.iter().map(|name| stream(name)));
    let outcome = engine(commit_every(1))
        .run(plan, source, memory(name).await)
        .await;
    refused(&outcome);
    let state = stored(memory(name).await.as_ref(), name).await;
    assert!(!state.tables.is_empty());
}

#[test]
fn the_default_state_limit_is_what_a_message_carrying_state_may_take() {
    assert_eq!(
        EngineConfig::default().growth().state_bytes().get(),
        16 << 20
    );
}

/// Loads `name`'s stream with its plans naming `partitions`, which must succeed; the ids of the
/// partitions the stored state then names.
async fn planned(name: &str, partitions: &[&str]) -> Vec<String> {
    let shape = Shape {
        partitions: partitions.iter().map(ToString::to_string).collect(),
        cursor: 8,
        replayable: true,
    };
    let outcome = engine(commit_every(1_000))
        .run(
            pipeline(name, [stream("events").read(ReadMode::Incremental)]),
            source(name, shape).await,
            memory(name).await,
        )
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let state = stored(memory(name).await.as_ref(), name).await;
    state
        .streams
        .get(&StreamName::new("events").expect("a name"))
        .map(|stream| stream.partitions.keys().map(ToString::to_string).collect())
        .unwrap_or_default()
}

#[tokio::test(start_paused = true)]
async fn a_partition_a_plan_omits_keeps_its_position() {
    let name = "stored-kept";
    assert_eq!(planned(name, &["p0", "p1"]).await, ["p0", "p1"]);
    // A plan may omit a partition for a moment, as a listing that failed in part does.
    assert_eq!(planned(name, &["p1"]).await, ["p0", "p1"]);
    assert_eq!(crate::support::published_rows(name, "events"), 2);
    // Named again, it resumes where it stood: no row is read twice.
    assert_eq!(planned(name, &["p0", "p1"]).await, ["p0", "p1"]);
    assert_eq!(crate::support::published_rows(name, "events"), 2);
}
