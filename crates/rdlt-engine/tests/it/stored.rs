//! What a destination stores of a pipeline's state: never more than an open's answer, a commit's
//! request, a plan's request and a report of committed positions carry.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use parking_lot::Mutex;
use rdlt_connector::wire::answer_bytes;
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
/// the cursor each checkpoints after its one row, whether it can read again what it
/// acknowledged, and whether a partition resumed from its first cursor sends one row more and a
/// cursor of half its size.
///
/// A partition whose id starts with `d` is read to its end: after its row and a cursor of a few
/// bytes it pushes a row more, so its position records it done.
#[derive(Clone, Debug)]
struct Shape {
    partitions: Vec<String>,
    cursor: usize,
    replayable: bool,
    again: bool,
}

static SHAPES: LazyLock<Mutex<BTreeMap<String, Shape>>> = LazyLock::new(Mutex::default);

/// The reads each test's source started, by its name.
static READS: LazyLock<Mutex<BTreeMap<String, Arc<AtomicUsize>>>> = LazyLock::new(Mutex::default);

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
        let shape = SHAPES.lock()[&source.name].clone();
        let reads = READS.lock().entry(source.name.clone()).or_default().clone();
        reads.fetch_add(1, Ordering::SeqCst);
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![1_i64]));
        let row = RecordBatch::try_from_iter([("id", ids)]).expect("a batch");
        if partition.id().as_str().starts_with('d') {
            if cursor.is_empty() {
                out.batch(row.clone()).await?;
                out.checkpoint(&format!("{}:d", partition.id())).await?;
                out.batch(row).await?;
            }
            return Ok(());
        }
        let (mark, size) = match cursor.chars().last() {
            None => ('c', shape.cursor),
            Some('c') if shape.again => ('d', shape.cursor / 2),
            Some(_) => return Ok(()),
        };
        out.batch(row).await?;
        let text = format!("{}:{}", partition.id(), mark.to_string().repeat(size));
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

/// The reads `name`'s source started.
fn reads(name: &str) -> usize {
    READS
        .lock()
        .get(name)
        .map_or(0, |reads| reads.load(Ordering::SeqCst))
}

/// `count` partition ids, `p0` onwards.
fn ids(count: usize) -> Vec<String> {
    (0..count).map(|index| format!("p{index}")).collect()
}

/// Bytes: what an open's answer carrying the state the memory destination `name` stores of
/// pipeline `name` holds decoded.
async fn carried(name: &str) -> u64 {
    let context = OpenContext {
        pipeline: PipelineId::parse(name).expect("a valid pipeline"),
        load_id: rdlt_connector::LoadId::from_parts(std::time::UNIX_EPOCH, 1),
    };
    let opened = memory(name).await.open(&context).await.expect("it opens");
    answer_bytes(&opened.state)
}

/// Bytes: the state limit an engine of `memory` advertises.
fn state_limit(memory: u64) -> u64 {
    let config = EngineConfig::builder().memory(memory).build();
    config.expect("a valid config").state_limit()
}

/// The state `destination` holds for pipeline `name`.
///
/// What it holds must be what one message may carry: an open's answer within the default limit.
async fn stored(destination: &dyn Destination, name: &str) -> PipelineState {
    let context = OpenContext {
        pipeline: PipelineId::parse(name).expect("a valid pipeline"),
        load_id: rdlt_connector::LoadId::from_parts(std::time::UNIX_EPOCH, 1),
    };
    let opened = destination
        .open(&context)
        .await
        .expect("the destination opens");
    let carried = answer_bytes(&opened.state);
    let limit = EngineConfig::default().state_limit();
    assert!(carried <= limit, "{carried} bytes of state");
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
            // Past the chunk's preamble.
            let mut at = 14;
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
        again: false,
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
    // Nothing was logged as committed, and the source heard of no position: what was staged of
    // the log is published with a commit alone.
    assert_eq!(acks(name), 0);
    let kinds = logged(store.as_ref(), name).await;
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
        again: false,
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
fn the_default_state_limit_is_the_state_an_open_may_answer_that_the_engine_advertises() {
    let config = EngineConfig::default();
    assert_eq!(config.state_limit(), config.limits().state_bytes);
    assert!(config.state_limit() <= rdlt_wire::Limits::default().state_bytes);
}

/// Loads `name`'s stream with its plans naming `partitions`, which must succeed; the ids of the
/// partitions the stored state then names.
async fn planned(name: &str, partitions: &[&str]) -> Vec<String> {
    let shape = Shape {
        partitions: partitions.iter().map(ToString::to_string).collect(),
        cursor: 8,
        replayable: true,
        again: false,
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

#[tokio::test(start_paused = true)]
async fn state_at_the_least_memory_stops_at_its_share_of_the_budget_and_still_opens() {
    let name = "stored-least";
    let least = EngineConfig::least_memory(16);
    let config = || commit_every(1).memory(least);
    let cursor = usize::try_from(config().build().expect("valid").limits().cursor_bytes)
        .expect("a size")
        - 64;
    let shape = Shape {
        partitions: ids(160),
        cursor,
        replayable: true,
        again: false,
    };
    let outcome = engine(config())
        .run(
            pipeline(name, [stream("events").read(ReadMode::Incremental)]),
            source(name, shape).await,
            memory(name).await,
        )
        .await;
    refused(&outcome);
    let carried = carried(name).await;
    let limit = state_limit(least);
    assert!(carried <= limit, "{carried} bytes of state");
    assert!(carried > limit / 2, "{carried} bytes of state");
}

#[tokio::test(start_paused = true)]
async fn state_past_a_lowered_limit_keeps_loading_while_it_shrinks() {
    let name = "stored-lowered";
    let least = EngineConfig::least_memory(16);
    let config = |memory| commit_every(1).memory(memory);
    let cursor = usize::try_from(config(least).build().expect("valid").limits().cursor_bytes)
        .expect("a size")
        - 64;
    let mut shape = Shape {
        partitions: ids(400),
        cursor,
        replayable: true,
        again: false,
    };
    // Under twice the memory, state grows to its limit, past what the least memory admits.
    let outcome = engine(config(2 * least))
        .run(
            pipeline(name, [stream("events").read(ReadMode::Incremental)]),
            source(name, shape.clone()).await,
            memory(name).await,
        )
        .await;
    refused(&outcome);
    let grown = carried(name).await;
    assert!(grown > state_limit(least), "{grown} bytes of state");
    let state = stored(memory(name).await.as_ref(), name).await;
    let recorded = &state.streams[&StreamName::new("events").expect("a name")].partitions;
    // Under the least memory, each partition's commit replaces its cursor with a smaller one.
    shape.partitions = recorded.keys().map(ToString::to_string).collect();
    shape.again = true;
    let outcome = engine(config(least))
        .run(
            pipeline(name, [stream("events").read(ReadMode::Incremental)]),
            source(name, shape.clone()).await,
            memory(name).await,
        )
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let shrunk = carried(name).await;
    assert!(shrunk < grown, "{shrunk} bytes of state");
    assert_eq!(
        crate::support::published_rows(name, "events"),
        2 * shape.partitions.len()
    );
}

/// Loads `name`'s stream of partitions `partitions` under `config`; the outcome.
async fn run(
    name: &str,
    partitions: Vec<String>,
    cursor: usize,
    config: EngineConfigBuilder,
) -> RunOutcome {
    let shape = Shape {
        partitions,
        cursor,
        replayable: true,
        again: false,
    };
    engine(config)
        .run(
            pipeline(name, [stream("events").read(ReadMode::Incremental)]),
            source(name, shape).await,
            memory(name).await,
        )
        .await
}

/// The positions stored state records of `name`'s stream, by partition.
async fn positions(name: &str) -> BTreeMap<String, rdlt_connector::PartitionState> {
    let state = stored(memory(name).await.as_ref(), name).await;
    state
        .streams
        .get(&StreamName::new("events").expect("a name"))
        .map(|stream| {
            let positions = stream.partitions.iter();
            positions
                .map(|(id, state)| (id.to_string(), state.clone()))
                .collect()
        })
        .unwrap_or_default()
}

/// Checks that `outcome` succeeded, and returns the partitions its report says it forgot.
fn succeeded(outcome: &RunOutcome) -> Vec<String> {
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    outcome
        .report
        .streams
        .get("events")
        .map(|stream| {
            assert_eq!(stream.forgotten.unlisted, 0);
            let partitions = stream.forgotten.partitions.iter();
            partitions.map(ToString::to_string).collect()
        })
        .unwrap_or_default()
}

#[tokio::test(start_paused = true)]
async fn a_done_partition_keeps_no_cursor_and_a_plan_naming_it_again_reads_nothing() {
    let name = "stored-done";
    let done = vec!["d0".to_owned(), "d1".to_owned()];
    assert_eq!(
        succeeded(&run(name, done.clone(), 8, commit_every(1)).await),
        Vec::<String>::new()
    );
    let read = reads(name);
    assert_eq!(read, 2);
    assert_eq!(crate::support::published_rows(name, "events"), 4);
    let recorded = positions(name).await;
    assert!(
        recorded
            .values()
            .all(|position| *position == rdlt_connector::PartitionState::Done)
    );
    // Named again, neither is read.
    succeeded(&run(name, done, 8, commit_every(1)).await);
    assert_eq!(reads(name), read);
    assert_eq!(crate::support::published_rows(name, "events"), 4);
}

#[tokio::test(start_paused = true)]
async fn done_markers_of_partitions_no_plan_names_stay_while_state_fits() {
    let name = "stored-unplanned";
    let first: Vec<String> = (0..3).map(|index| format!("d{index}")).collect();
    succeeded(&run(name, first, 8, commit_every(1)).await);
    let forgotten = succeeded(&run(name, vec!["d9".to_owned()], 8, commit_every(1)).await);
    assert_eq!(forgotten, Vec::<String>::new());
    assert_eq!(
        positions(name).await.keys().collect::<Vec<_>>(),
        ["d0", "d1", "d2", "d9"]
    );
}

#[tokio::test(start_paused = true)]
async fn churning_done_partitions_keep_state_bounded_and_loading_at_the_least_memory() {
    let name = "stored-churned";
    let least = EngineConfig::least_memory(16);
    let config = || commit_every(1).memory(least);
    let limit = config().build().expect("valid").state_limit();
    // The oldest done partitions, named to sort after the later ones, then a stream of cursors
    // that fills state to its limit.
    let oldest: Vec<String> = (0..10).map(|index| format!("dz{index}")).collect();
    succeeded(&run(name, oldest.clone(), 4000, config()).await);
    let filling = oldest
        .iter()
        .cloned()
        .chain((0..500).map(|index| format!("c{index}")));
    refused(&run(name, filling.collect(), 4000, config()).await);
    let cursors: Vec<String> = positions(name)
        .await
        .into_keys()
        .filter(|id| id.starts_with('c'))
        .collect();
    assert!(!cursors.is_empty());
    // Each later run reads the cursors' partitions, which add nothing, eight new done ones, and
    // the oldest done one, which it keeps.
    let kept = oldest[0].clone();
    let mut forgotten = Vec::new();
    let mut earlier: Vec<String> = oldest[1..].to_vec();
    for round in 0..6 {
        let rows = crate::support::published_rows(name, "events");
        let churned: Vec<String> = (0..8).map(|index| format!("dr{round}x{index}")).collect();
        let planned = cursors
            .iter()
            .chain(&churned)
            .chain([&kept])
            .cloned()
            .collect();
        let forgot = succeeded(&run(name, planned, 4000, config()).await);
        assert!(
            forgot.iter().all(|id| earlier.contains(id)),
            "round {round} forgot {forgot:?}"
        );
        assert_eq!(
            crate::support::published_rows(name, "events"),
            rows + 16,
            "round {round}"
        );
        assert!(carried(name).await <= limit);
        forgotten.extend(forgot);
        earlier.extend(churned);
    }
    // The oldest unplanned go first, and a partition that is not done is never forgotten.
    let unplanned = &oldest[1..];
    assert!(forgotten.len() > unplanned.len(), "{forgotten:?}");
    let mut first: Vec<String> = forgotten[..unplanned.len()].to_vec();
    first.sort();
    assert_eq!(first, unplanned);
    let recorded = positions(name).await;
    assert!(recorded.contains_key(&kept));
    assert!(cursors.iter().all(|id| recorded.contains_key(id)));
    assert!(forgotten.iter().all(|id| !recorded.contains_key(id)));
}

/// The files source over `root`.
async fn files(root: &std::path::Path) -> Arc<dyn rdlt_connector::Source> {
    let connected = source_factory::<rdlt_connector_reference::FilesSource>()
        .connect(json!({ "root": root }), ConnectContext::new())
        .await
        .expect("the files source connects");
    Arc::from(connected)
}

/// Replaces the files of stream `events` under `root` with `names`, a record each.
fn churn(root: &std::path::Path, names: &[String]) {
    let dir = root.join("events");
    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("the old files go");
    }
    std::fs::create_dir(&dir).expect("the stream's directory");
    for (id, name) in (0_u64..).zip(names) {
        std::fs::write(
            dir.join(format!("{name}.jsonl")),
            format!("{{\"id\":{id}}}\n"),
        )
        .expect("a file");
    }
}

#[tokio::test(start_paused = true)]
async fn churning_files_keep_state_bounded_at_the_least_memory() {
    let name = "stored-files";
    let base = tempfile::tempdir().expect("a temporary directory");
    // The least memory, its state held to 32 KiB.
    let limits = rdlt_wire::Limits {
        state_bytes: 32 << 10,
        ..rdlt_wire::Limits::default()
    };
    let config = || {
        commit_every(10)
            .memory(EngineConfig::least_memory(16))
            .limits(limits)
    };
    let limit = config().build().expect("valid").state_limit();
    // The files source reads in full: each run reads the files it lists in a cycle of its own,
    // whose first commit deletes the entries of the cycle before.
    for round in 0..12 {
        let names: Vec<String> = (0..40)
            .map(|index| format!("r{round:02}f{index:02}"))
            .collect();
        churn(base.path(), &names);
        let rows = crate::support::published_rows(name, "events");
        let outcome = engine(config())
            .run(
                pipeline(name, [stream("events")]),
                files(base.path()).await,
                memory(name).await,
            )
            .await;
        assert_eq!(succeeded(&outcome), Vec::<String>::new(), "round {round}");
        assert_eq!(
            crate::support::published_rows(name, "events"),
            rows + 40,
            "round {round}"
        );
        assert!(carried(name).await <= limit, "round {round}");
        // Each file read to its end is done, its partition's entry no cursor.
        let recorded = positions(name).await;
        let read: Vec<String> = names.iter().map(|file| format!("{file}.jsonl")).collect();
        assert_eq!(
            recorded.keys().cloned().collect::<Vec<_>>(),
            read,
            "round {round}"
        );
        assert!(
            recorded
                .values()
                .all(|position| *position == rdlt_connector::PartitionState::Done)
        );
    }
}

#[tokio::test(start_paused = true)]
async fn each_full_read_of_the_files_reads_every_file_from_its_start() {
    let name = "stored-appended";
    let base = tempfile::tempdir().expect("a temporary directory");
    churn(base.path(), &["first".to_owned()]);
    let load = || async {
        engine(commit_every(10))
            .run(
                pipeline(name, [stream("events")]),
                files(base.path()).await,
                memory(name).await,
            )
            .await
    };
    succeeded(&load().await);
    assert_eq!(crate::support::published_rows(name, "events"), 1);
    // A cycle of its own reads the first file again, what was appended to it with it, and the
    // new one; a file is a partition, done once read to its end within its cycle.
    let first = base.path().join("events").join("first.jsonl");
    let mut appended = std::fs::read_to_string(&first).expect("the file reads");
    appended.push_str("{\"id\":7}\n");
    std::fs::write(&first, appended).expect("the line is appended");
    std::fs::write(
        base.path().join("events").join("second.jsonl"),
        "{\"id\":8}\n",
    )
    .expect("a new file");
    succeeded(&load().await);
    assert_eq!(crate::support::published_rows(name, "events"), 4);
    let recorded = positions(name).await;
    assert_eq!(recorded.len(), 2);
    assert!(
        recorded
            .values()
            .all(|position| *position == rdlt_connector::PartitionState::Done)
    );
}
