//! What a read keeps for its decoder's schema and dictionaries is released however the read ends,
//! once; a read that would keep more than its admission lets it fails at the frame that would;
//! and an event its admission refuses fails the read.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use arrow_array::types::Int8Type;
use arrow_array::{ArrayRef, DictionaryArray, Int8Array, RecordBatch, StringArray};
use rdlt_connector::prelude::*;
use rdlt_connector::serve::Served;
use rdlt_connector::{
    Admission, BoxFuture, ConnectContext, LogLevel, PartitionFeed, PartitionId, Permit, PipelineId,
    Push, ReadMode, ReadRequest, Role, Source as _, SourceEvent, StreamState,
    admitted_partition_channel, source_factory,
};
use rdlt_host::{Connection, Options, RemoteSource};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::flow::NoCursor;
use crate::support::served;

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct KeyedConfig {
    /// How the read ends after its batches: `done`, `fails`, or `waits` for its stop.
    then: String,
}

/// A source of one stream, `keyed`, of two batches of one dictionary column, each with a
/// dictionary of its own, of a hundred and of a hundred and fifty kilobytes.
#[derive(Debug)]
struct Keyed {
    config: KeyedConfig,
}

#[source(id = "test.keyed")]
impl SourceConnector for Keyed {
    type Config = KeyedConfig;

    async fn connect(config: KeyedConfig, _: &ConnectContext) -> Result<Self> {
        Ok(Self { config })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new().with(KeyedStream)
    }
}

struct KeyedStream;

impl ReadStream<Keyed> for KeyedStream {
    type Cursor = NoCursor;

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(StreamName::new("keyed").expect("a valid stream name"))
    }

    async fn read(
        &self,
        source: &Keyed,
        _: &Partition,
        _: NoCursor,
        out: &mut Emitter<NoCursor>,
    ) -> Result<()> {
        for bytes in [100_000, 150_000] {
            let values = StringArray::from(vec!["x".repeat(bytes)]);
            let keyed =
                DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0]), Arc::new(values));
            let column: ArrayRef = Arc::new(keyed.expect("a valid dictionary"));
            out.batch(RecordBatch::try_from_iter([("tag", column)]).expect("a valid batch"))
                .await?;
        }
        match source.config.then.as_str() {
            "done" => Ok(()),
            "fails" => Err(ConnectorError::data("the source failed").with_code("keyed_failed")),
            _ => loop {
                out.log(LogLevel::Debug, "waiting").await?;
                tokio::time::sleep(Duration::from_millis(5)).await;
            },
        }
    }
}

/// Counts what a read keeps beside its events, and refuses pushes where told to.
#[derive(Default)]
struct Ledger {
    /// Bytes kept now.
    kept: AtomicU64,
    /// The most bytes kept at once.
    most: AtomicU64,
    charges: AtomicU64,
    releases: AtomicU64,
    refuses: AtomicBool,
    /// Bytes: the most a read may keep, where it is not zero.
    limit: AtomicU64,
}

struct Kept(Arc<Ledger>, u64);

impl Drop for Kept {
    fn drop(&mut self) {
        self.0.kept.fetch_sub(self.1, Ordering::SeqCst);
        self.0.releases.fetch_add(1, Ordering::SeqCst);
    }
}

struct Counting(Arc<Ledger>);

impl Admission for Counting {
    fn admit<'a>(&'a self, event: &'a SourceEvent) -> BoxFuture<'a, Result<Option<Permit>>> {
        Box::pin(async move {
            if self.0.refuses.load(Ordering::SeqCst) && matches!(event, SourceEvent::Push(_)) {
                let refused = ConnectorError::new(ConnectorErrorKind::Transient, "no room");
                return Err(refused.with_code("no_room"));
            }
            Ok(None)
        })
    }

    fn charge(&self, bytes: u64) -> Result<Permit> {
        let limit = self.0.limit.load(Ordering::SeqCst);
        if limit > 0 && bytes > limit {
            return Err(ConnectorError::exceeds(rdlt_connector::LimitExceeded {
                name: "read kept bytes",
                limit,
                actual: bytes,
            }));
        }
        let kept = self.0.kept.fetch_add(bytes, Ordering::SeqCst) + bytes;
        self.0.most.fetch_max(kept, Ordering::SeqCst);
        self.0.charges.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(Kept(Arc::clone(&self.0), bytes)))
    }
}

/// A read of the `keyed` stream of a source that ends as `then` says, into a channel `ledger`
/// admits: the read, and the channel's feed.
async fn reading(
    then: &str,
    ledger: &Arc<Ledger>,
) -> (tokio::task::JoinHandle<Result<()>>, PartitionFeed) {
    let io = served(Served::new().with_source(source_factory::<Keyed>()));
    let config = serde_json::json!({ "then": then });
    let connection = Connection::connect(io, Role::Source, &config, Options::default())
        .await
        .expect("the source handshakes");
    let source = RemoteSource::new(connection);
    let admission = Arc::new(Counting(Arc::clone(ledger)));
    let (sink, feed) =
        admitted_partition_channel(NonZeroUsize::new(8).expect("not zero"), admission);
    let request = ReadRequest::new(
        StreamName::new("keyed").expect("a valid stream name"),
        Partition::single(),
        None,
    );
    let read = tokio::spawn(async move { source.read(request, sink).await });
    (read, feed)
}

/// Takes `count` batches from `feed`.
async fn batches(feed: &mut PartitionFeed, count: usize) {
    let mut taken = 0;
    while taken < count {
        let event = tokio::time::timeout(Duration::from_secs(30), feed.recv()).await;
        match event.expect("events arrive") {
            Some(SourceEvent::Push(Push::Arrow(_))) => taken += 1,
            Some(_) => {}
            None => panic!("the read ended after {taken} batches"),
        }
    }
}

/// What `ledger` counted: bytes kept now, charges and releases.
fn counted(ledger: &Ledger) -> (u64, u64, u64) {
    (
        ledger.kept.load(Ordering::SeqCst),
        ledger.charges.load(Ordering::SeqCst),
        ledger.releases.load(Ordering::SeqCst),
    )
}

/// Asserts each dictionary was charged alone, and each charge released once.
fn released_once(ledger: &Ledger) {
    let (kept, charges, releases) = counted(ledger);
    assert_eq!(kept, 0, "{charges} charges, {releases} releases");
    // The schema, then it with the first batch's dictionary, which the second's replaced.
    assert_eq!((charges, releases), (3, 3));
    // The dictionary replaced was released before the next was charged.
    let most = ledger.most.load(Ordering::SeqCst);
    assert!((150_000..250_000).contains(&most), "{most} bytes at once");
}

#[tokio::test]
async fn a_read_that_ends_releases_what_it_kept() {
    let ledger = Arc::new(Ledger::default());
    let (read, mut feed) = reading("done", &ledger).await;
    batches(&mut feed, 2).await;
    read.await
        .expect("the read ran")
        .expect("the read succeeds");
    released_once(&ledger);
}

#[tokio::test]
async fn a_read_that_fails_releases_what_it_kept() {
    let ledger = Arc::new(Ledger::default());
    let (read, mut feed) = reading("fails", &ledger).await;
    batches(&mut feed, 2).await;
    let failed = read
        .await
        .expect("the read ran")
        .expect_err("the read fails");
    assert_eq!(failed.code(), Some("keyed_failed"));
    released_once(&ledger);
}

#[tokio::test]
async fn a_read_the_engine_stops_releases_what_it_kept() {
    let ledger = Arc::new(Ledger::default());
    let (read, mut feed) = reading("waits", &ledger).await;
    batches(&mut feed, 2).await;
    assert!(
        counted(&ledger).0 >= 100_000,
        "the read keeps its dictionary"
    );
    feed.stop();
    while feed.recv().await.is_some() {}
    // A stopped read ends as its source ends it; what it kept goes with it either way.
    drop(read.await.expect("the read ran"));
    released_once(&ledger);
}

#[tokio::test]
async fn a_read_that_is_dropped_releases_what_it_kept() {
    let ledger = Arc::new(Ledger::default());
    let (read, mut feed) = reading("waits", &ledger).await;
    batches(&mut feed, 2).await;
    assert!(
        counted(&ledger).0 >= 100_000,
        "the read keeps its dictionary"
    );
    read.abort();
    assert!(read.await.expect_err("the read was dropped").is_cancelled());
    released_once(&ledger);
}

#[tokio::test]
async fn an_event_its_admission_refuses_fails_the_read_with_why() {
    let ledger = Arc::new(Ledger::default());
    ledger.refuses.store(true, Ordering::SeqCst);
    let (read, _feed) = reading("waits", &ledger).await;
    let ended = tokio::time::timeout(Duration::from_secs(30), read).await;
    let refused = ended
        .expect("the read ends")
        .expect("the read ran")
        .expect_err("the read fails");
    assert_eq!(refused.code(), Some("no_room"));
    assert!(refused.is_retryable());
    // The schema and the first batch's dictionary were kept, and went with the read.
    assert_eq!(counted(&ledger), (0, 2, 2));
}

#[tokio::test]
async fn a_read_that_would_keep_more_than_it_may_fails_at_the_frame_that_would() {
    let ledger = Arc::new(Ledger::default());
    // The first batch's dictionary, of a hundred kilobytes, is within the limit; the second's,
    // of a hundred and fifty, is not.
    ledger.limit.store(120_000, Ordering::SeqCst);
    let (read, mut feed) = reading("waits", &ledger).await;
    batches(&mut feed, 1).await;
    let ended = tokio::time::timeout(Duration::from_secs(30), read).await;
    let refused = ended
        .expect("the read ends")
        .expect("the read ran")
        .expect_err("the read fails");
    assert_eq!(refused.code(), Some("limit_exceeded"));
    let limit = refused.limit().expect("the limit passed");
    assert_eq!((limit.name, limit.limit), ("read kept bytes", 120_000));
    assert!(limit.actual >= 150_000, "{limit:?}");
    // The batch that came with the dictionary never reached the engine.
    let mut after = 0;
    while let Some(event) = feed.recv().await {
        after += usize::from(matches!(event, SourceEvent::Push(_)));
    }
    assert_eq!(after, 0);
    // What the read kept went with it, and never passed the limit.
    assert_eq!(counted(&ledger), (0, 2, 2));
    assert!(ledger.most.load(Ordering::SeqCst) <= 120_000);
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
struct WideConfig {
    /// Bytes: the single value each read's dictionary holds.
    dictionary: usize,
}

/// A source of one stream, `wide`, of sixteen partitions: each read sends a batch whose
/// dictionary is one value of the configured bytes, a checkpoint, then eight batches of ten
/// thousand rows that use the dictionary, each with its checkpoint.
#[derive(Debug)]
struct Wide {
    config: WideConfig,
}

#[source(id = "test.wide")]
impl SourceConnector for Wide {
    type Config = WideConfig;

    async fn connect(config: WideConfig, _: &ConnectContext) -> Result<Self> {
        Ok(Self { config })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new().with(WideStream)
    }
}

struct WideStream;

/// Rows each batch of a `wide` read holds.
const WIDE_ROWS: usize = 10_000;

impl ReadStream<Wide> for WideStream {
    type Cursor = String;

    fn spec(&self) -> StreamSpec {
        let name = StreamName::new("wide").expect("a valid stream name");
        StreamSpec::new(name).with_read_modes([ReadMode::Full, ReadMode::Incremental])
    }

    async fn partitions(&self, _: &Wide, _: &StreamState) -> Result<Vec<Partition>> {
        let part = |index| {
            let id = PartitionId::parse(format!("p{index}"));
            Partition::new(id.expect("a valid partition id"))
        };
        Ok((0..16).map(part).collect())
    }

    async fn read(
        &self,
        source: &Wide,
        _: &Partition,
        _: String,
        out: &mut Emitter<String>,
    ) -> Result<()> {
        let values = Arc::new(StringArray::from(vec![
            "x".repeat(source.config.dictionary),
        ]));
        for step in 0..9 {
            // The first batch names the value once, so the engine holds little beside what
            // the read keeps; the rest are keys alone.
            let rows = if step == 0 { 1 } else { WIDE_ROWS };
            let keys = Int8Array::from(vec![if step == 0 { Some(0) } else { None }; rows]);
            let keyed = DictionaryArray::<Int8Type>::try_new(keys, values.clone());
            let column: ArrayRef = Arc::new(keyed.expect("a valid dictionary"));
            out.batch(RecordBatch::try_from_iter([("tag", column)]).expect("a valid batch"))
                .await?;
            out.checkpoint(&format!("{step}")).await?;
        }
        Ok(())
    }
}

/// Loads the `wide` stream, each of whose sixteen reads keeps a dictionary of `dictionary`
/// bytes, through an engine of the default budget and partitions.
async fn wide(store: &str, dictionary: usize) -> rdlt_engine::RunOutcome {
    use rdlt_engine::{EngineConfig, PipelinePlan, RetryPolicy, StreamPlan};
    let io = served(Served::new().with_source(source_factory::<Wide>()));
    let config = serde_json::json!({ "dictionary": dictionary });
    let connection = Connection::connect(io, Role::Source, &config, Options::default())
        .await
        .expect("the source handshakes");
    let source = RemoteSource::new(connection);
    let destination = crate::support::memory_destination(store, Options::default()).await;
    let config = EngineConfig::builder()
        .retry(RetryPolicy::default().max_attempts(1))
        .build()
        .expect("the defaults are valid");
    assert_eq!(config.memory().get(), 256 << 20);
    assert_eq!(config.partitions().get(), 16);
    let threads = NonZeroUsize::new(4).expect("not zero");
    let pool = rdlt_engine::RayonPool::new(threads).expect("the compute pool starts");
    let engine = rdlt_engine::Engine::new(config, Arc::new(rdlt_engine::SystemEnv::new(pool)));
    let stream = StreamPlan::new(StreamName::new("wide").expect("a valid stream name"));
    let plan = PipelinePlan::new(PipelineId::parse(store).expect("a valid id"), [stream]);
    let run = engine.run(
        plan.expect("a valid plan"),
        Arc::new(source),
        Arc::new(destination),
    );
    tokio::time::timeout(Duration::from_secs(300), run)
        .await
        .expect("no read waits on the budget for what another keeps")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sixteen_reads_keeping_all_they_may_leave_pushes_and_checkpoints_flowing() {
    // Each read may keep four mebibytes of the default budget: these keep nearly all of it.
    let outcome = wide("wide_within", (4 << 20) - (64 << 10)).await;
    assert_eq!(
        outcome.report.status,
        rdlt_engine::RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(outcome.report.rows, 16 * (1 + 8 * WIDE_ROWS as u64));
    // Sixteen reads kept sixty megabytes between them, beside what was pushed.
    let peak = outcome.report.peak_memory;
    assert!((60 << 20..=256 << 20).contains(&peak), "{peak} bytes");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_keeping_more_than_its_part_of_the_budget_fails_naming_the_limit() {
    let outcome = wide("wide_beyond", (4 << 20) + 1).await;
    let error = outcome.error.expect("the run fails");
    assert_eq!(
        (error.kind(), error.code()),
        (rdlt_engine::ErrorKind::Source, Some("limit_exceeded"))
    );
    let said = format!("{:?}", error.report());
    assert!(said.contains("read kept bytes"), "{said}");
    assert!(said.contains("over the limit of 4194304"), "{said}");
    assert_eq!(outcome.report.rows, 0);
    assert!(outcome.report.peak_memory <= 256 << 20);
}
