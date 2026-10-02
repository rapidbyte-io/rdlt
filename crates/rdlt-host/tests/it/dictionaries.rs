//! What a read keeps for its decoder's dictionaries is released however the read ends, once, and
//! an event its admission refuses fails the read.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use arrow_array::types::Int8Type;
use arrow_array::{ArrayRef, DictionaryArray, Int8Array, RecordBatch, StringArray};
use rdlt_connector::prelude::*;
use rdlt_connector::serve::Served;
use rdlt_connector::{
    Admission, BoxFuture, ConnectContext, LogLevel, PartitionFeed, Permit, Push, ReadRequest, Role,
    Source as _, SourceEvent, admitted_partition_channel, source_factory,
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

    fn charge(&self, bytes: u64) -> Permit {
        let kept = self.0.kept.fetch_add(bytes, Ordering::SeqCst) + bytes;
        self.0.most.fetch_max(kept, Ordering::SeqCst);
        self.0.charges.fetch_add(1, Ordering::SeqCst);
        Box::new(Kept(Arc::clone(&self.0), bytes))
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
    // The second batch's dictionary replaced the first's.
    assert_eq!((charges, releases), (2, 2));
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
    // The first batch's dictionary was kept, and went with the read.
    assert_eq!(counted(&ledger), (0, 1, 1));
}
