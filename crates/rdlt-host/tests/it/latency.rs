//! Calls over a link with a round trip of tens of milliseconds: credit and HTTP/2's windows keep
//! mebibytes of a call in flight, so a call over a distant link is slowed by a round trip only as
//! often as its windows fill.
//!
//! Each call is made twice, over a socket and over a link that delays every read; what the link
//! adds is its round trips, whatever the machine's speed.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use rdlt_connector::serve::Served;
use rdlt_connector::{
    Destination as _, Partition, Push, ReadRequest, Role, SegmentId, Source as _, SourceEvent,
    StreamName, destination_factory, partition_channel, source_factory,
};
use rdlt_connector_reference::MemoryDestination;
use rdlt_host::{Connection, Options, RemoteDestination, RemoteSource};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::time::Instant;

use crate::flow::Blobs;
use crate::sessions::{context, table};
use crate::support::served;

/// How long the distant link takes to carry bytes one way.
const DELAY: Duration = Duration::from_millis(25);

/// Bytes: what a call moves, eight frames of eight megabytes.
const MOVED: u64 = 64_000_000;

/// How far a connector is from its host.
#[derive(Clone, Copy, Debug)]
enum Link {
    Near,
    Distant,
}

impl Link {
    /// The host's end of this link to `connector`.
    fn to(self, connector: UnixStream) -> UnixStream {
        match self {
            Self::Near => connector,
            Self::Distant => distant(connector),
        }
    }
}

/// The host's end of a link to `connector` that carries each read [`DELAY`] later, both ways.
fn distant(connector: UnixStream) -> UnixStream {
    let (host, link) = UnixStream::pair().expect("a socket pair");
    let (from_host, to_host) = link.into_split();
    let (from_connector, to_connector) = connector.into_split();
    tokio::spawn(carry(from_host, to_connector));
    tokio::spawn(carry(from_connector, to_host));
    host
}

/// Copies `from` to `to`, each read written [`DELAY`] after it arrived, in order.
async fn carry(mut from: OwnedReadHalf, mut to: OwnedWriteHalf) {
    let (arrived, mut arriving) = tokio::sync::mpsc::unbounded_channel::<(Instant, Vec<u8>)>();
    tokio::spawn(async move {
        while let Some((due, bytes)) = arriving.recv().await {
            tokio::time::sleep_until(due).await;
            if to.write_all(&bytes).await.is_err() {
                return;
            }
        }
    });
    let mut buffer = vec![0; 1 << 16];
    while let Ok(read @ 1..) = from.read(&mut buffer).await {
        let due = Instant::now() + DELAY;
        if arrived.send((due, buffer[..read].to_vec())).is_err() {
            return;
        }
    }
}

/// How long writing [`MOVED`] bytes to the memory destination over `link` takes, its flush
/// included.
async fn write(link: Link, store: &str) -> Duration {
    let rows = i64::try_from(MOVED / 8 / 8).expect("fits");
    let values: Vec<i64> = (0..rows).collect();
    let batch = RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(values)) as _)])
        .expect("a valid batch");
    let destination = Served::new().with_destination(destination_factory::<MemoryDestination>());
    let config = serde_json::json!({ "store": store });
    let io = link.to(served(destination));
    let connection = Connection::connect(io, Role::Destination, &config, Options::default())
        .await
        .expect("the destination handshakes");
    let destination = RemoteDestination::new(connection).expect("its capabilities are declared");
    let mut opened = destination
        .open(&context())
        .await
        .expect("the session opens");
    let mut writer = opened
        .session
        .writer(&table())
        .await
        .expect("the writer opens");
    let started = Instant::now();
    for segment in 1..=8 {
        writer
            .write(SegmentId(segment), batch.clone())
            .await
            .expect("the batch is written");
    }
    let stats = writer.flush().await.expect("the batches are staged");
    assert_eq!(stats.rows, 8 * rows.unsigned_abs());
    started.elapsed()
}

/// How long reading [`MOVED`] bytes from a source over `link` takes.
async fn read(link: Link) -> Duration {
    let source = Served::new().with_source(source_factory::<Blobs>());
    let config = serde_json::json!({ "bytes": 1_000_000, "ahead": 7, "alike": true });
    let io = link.to(served(source));
    let connection = Connection::connect(io, Role::Source, &config, Options::default())
        .await
        .expect("the source handshakes");
    let source = RemoteSource::new(connection);
    let (sink, mut feed) = partition_channel(std::num::NonZeroUsize::new(4).expect("not zero"));
    let request = ReadRequest::new(
        StreamName::new("blobs").expect("a valid stream name"),
        Partition::single(),
        None,
    );
    let started = Instant::now();
    let reading = tokio::spawn(async move { source.read(request, sink).await });
    let mut rows = 0;
    while rows < 64 {
        match feed.recv().await {
            Some(SourceEvent::Push(Push::Arrow(batch))) => rows += batch.num_rows(),
            other => panic!("the read ended or sent another event: {other:?}"),
        }
    }
    reading.abort();
    started.elapsed()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_distant_connector_costs_a_round_trip_for_each_window_its_calls_move() {
    // A round trip for every two mebibytes, half what each call's windows hold, and a few to
    // open and close the call.
    let windows = u32::try_from(MOVED / (2 << 20)).expect("few windows");
    let added = 2 * DELAY * (windows + 4);
    let calls = async {
        let near = write(Link::Near, "latency_near").await;
        let distant = write(Link::Distant, "latency_distant").await;
        let written = distant.saturating_sub(near);
        let near = read(Link::Near).await;
        let distant = read(Link::Distant).await;
        (written, distant.saturating_sub(near))
    };
    let (written, read) = tokio::time::timeout(Duration::from_secs(60), calls)
        .await
        .expect("the calls end");
    assert!(written <= added, "the link added {written:?} to the write");
    assert!(read <= added, "the link added {read:?} to the read");
}
