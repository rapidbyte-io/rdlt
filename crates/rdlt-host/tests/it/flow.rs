//! Flow control and failures on busy connections: reads the engine cannot keep up with, writes a
//! destination does not take or refuses, and errors and frames at the limits.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use rdlt_connector::prelude::*;
use rdlt_connector::serve::Served;
use rdlt_connector::{
    ConnectContext, Destination as _, DestinationWriter, LoadId, OpenContext, PipelineId,
    ReadRequest, Role, SchemaVersion, SegmentId, Source as _, TablePath, TableRef,
    destination_factory, partition_channel, source_factory,
};
use rdlt_connector_reference::MemoryDestination;
use rdlt_host::{CONNECTOR_LOST, Connection, DEADLINE_EXCEEDED, Deadlines, Options};
use rdlt_host::{RemoteDestination, RemoteSource};
use rdlt_wire::Limits;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::support::connectors::{Writes, Writing};
use crate::support::{served, served_within};

/// Options that notice a lost connector within a few tenths of a second.
fn quick() -> Options {
    Options {
        heartbeat: Duration::from_millis(50),
        missed: 3,
        ..Options::default()
    }
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct BlobsConfig {
    /// The length of the message the check fails with; zero checks cleanly.
    #[serde(default)]
    message: usize,
}

/// A source of one endless stream, `blobs`, of 256 KiB strings.
#[derive(Debug)]
pub(crate) struct Blobs {
    config: BlobsConfig,
}

#[source(id = "test.blobs")]
impl SourceConnector for Blobs {
    type Config = BlobsConfig;

    async fn connect(config: BlobsConfig, _: &ConnectContext) -> Result<Self> {
        Ok(Self { config })
    }

    async fn check(&self) -> Result<()> {
        if self.config.message > 0 {
            let message = "x".repeat(self.config.message);
            return Err(ConnectorError::new(ConnectorErrorKind::Auth, message).with_code("denied"));
        }
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new().with(BlobStream)
    }
}

struct BlobStream;

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub(crate) struct NoCursor;

impl ReadStream<Blobs> for BlobStream {
    type Cursor = NoCursor;

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(StreamName::new("blobs").expect("a valid stream name"))
    }

    async fn read(
        &self,
        _: &Blobs,
        _: &Partition,
        _: NoCursor,
        out: &mut Emitter<NoCursor>,
    ) -> Result<()> {
        let blob = "b".repeat(256 * 1024);
        loop {
            let column: ArrayRef = Arc::new(StringArray::from(vec![blob.clone()]));
            let batch = RecordBatch::try_from_iter([("b", column)]).expect("a valid batch");
            out.batch(batch).await?;
        }
    }
}

async fn blobs(config: serde_json::Value, options: Options) -> RemoteSource {
    let io = served(Served::new().with_source(source_factory::<Blobs>()));
    let connection = Connection::connect(io, Role::Source, &config, options)
        .await
        .expect("the source handshakes");
    RemoteSource::new(connection)
}

#[tokio::test(flavor = "multi_thread")]
async fn backpressured_partitions_do_not_lose_a_live_source() {
    let source = Arc::new(blobs(serde_json::json!({}), quick()).await);
    let mut feeds = Vec::new();
    for _ in 0..8 {
        // The engine takes one event of each read and no more, as when its lanes are full.
        let (sink, mut feed) = partition_channel(NonZeroUsize::new(1).expect("not zero"));
        let source = Arc::clone(&source);
        tokio::spawn(async move {
            let request = ReadRequest {
                stream: StreamName::new("blobs").expect("a valid stream name"),
                partition: Partition::single(),
                cursor: None,
            };
            source.read(request, sink).await
        });
        feed.recv().await.expect("the read sends");
        feeds.push(feed);
    }
    // Well past the heartbeat's patience: the source is live, only the engine is behind.
    tokio::time::sleep(Duration::from_secs(1)).await;
    source.check().await.expect("the source is still connected");
}

#[tokio::test]
async fn an_error_at_the_control_string_limit_keeps_its_kind_and_code() {
    let limit = usize::try_from(rdlt_wire::limits::CONTROL_STRING_BYTES).expect("fits");
    for length in [limit, 4 * limit] {
        let source = blobs(serde_json::json!({ "message": length }), Options::default()).await;
        let error = source.check().await.unwrap_err();
        assert_eq!(
            (error.kind(), error.code()),
            (ConnectorErrorKind::Auth, Some("denied")),
            "{length}"
        );
        assert_eq!(error.to_string().len(), limit, "{length}");
    }
}

fn table() -> TableRef {
    TableRef {
        path: TablePath::new(["items"]).expect("a valid table path"),
        name: Arc::from("items"),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    }
}

fn ids(count: i64) -> RecordBatch {
    let values: Vec<i64> = (0..count).collect();
    RecordBatch::try_from_iter([("id", Arc::new(arrow_array::Int64Array::from(values)) as _)])
        .expect("a valid batch")
}

/// A writer of `table` in a session of the destination `served` over `store`, within `limits`.
async fn writer(
    served: Served,
    limits: Limits,
    store: &str,
    options: Options,
) -> Box<dyn DestinationWriter> {
    let io = served_within(served, limits);
    let config = serde_json::json!({ "store": store });
    let connection = Connection::connect(io, Role::Destination, &config, options)
        .await
        .expect("the destination handshakes");
    let destination = RemoteDestination::new(connection).expect("its capabilities are declared");
    let context = OpenContext {
        pipeline: PipelineId::parse("flow").expect("a valid pipeline id"),
        load_id: LoadId::from_parts(std::time::UNIX_EPOCH, 1),
    };
    let mut opened = destination.open(&context).await.expect("the session opens");
    opened
        .session
        .writer(&table())
        .await
        .expect("the writer opens")
}

/// Writes `batch` again and again until a write fails, or the flush once `count` succeed.
async fn write_until_failed(
    writer: &mut dyn DestinationWriter,
    batch: &RecordBatch,
    count: u64,
) -> ConnectorError {
    for segment in 1..=count {
        if let Err(error) = writer.write(SegmentId(segment), batch.clone()).await {
            return error;
        }
    }
    writer.flush().await.expect_err("the flush fails")
}

#[tokio::test]
async fn a_failed_write_keeps_its_error_through_the_remote_writer() {
    let served = Served::new().with_destination(Writes::factory(Writing::Fails));
    let mut writer = writer(served, Limits::default(), "flow_fails", Options::default()).await;
    let error = write_until_failed(writer.as_mut(), &ids(10), 200).await;
    assert_eq!(
        (error.kind(), error.to_string()),
        (ConnectorErrorKind::Data, "the write was refused".to_owned())
    );
}

#[tokio::test]
async fn a_stalled_writer_fails_once_its_write_ack_deadline_passes() {
    let served = Served::new().with_destination(Writes::factory(Writing::Stalls));
    let options = Options {
        deadlines: Deadlines {
            write_ack: Duration::from_millis(300),
            ..Deadlines::default()
        },
        ..quick()
    };
    let mut writer = writer(served, Limits::default(), "flow_stalls", options).await;
    // Batches of about 160 KB, so the transport's windows fill before the connector's credit.
    let written = tokio::time::timeout(
        Duration::from_secs(10),
        write_until_failed(writer.as_mut(), &ids(20_000), 40),
    )
    .await
    .expect("the writer does not hang past its deadline");
    assert_eq!(written.code(), Some(DEADLINE_EXCEEDED));
}

#[tokio::test]
async fn a_write_beyond_the_connectors_frame_limit_is_refused_typed() {
    let limits = Limits {
        frame_bytes: 64 * 1024,
        ..Limits::default()
    };
    let served = Served::new().with_destination(destination_factory::<MemoryDestination>());
    let mut writer = writer(served, limits, "flow_limit", Options::default()).await;
    let error = write_until_failed(writer.as_mut(), &ids(200_000), 1).await;
    assert_eq!(
        error.limit().map(|limit| (limit.name, limit.limit)),
        Some(("frame bytes", 64 * 1024))
    );
    assert_ne!(error.code(), Some(CONNECTOR_LOST));
}
