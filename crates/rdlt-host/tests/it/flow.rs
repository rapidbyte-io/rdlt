//! Flow control and failures on busy connections: reads the engine cannot keep up with, writes a
//! destination does not take or refuses, and errors and frames at the limits.

use std::num::{NonZeroU32, NonZeroUsize};
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
use rdlt_wire::limits::MIN_FRAME_BYTES;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::support::connectors::{Gate, Writes, Writing};
use crate::support::{served, served_within};

/// Options that notice a lost connector within a few tenths of a second.
fn quick() -> Options {
    Options {
        heartbeat: Duration::from_millis(50),
        missed: NonZeroU32::new(3).expect("not zero"),
        ..Options::default()
    }
}

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(crate) struct BlobsConfig {
    /// The length of the message the check fails with; zero checks cleanly.
    #[serde(default)]
    message: usize,
    /// The length of each string; zero for 256 KiB.
    #[serde(default)]
    bytes: usize,
    /// Rows ahead of each string, in its batch.
    #[serde(default)]
    ahead: usize,
    /// Whether the rows ahead are strings of the same length, not of one byte.
    #[serde(default)]
    alike: bool,
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
        source: &Blobs,
        _: &Partition,
        _: NoCursor,
        out: &mut Emitter<NoCursor>,
    ) -> Result<()> {
        let bytes = match source.config.bytes {
            0 => 256 * 1024,
            bytes => bytes,
        };
        let blob = "b".repeat(bytes);
        loop {
            let ahead = if source.config.alike {
                blob.as_str()
            } else {
                "b"
            };
            let mut rows = vec![ahead.to_owned(); source.config.ahead];
            rows.push(blob.clone());
            let column: ArrayRef = Arc::new(StringArray::from(rows));
            let batch = RecordBatch::try_from_iter([("b", column)]).expect("a valid batch");
            out.batch(batch).await?;
        }
    }
}

async fn blobs(config: serde_json::Value, options: &Options) -> RemoteSource {
    let io = served(Served::new().with_source(source_factory::<Blobs>()));
    let connection = Connection::connect(io, Role::Source, &config, *options)
        .await
        .expect("the source handshakes");
    RemoteSource::new(connection)
}

#[tokio::test(flavor = "multi_thread")]
async fn backpressured_partitions_do_not_lose_a_live_source() {
    let source = Arc::new(blobs(serde_json::json!({}), &quick()).await);
    let mut feeds = Vec::new();
    for _ in 0..8 {
        // The engine takes one event of each read and no more, as when its lanes are full.
        let (sink, mut feed) = partition_channel(NonZeroUsize::new(1).expect("not zero"));
        let source = Arc::clone(&source);
        tokio::spawn(async move {
            let request = ReadRequest::new(
                StreamName::new("blobs").expect("a valid stream name"),
                Partition::single(),
                None,
            );
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
        let source = blobs(
            serde_json::json!({ "message": length }),
            &Options::default(),
        )
        .await;
        let error = source.check().await.unwrap_err();
        assert_eq!(
            (error.kind(), error.code()),
            (ConnectorErrorKind::Auth, Some("denied")),
            "{length}"
        );
        // Cut by the connector to the control string limit, and by the host to what it
        // keeps of an error's text.
        assert_eq!(
            error.to_string().len(),
            rdlt_connector::limits::MAX_ERROR_TEXT_BYTES,
            "{length}"
        );
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
pub(crate) async fn writer(
    served: Served,
    limits: Limits,
    store: &str,
    options: &Options,
) -> Box<dyn DestinationWriter> {
    let io = served_within(served, limits);
    let config = serde_json::json!({ "store": store });
    let connection = Connection::connect(io, Role::Destination, &config, *options)
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
    let mut writer = writer(served, Limits::default(), "flow_fails", &Options::default()).await;
    let error = write_until_failed(writer.as_mut(), &ids(10), 200).await;
    assert_eq!(
        (error.kind(), error.to_string()),
        (ConnectorErrorKind::Data, "the write was refused".to_owned())
    );
}

#[tokio::test]
async fn a_writer_that_panics_fails_the_write_with_an_internal_error() {
    let served = Served::new().with_destination(Writes::factory(Writing::Panics));
    let mut writer = writer(
        served,
        Limits::default(),
        "flow_panics",
        &Options::default(),
    )
    .await;
    let error = write_until_failed(writer.as_mut(), &ids(10), 200).await;
    // The panic fails the write as the connector's error, not as a write it ended out of turn.
    assert_eq!(
        (error.kind(), error.code()),
        (ConnectorErrorKind::Internal, None)
    );
    assert!(error.to_string().contains("panicked"), "{error}");
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
    let mut writer = writer(served, Limits::default(), "flow_stalls", &options).await;
    // Batches of about 160 KB: 27 of them spend the credit floor, and the connector's writer
    // stalls on the first it takes, so beyond the first's no credit comes back and the writer
    // waits for credit that does not come.
    let written = tokio::time::timeout(
        Duration::from_secs(10),
        write_until_failed(writer.as_mut(), &ids(20_000), 40),
    )
    .await
    .expect("the writer does not hang past its deadline");
    assert_eq!(written.code(), Some(DEADLINE_EXCEEDED));
}

#[tokio::test]
async fn a_schema_that_could_not_be_sent_is_sent_again_with_the_next_write() {
    static GATE: Gate = Gate::new();
    let served = Served::new().with_destination(Writes::factory(Writing::Gated(&GATE)));
    let options = Options {
        deadlines: Deadlines {
            write_ack: Duration::from_millis(300),
            ..Deadlines::default()
        },
        ..Options::default()
    };
    let mut writer = writer(served, Limits::default(), "flow_gated", &options).await;
    // Frames of 4.8 MB, the first beyond the credit the connector opens with. Its writer takes
    // the first and waits, which returns the first's credit and grows the window to two frames:
    // the second waits decoded for the writer, the third to be decoded, and no credit is left.
    for _ in 0..3 {
        writer
            .write(SegmentId(1), ids(600_000))
            .await
            .expect("the frame goes on the credit left");
    }
    // A batch of another schema: its schema waits for credit, and its deadline passes.
    let texts: ArrayRef = Arc::new(StringArray::from(vec!["some text"; 10]));
    let texts = RecordBatch::try_from_iter([("t", texts)]).expect("a valid batch");
    let late = writer
        .write(SegmentId(1), texts.clone())
        .await
        .expect_err("no credit returns in time");
    assert_eq!(late.code(), Some(DEADLINE_EXCEEDED));
    // Written again once the connector's writer goes on, the batch follows its schema.
    GATE.open();
    writer
        .write(SegmentId(1), texts)
        .await
        .expect("the batch is written");
    writer
        .flush()
        .await
        .expect("the connector decoded every frame");
    let kept = GATE.kept.lock().expect("the lock is not poisoned");
    let rows: Vec<_> = kept.iter().map(|(_, batch)| batch.num_rows()).collect();
    assert_eq!(rows, [600_000, 600_000, 600_000, 10]);
}

#[tokio::test]
async fn a_row_beyond_the_connectors_frame_limit_is_refused_typed() {
    let limits = Limits {
        frame_bytes: MIN_FRAME_BYTES,
        ..Limits::default()
    };
    let served = Served::new().with_destination(destination_factory::<MemoryDestination>());
    let mut writer = writer(served, limits, "flow_limit", &Options::default()).await;
    // Rows that fit a frame go, as many frames as they need; a row that fits none is refused
    // where the cut reaches it, and its caller must not commit the segment it was written to.
    let small: ArrayRef = Arc::new(StringArray::from(vec!["some text"; 500_000]));
    let rows = RecordBatch::try_from_iter([("b", small)]).expect("a valid batch");
    writer
        .write(SegmentId(1), rows)
        .await
        .expect("a batch of small rows is cut to the limit");
    let blob: ArrayRef = Arc::new(StringArray::from(vec!["b".repeat(5 << 20)]));
    let row = RecordBatch::try_from_iter([("b", blob)]).expect("a valid batch");
    let error = write_until_failed(writer.as_mut(), &row, 1).await;
    assert_eq!(
        (
            error.code(),
            error.limit().map(|limit| (limit.name, limit.limit))
        ),
        (
            Some("limit_exceeded"),
            Some(("frame bytes", MIN_FRAME_BYTES))
        )
    );
    assert_ne!(error.code(), Some(CONNECTOR_LOST));
    // The write goes on: a row of the refused row's schema that fits is written.
    let short: ArrayRef = Arc::new(StringArray::from(vec!["b"]));
    let row = RecordBatch::try_from_iter([("b", short)]).expect("a valid batch");
    writer
        .write(SegmentId(2), row)
        .await
        .expect("a row within the limit is written");
    writer.flush().await.expect("the rows are staged");
}

#[tokio::test]
async fn a_read_frame_beyond_the_hosts_limit_is_refused_typed() {
    let options = Options {
        limits: Limits {
            frame_bytes: MIN_FRAME_BYTES,
            ..Limits::default()
        },
        ..Options::default()
    };
    let source = blobs(serde_json::json!({ "bytes": 5 << 20 }), &options).await;
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(4).expect("not zero"));
    let request = ReadRequest::new(
        StreamName::new("blobs").expect("a valid stream name"),
        Partition::single(),
        None,
    );
    let reading = tokio::spawn(async move { source.read(request, sink).await });
    while feed.recv().await.is_some() {}
    let error = reading.await.expect("the read ends").unwrap_err();
    assert_eq!(
        error.limit().map(|limit| (limit.name, limit.limit)),
        Some(("frame bytes", MIN_FRAME_BYTES))
    );
}

#[tokio::test]
async fn a_row_beyond_the_hosts_frame_limit_ends_the_read_after_the_rows_before_it() {
    let options = Options {
        limits: Limits {
            frame_bytes: MIN_FRAME_BYTES,
            ..Limits::default()
        },
        ..Options::default()
    };
    let source = blobs(
        serde_json::json!({ "bytes": 5 << 20, "ahead": 100 }),
        &options,
    )
    .await;
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(4).expect("not zero"));
    let request = ReadRequest::new(
        StreamName::new("blobs").expect("a valid stream name"),
        Partition::single(),
        None,
    );
    let reading = tokio::spawn(async move { source.read(request, sink).await });
    let mut pushed = Vec::new();
    while let Some(event) = feed.recv().await {
        match event {
            rdlt_connector::SourceEvent::Push(rdlt_connector::Push::Arrow(batch)) => {
                pushed.push(batch.num_rows());
            }
            other => panic!("an event other than a push of rows: {other:?}"),
        }
    }
    // The rows ahead of the row that fits no frame arrive, in a segment no checkpoint ends: the
    // read fails with the limit, and the engine discards them with its attempt.
    assert_eq!(pushed.iter().sum::<usize>(), 100, "{pushed:?}");
    let error = reading.await.expect("the read ends").unwrap_err();
    assert_eq!(
        (error.code(), error.limit().map(|limit| limit.name)),
        (Some("limit_exceeded"), Some("frame bytes"))
    );
}

#[tokio::test]
async fn a_batch_is_cut_to_the_hosts_own_frames_for_a_connector_that_takes_larger() {
    // A destination taking frames of 256 MiB, a host that sends none over 64 MiB, and a batch of
    // 80 rows of a mebibyte: it goes as frames the host can send.
    let limits = Limits {
        frame_bytes: 256 << 20,
        ..Limits::default()
    };
    let served = Served::new().with_destination(destination_factory::<MemoryDestination>());
    let mut writer = writer(served, limits, "flow_larger", &Options::default()).await;
    let rows: ArrayRef = Arc::new(StringArray::from(vec!["b".repeat(1 << 20); 80]));
    let batch = RecordBatch::try_from_iter([("b", rows)]).expect("a valid batch");
    writer
        .write(SegmentId(1), batch)
        .await
        .expect("the batch is written");
    let stats = writer.flush().await.expect("the batch is staged");
    assert_eq!(stats.rows, 80);
}

#[tokio::test]
async fn a_batch_is_cut_to_the_connectors_own_frames_for_a_host_that_takes_larger() {
    // The mirror: a host taking frames of 256 MiB reads rows of five mebibytes, twenty to a
    // batch, from a connector that sends no frame over 64 MiB.
    let options = Options {
        limits: Limits {
            frame_bytes: 256 << 20,
            ..Limits::default()
        },
        ..Options::default()
    };
    let config = serde_json::json!({ "bytes": 5 << 20, "ahead": 19, "alike": true });
    let source = blobs(config, &options).await;
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(4).expect("not zero"));
    let request = ReadRequest::new(
        StreamName::new("blobs").expect("a valid stream name"),
        Partition::single(),
        None,
    );
    let reading = tokio::spawn(async move { source.read(request, sink).await });
    let mut rows = 0;
    while rows < 40 {
        let event = tokio::time::timeout(Duration::from_secs(30), feed.recv()).await;
        match event.expect("rows arrive") {
            Some(rdlt_connector::SourceEvent::Push(rdlt_connector::Push::Arrow(batch))) => {
                assert!(
                    batch.num_rows() < 20,
                    "a batch of {} rows",
                    batch.num_rows()
                );
                rows += batch.num_rows();
            }
            other => panic!("the read ended or sent another event: {other:?}"),
        }
    }
    reading.abort();
}

/// A writer of the table `orders` on a fake destination that answers its writes as `fault` says.
pub(crate) async fn trickled(
    fault: crate::support::fake::Fault,
    options: &Options,
) -> Box<dyn DestinationWriter> {
    use crate::support::fake::{Fake, serve_fake};
    let connection = Connection::connect(
        serve_fake(Fake(fault)),
        Role::Destination,
        &serde_json::json!({}),
        *options,
    )
    .await
    .expect("the fake connects");
    let destination = RemoteDestination::new(connection).expect("the fake declares capabilities");
    let context = OpenContext {
        pipeline: PipelineId::parse("trickled").expect("a pipeline id"),
        load_id: LoadId::from_parts(std::time::UNIX_EPOCH, 1),
    };
    let mut opened = destination.open(&context).await.expect("the session opens");
    opened
        .session
        .writer(&table())
        .await
        .expect("the writer opens")
}

#[tokio::test(start_paused = true)]
async fn a_write_whose_credit_trickles_in_fails_at_its_write_ack_deadline() {
    use crate::support::fake::Fault;
    let deadline = Deadlines::default().write_ack;
    // A byte of credit just within each answer's deadline never makes up a frame's size.
    let every = deadline
        .checked_sub(Duration::from_secs(60))
        .expect("the deadline is longer than a minute");
    let mut writer = trickled(Fault::Trickles(every, 1), &Options::default()).await;
    let started = tokio::time::Instant::now();
    let writing = async {
        for segment in 1..=4 {
            writer.write(SegmentId(segment), ids(10)).await?;
        }
        Ok::<_, ConnectorError>(())
    };
    // Bounded on the paused clock: a wait no deadline ends fails here, a day on.
    let day = Duration::from_hours(24);
    let written = tokio::time::timeout(day, writing)
        .await
        .expect("the writes end");
    assert_eq!(written.unwrap_err().code(), Some(DEADLINE_EXCEEDED));
    let elapsed = started.elapsed();
    assert!(elapsed <= every + deadline * 2, "{elapsed:?}");
    let mut writer = trickled(Fault::Trickles(every, 1 << 30), &Options::default()).await;
    let flushed = tokio::time::timeout(day, writer.flush())
        .await
        .expect("the flush ends");
    assert_eq!(flushed.unwrap_err().code(), Some(DEADLINE_EXCEEDED));
}

#[tokio::test(start_paused = true)]
async fn a_credit_of_no_bytes_is_refused() {
    use crate::support::fake::Fault;
    let mut writer = trickled(
        Fault::Trickles(Duration::from_secs(1), 0),
        &Options::default(),
    )
    .await;
    let written = writer.write(SegmentId(1), ids(10));
    let error = tokio::time::timeout(Duration::from_hours(24), written)
        .await
        .expect("the write ends")
        .expect_err("the write is refused");
    assert_eq!(error.code(), Some("invalid_message"));
}

/// Limits whose largest message is a frame of the protocol's least, so a few writes' frames fill
/// a served connection's window.
fn framed() -> Limits {
    Limits {
        frame_bytes: MIN_FRAME_BYTES,
        catalog_bytes: 1 << 20,
        state_bytes: 1 << 20,
        config_bytes: 1 << 20,
        cursor_bytes: 1 << 20,
        schema_bytes: 1 << 20,
        ..Limits::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn five_writers_of_frame_sized_batches_through_one_connection_are_all_taken() {
    use rdlt_connector::{CommitMeta, CommitSeq, SegmentSet};
    let served = Served::new().with_destination(destination_factory::<MemoryDestination>());
    let io = served_within(served, framed());
    let options = Options {
        limits: framed(),
        ..Options::default()
    };
    let config = serde_json::json!({ "store": "five_writers" });
    let connection = Connection::connect(io, Role::Destination, &config, options)
        .await
        .expect("the destination handshakes");
    let destination = RemoteDestination::new(connection).expect("its capabilities are declared");
    let context = OpenContext {
        pipeline: PipelineId::parse("five").expect("a valid pipeline id"),
        load_id: LoadId::from_parts(std::time::UNIX_EPOCH, 1),
    };
    let mut opened = destination.open(&context).await.expect("the session opens");
    let mut writers = Vec::new();
    for _ in 0..5 {
        writers.push(
            opened
                .session
                .writer(&table())
                .await
                .expect("a writer opens"),
        );
    }
    // Each batch takes a frame of nearly the frame limit: five of them, all in flight, pass the
    // window of four, and the fifth waits for room rather than failing.
    let rows = i64::try_from(MIN_FRAME_BYTES * 15 / 16 / 8).unwrap();
    let mut writing = tokio::task::JoinSet::new();
    for (index, mut writer) in writers.into_iter().enumerate() {
        let segment = SegmentId(u64::try_from(index).unwrap() + 1);
        writing.spawn(async move {
            writer.write(segment, ids(rows)).await?;
            writer.flush().await
        });
    }
    // A commit behind the writes, on the same connection, is taken too.
    let meta = CommitMeta {
        load_id: context.load_id,
        commit_seq: CommitSeq::FIRST,
        epoch: opened.epoch,
        segments: SegmentSet::new(),
        abandoned: SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
        horizon: None,
    };
    let bounded = Duration::from_secs(60);
    let committed = tokio::time::timeout(bounded, opened.session.commit(&meta)).await;
    committed
        .expect("the commit is not held for ever")
        .expect("the commit is taken");
    let written = tokio::time::timeout(bounded, writing.join_all())
        .await
        .expect("no writer is held for ever");
    assert_eq!(written.len(), 5);
    for stats in written {
        assert!(stats.expect("each write is taken").rows > 0);
    }
}
