//! A frame whose parts are each within the limits, but which holds more than they bound as a
//! whole, is refused where it is received: by the host reading from a connector, and by a served
//! connector the host writes to.
//!
//! A sender cuts a batch to its receiver's limits, so rows that each fit are never refused.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::builder::BinaryViewBuilder;
use arrow_array::{ArrayRef, Int64Array, NullArray, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use rdlt_connector::serve::Served;
use rdlt_connector::wire::v1;
use rdlt_connector::{
    ConnectorError, ConnectorErrorKind, Destination as _, Partition, PipelineId, ReadRequest, Role,
    SegmentId, Source as _, StreamName, partition_channel,
};
use rdlt_connector_reference::{MemoryDestination, published};
use rdlt_engine::{PipelinePlan, RunStatus, StreamPlan};
use rdlt_host::{Connection, Options, RemoteDestination, RemoteSource};
use rdlt_wire::limits::{MIN_BATCH_ROWS, MIN_BATCH_VALUES};
use rdlt_wire::{Encoder, IpcFrame, Limits};
use tokio_stream::wrappers::ReceiverStream;

use crate::sessions::{context, raw_session, table};
use crate::support::connectors::{Kept, Ticks, Writes, Writing, flagged};
use crate::support::{Fake, Fault, engine, serve_fake, served, served_within};

type Sent = (Bytes, Vec<IpcFrame>);

/// `batch`'s schema message and frames, as the wire's encoder sends them.
fn encoded(batch: &RecordBatch) -> Sent {
    let mut encoder = Encoder::default();
    let schema = encoder.schema(&batch.schema()).expect("the schema encodes");
    (schema, encoder.batch(batch).expect("the batch encodes"))
}

/// `columns` columns of nulls, `rows` rows each: values that take no bytes of a frame.
fn nulls(columns: usize, rows: usize) -> RecordBatch {
    let fields: Vec<_> = (0..columns)
        .map(|column| Field::new(format!("c{column}"), DataType::Null, true))
        .collect();
    let column: ArrayRef = Arc::new(NullArray::new(rows));
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        vec![column; columns],
        &options,
    )
    .expect("a batch of nulls")
}

/// Two buffers of one column that share the body's first bytes.
fn shared_buffers() -> Sent {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
    let batch = RecordBatch::try_from_iter([("n", values)]).expect("a batch");
    let (schema, mut frames) = encoded(&batch);
    // The values' buffer follows the validity's, 64 bytes into the body and 24 bytes long; here
    // it starts with it.
    let described: Vec<u8> = [64_i64, 24]
        .into_iter()
        .flat_map(i64::to_le_bytes)
        .collect();
    let mut header = frames[0].header.to_vec();
    let at = header
        .windows(described.len())
        .position(|window| window == described)
        .expect("the values' buffer is described");
    header[at..at + 8].copy_from_slice(&0_i64.to_le_bytes());
    frames[0].header = Bytes::from(header);
    (schema, frames)
}

/// More values than a frame may hold, in a frame with no body.
fn body_free_values() -> Sent {
    encoded(&nulls(65, 1 << 20))
}

/// Views that each name the whole of one mebibyte, four gibibytes between them.
fn aliased_views() -> Sent {
    let mut views = BinaryViewBuilder::new();
    let block = views.append_block(vec![b'a'; 1 << 20].into());
    for _ in 0..4_096 {
        views
            .try_append_view(block, 0, 1 << 20)
            .expect("a view of the block");
    }
    let views: ArrayRef = Arc::new(views.finish());
    encoded(&RecordBatch::try_from_iter([("v", views)]).expect("a batch"))
}

/// A schema of one column more than the limit.
fn wide_schema() -> Sent {
    (encoded(&nulls(10_001, 0)).0, Vec::new())
}

/// A schema whose one field's name is a byte longer than a control string may be.
fn long_name() -> Sent {
    let batch = RecordBatch::try_from_iter([(
        "n".repeat((64 << 10) + 1),
        Arc::new(NullArray::new(1)) as ArrayRef,
    )]);
    (encoded(&batch.expect("a batch")).0, Vec::new())
}

/// What makes frames a receiver refuses, the refusal's code, and the limit it names.
type Refused = (fn() -> Sent, &'static str, Option<&'static str>);

/// Frames a receiver refuses.
const REFUSED: [Refused; 5] = [
    (shared_buffers, "malformed_frame", None),
    (body_free_values, "limit_exceeded", Some("batch values")),
    (aliased_views, "limit_exceeded", Some("view bytes")),
    (wide_schema, "limit_exceeded", Some("schema columns")),
    (long_name, "limit_exceeded", Some("control string bytes")),
];

fn refusal(error: &ConnectorError) -> (Option<&str>, Option<&'static str>) {
    (error.code(), error.limit().map(|limit| limit.name))
}

#[tokio::test]
async fn the_host_refuses_a_connectors_frame_no_limit_bounds_as_a_whole() {
    for (sent, code, limit) in REFUSED {
        let io = serve_fake(Fake(Fault::Sends(sent)));
        let config = serde_json::json!({});
        let connection = Connection::connect(io, Role::Source, &config, Options::default())
            .await
            .expect("the fake handshakes");
        let (sink, mut feed) = partition_channel(std::num::NonZeroUsize::new(8).expect("not 0"));
        let drain = tokio::spawn(async move { while feed.recv().await.is_some() {} });
        let request = ReadRequest::new(
            StreamName::new("items").expect("a valid stream name"),
            Partition::single(),
            None,
        );
        let source = RemoteSource::new(connection);
        let error = tokio::time::timeout(Duration::from_secs(30), source.read(request, sink))
            .await
            .expect("the read ends")
            .expect_err("the read is refused");
        drain.abort();
        assert_eq!(refusal(&error), (Some(code), limit), "{error}");
        if limit.is_none() {
            assert_eq!(error.kind(), ConnectorErrorKind::Internal);
        }
    }
}

#[tokio::test]
async fn a_served_connector_refuses_a_hosts_frame_no_limit_bounds_as_a_whole() {
    use v1::write_frame::Frame;
    for (sent, code, limit) in REFUSED {
        let served = Served::new().with_destination(Writes::factory(Writing::Panics));
        let (mut client, session) = raw_session(served, "frames").await;
        let (schema, batches) = sent();
        let start = Frame::Start(v1::WriteStart {
            session,
            table: Some(v1::TableRef::from(&table())),
        });
        let schema = Frame::Schema(v1::WriteSchema {
            version: 1,
            ipc_schema: schema,
        });
        let batches = batches.into_iter().map(|frame| {
            Frame::Batch(v1::WriteBatch {
                segment: 1,
                data_header: frame.header,
                data_body: frame.body,
            })
        });
        let (frames, receiver) = tokio::sync::mpsc::channel(8);
        for frame in [start, schema].into_iter().chain(batches) {
            let frame = v1::WriteFrame { frame: Some(frame) };
            frames.send(frame).await.expect("the write is open");
        }
        let mut acks = client
            .write(ReceiverStream::new(receiver))
            .await
            .expect("the write starts")
            .into_inner();
        let mut error = None;
        while let Some(ack) = tokio::time::timeout(Duration::from_secs(30), acks.message())
            .await
            .expect("the write answers")
            .expect("the write's answers arrive")
        {
            if let Some(v1::write_ack::Ack::Error(refused)) = ack.ack {
                error = Some(refused);
            }
        }
        let error = ConnectorError::try_from(error.expect("the write answers its error"));
        let error = error.expect("a connector error");
        assert_eq!(refusal(&error), (Some(code), limit), "{error}");
    }
}

/// The rows of `pieces`, checked to be `whole`'s, once each and in order: how many each holds.
fn in_order(whole: &RecordBatch, pieces: &[RecordBatch]) -> Vec<usize> {
    let mut start = 0;
    for piece in pieces {
        assert_eq!(piece, &whole.slice(start, piece.num_rows()), "at {start}");
        start += piece.num_rows();
    }
    assert_eq!(start, whole.num_rows());
    pieces.iter().map(RecordBatch::num_rows).collect()
}

/// A million rows of 64 columns of flags and their ids: more values than one frame may hold,
/// in a frame a quarter of the frame limit.
const ROWS: u64 = 1 << 20;
const FLAGS: usize = 64;

#[tokio::test]
async fn a_connector_cuts_a_batch_of_more_values_than_the_hosts_frame_may_hold() {
    let io = served(Served::new().with_source(rdlt_connector::source_factory::<Ticks>()));
    let config = serde_json::json!({ "rows": ROWS, "flags": FLAGS });
    let connection = Connection::connect(io, Role::Source, &config, Options::default())
        .await
        .expect("the source handshakes");
    let source = RemoteSource::new(connection);
    let (sink, mut feed) = partition_channel(std::num::NonZeroUsize::new(64).expect("not 0"));
    let request = ReadRequest::new(
        StreamName::new("ticks").expect("a valid stream name"),
        Partition::single(),
        None,
    );
    let read = tokio::time::timeout(Duration::from_secs(60), source.read(request, sink));
    read.await
        .expect("the read ends")
        .expect("the read succeeds");
    let mut events = Vec::new();
    while let Some(event) = feed.recv().await {
        events.push(event);
    }
    // The pushes, then the checkpoint that follows the batch they were cut from.
    let checkpoint = events.pop().expect("a checkpoint");
    assert!(
        matches!(checkpoint, rdlt_connector::SourceEvent::Checkpoint { .. }),
        "{checkpoint:?}"
    );
    let pushes = events.into_iter().map(|event| match event {
        rdlt_connector::SourceEvent::Push(rdlt_connector::Push::Arrow(batch)) => batch,
        other => panic!("an event other than a push of rows: {other:?}"),
    });
    let pieces = in_order(&flagged(ROWS, FLAGS), &pushes.collect::<Vec<_>>());
    // At 65 values a row, a frame's values hold 1032444 rows.
    assert_eq!(pieces, [1_032_444, 16_132]);
}

/// A writer of a destination that keeps what it is given in `kept`, served within `limits`.
async fn keeping(
    kept: &'static Kept,
    limits: Limits,
) -> Box<dyn rdlt_connector::DestinationWriter> {
    let keeping = Served::new().with_destination(Writes::factory(Writing::Keeps(kept)));
    let config = serde_json::json!({ "store": "frames_kept" });
    let io = served_within(keeping, limits);
    let connection = Connection::connect(io, Role::Destination, &config, Options::default())
        .await
        .expect("the destination handshakes");
    let destination = RemoteDestination::new(connection).expect("its capabilities");
    let mut opened = destination
        .open(&context())
        .await
        .expect("the session opens");
    opened.session.writer(&table()).await.expect("a writer")
}

#[tokio::test]
async fn the_host_cuts_a_batch_of_more_values_than_a_connectors_frame_may_hold() {
    static KEPT: Kept = Kept::new(Vec::new());
    let mut writer = keeping(&KEPT, Limits::default()).await;
    let whole = flagged(ROWS, FLAGS);
    writer
        .write(SegmentId(7), whole.clone())
        .await
        .expect("the batch is written");
    writer.flush().await.expect("the writes are staged");
    let kept = std::mem::take(&mut *KEPT.lock().expect("the lock is not poisoned"));
    // Each piece is a write of the segment its batch was.
    assert!(kept.iter().all(|(segment, _)| *segment == 7));
    let pieces: Vec<_> = kept.into_iter().map(|(_, batch)| batch).collect();
    assert_eq!(in_order(&whole, &pieces), [1_032_444, 16_132]);
}

#[tokio::test]
async fn a_batch_no_schema_message_describes_is_refused_before_any_of_it_is_sent() {
    use arrow_array::types::Int8Type;
    use arrow_array::{DictionaryArray, StringArray};
    static KEPT: Kept = Kept::new(Vec::new());
    let mut writer = keeping(&KEPT, Limits::default()).await;
    let tags = StringArray::from(vec!["a", "bb"]);
    let inner = DictionaryArray::<Int8Type>::try_new(vec![0, 1].into(), Arc::new(tags));
    let inner = inner.expect("a dictionary");
    let twice = DictionaryArray::<Int8Type>::try_new(vec![1, 0].into(), Arc::new(inner));
    let twice: ArrayRef = Arc::new(twice.expect("a dictionary of dictionaries"));
    let batch = RecordBatch::try_from_iter([("d", twice)]).expect("a batch");
    let error = writer
        .write(SegmentId(1), batch)
        .await
        .expect_err("the batch is refused");
    assert_eq!(
        (error.kind(), error.code()),
        (ConnectorErrorKind::Unsupported, Some("unsendable_type"))
    );
    // The write goes on, and the destination was given nothing of the refused batch.
    let plain = flagged(3, 1);
    writer
        .write(SegmentId(2), plain.clone())
        .await
        .expect("a batch is written");
    writer.flush().await.expect("the write is staged");
    let kept = std::mem::take(&mut *KEPT.lock().expect("the lock is not poisoned"));
    assert_eq!(kept, [(2, plain)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn rows_cut_to_both_ends_limits_load_once_each_and_in_order() {
    // The host takes the fewest values a frame the protocol allows from the source, and the
    // destination the fewest rows a frame from the host: the batch is cut on its way in and again
    // on its way out.
    let io = served(Served::new().with_source(rdlt_connector::source_factory::<Ticks>()));
    let config = serde_json::json!({ "rows": 5_000, "flags": 300 });
    let within = |limits| Options {
        limits,
        ..Options::default()
    };
    let few_values = Limits {
        batch_values: MIN_BATCH_VALUES,
        ..Limits::default()
    };
    let connection = Connection::connect(io, Role::Source, &config, within(few_values));
    let source = RemoteSource::new(connection.await.expect("the source handshakes"));
    let few_rows = Limits {
        batch_rows: MIN_BATCH_ROWS,
        ..Limits::default()
    };
    let memory = rdlt_connector::destination_factory::<MemoryDestination>();
    let io = served_within(Served::new().with_destination(memory), few_rows);
    let config = serde_json::json!({ "store": "frames_cut" });
    let connection = Connection::connect(io, Role::Destination, &config, Options::default());
    let connection = connection.await.expect("the destination handshakes");
    let destination = RemoteDestination::new(connection).expect("its capabilities");
    let stream = StreamName::new("ticks").expect("a valid stream name");
    let pipeline = PipelineId::parse("frames_cut").expect("a valid pipeline id");
    let plan = PipelinePlan::new(pipeline, [StreamPlan::new(stream)]).expect("a plan");
    let outcome = engine(50)
        .run(plan, Arc::new(source), Arc::new(destination))
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let batches = published("frames_cut", "ticks");
    assert!(batches.iter().all(|batch| batch.num_rows() <= 1_024));
    assert!(batches.len() >= 5);
    let ids = batches.iter().flat_map(|batch| {
        let ids = batch.column_by_name("id").expect("the ids");
        let ids = ids.as_any().downcast_ref::<Int64Array>().expect("integers");
        ids.values().to_vec()
    });
    assert_eq!(ids.collect::<Vec<_>>(), (0..5_000).collect::<Vec<_>>());
}
