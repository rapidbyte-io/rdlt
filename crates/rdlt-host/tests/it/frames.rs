//! A frame whose parts are each within the limits, but which holds more than they bound as a
//! whole, is refused where it is received: by the host reading from a connector, and by a served
//! connector the host writes to.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::builder::BinaryViewBuilder;
use arrow_array::{ArrayRef, Int64Array, NullArray, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use rdlt_connector::serve::Served;
use rdlt_connector::wire::v1;
use rdlt_connector::{
    ConnectorError, ConnectorErrorKind, Partition, ReadRequest, Role, Source as _, StreamName,
    partition_channel,
};
use rdlt_host::{Connection, Options, RemoteSource};
use rdlt_wire::{Encoder, IpcFrame};
use tokio_stream::wrappers::ReceiverStream;

use crate::sessions::{raw_session, table};
use crate::support::connectors::{Writes, Writing};
use crate::support::{Fake, Fault, serve_fake};

type Sent = (Bytes, Vec<IpcFrame>);

/// `batch`'s schema message and frames, as the wire's encoder sends them.
fn encoded(batch: &RecordBatch) -> Sent {
    let mut encoder = Encoder::default();
    let schema = encoder.schema(&batch.schema());
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
