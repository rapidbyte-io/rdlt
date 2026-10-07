//! What a write stages between two flushes is bounded on both ends: a served destination refuses
//! more than its limit, and the host's writer flushes before it would send more.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use rdlt_connector::serve::Served;
use rdlt_connector::wire::v1;
use rdlt_connector::{
    ConnectorError, Destination as _, LoadId, OpenContext, PipelineId, Role, SegmentId,
    destination_factory,
};
use rdlt_connector_reference::MemoryDestination;
use rdlt_host::{Connection, Options, RemoteDestination};
use rdlt_wire::limits::MIN_FRAME_BYTES;
use rdlt_wire::{Encoder, Limits};
use tokio_stream::wrappers::ReceiverStream;

use crate::sessions::{raw_session_within, table};
use crate::support::served_within;

/// Limits under which a write may stage four frames of the least bytes a peer may ask for
/// between flushes: sixteen mebibytes.
fn limits() -> Limits {
    Limits {
        frame_bytes: MIN_FRAME_BYTES,
        ..Limits::default()
    }
}

/// Rows: the ids of a frame of a mebibyte.
const MEBIBYTE: i64 = 131_072;

fn ids(count: i64) -> RecordBatch {
    let values: Vec<i64> = (0..count).collect();
    RecordBatch::try_from_iter([("id", Arc::new(arrow_array::Int64Array::from(values)) as _)])
        .expect("a valid batch")
}

/// What a raw write of `batches` frames of a mebibyte of ids answers with, a flush sent after every
/// `flush_every` of them: the error it ended with, if any, and how many flushes it answered.
async fn raw_write(
    store: &str,
    batches: usize,
    flush_every: usize,
) -> (Option<ConnectorError>, usize) {
    use v1::write_frame::Frame;
    let served = Served::new().with_destination(destination_factory::<MemoryDestination>());
    let (mut client, session) = raw_session_within(served, store, limits()).await;
    let batch = ids(MEBIBYTE);
    let mut encoder = Encoder::default();
    let mut sent = vec![
        Frame::Start(v1::WriteStart {
            session,
            table: Some(v1::TableRef::from(&table())),
        }),
        Frame::Schema(v1::WriteSchema {
            version: 1,
            ipc_schema: encoder.schema(&batch.schema()).expect("the schema encodes"),
        }),
    ];
    for index in 1..=batches {
        for frame in encoder.batch(&batch).expect("the batch encodes") {
            sent.push(Frame::Batch(v1::WriteBatch {
                segment: 1,
                data_header: frame.header,
                data_body: frame.body,
            }));
        }
        if index % flush_every == 0 {
            sent.push(Frame::Flush(v1::Unit {}));
        }
    }
    let (frames, receiver) = tokio::sync::mpsc::channel(sent.len());
    for frame in sent {
        let frame = v1::WriteFrame { frame: Some(frame) };
        frames.send(frame).await.expect("the write is open");
    }
    drop(frames);
    let mut acks = client
        .write(ReceiverStream::new(receiver))
        .await
        .expect("the write starts")
        .into_inner();
    let (mut error, mut flushes) = (None, 0);
    while let Some(ack) = tokio::time::timeout(Duration::from_secs(30), acks.message())
        .await
        .expect("the write answers")
        .expect("the write's answers arrive")
    {
        match ack.ack {
            Some(v1::write_ack::Ack::Error(refused)) => error = Some(refused),
            Some(v1::write_ack::Ack::Flushed(_)) => flushes += 1,
            _ => {}
        }
    }
    let error = error.map(|error| ConnectorError::try_from(error).expect("a connector error"));
    (error, flushes)
}

#[tokio::test]
async fn a_served_write_refuses_more_than_it_may_stage_between_flushes() {
    // Forty frames of a mebibyte pass the sixteen a write may stage.
    let (error, flushes) = raw_write("staged_refused", 40, usize::MAX).await;
    let error = error.expect("the write is refused");
    assert_eq!(error.code(), Some("limit_exceeded"), "{error}");
    let limit = error.limit().expect("a refusal names its limit");
    assert_eq!(
        (limit.name, limit.limit),
        ("staged bytes", 4 * MIN_FRAME_BYTES)
    );
    assert!(limit.actual > limit.limit);
    assert_eq!(flushes, 0);
}

#[tokio::test]
async fn a_served_write_that_flushes_in_time_stages_as_much_as_it_likes() {
    let (error, flushes) = raw_write("staged_flushed", 40, 8).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(flushes, 5);
}

#[tokio::test]
async fn the_hosts_writer_flushes_before_it_stages_more_than_the_destination_holds() {
    // Frames of a mebibyte, within the credit floor; and frames of nearly the frame limit, whose
    // window of two frames leaves credit to spare when the destination's limit nears.
    let near = i64::try_from(MIN_FRAME_BYTES * 15 / 16 / 8).expect("fits");
    for (store, rows, writes) in [("staged_host", MEBIBYTE, 40), ("staged_spare", near, 10)] {
        let served = Served::new().with_destination(destination_factory::<MemoryDestination>());
        let io = served_within(served, limits());
        let config = serde_json::json!({ "store": store });
        let connection = Connection::connect(io, Role::Destination, &config, Options::default())
            .await
            .expect("the destination handshakes");
        let destination =
            RemoteDestination::new(connection).expect("its capabilities are declared");
        let context = OpenContext {
            pipeline: PipelineId::parse("staging").expect("a valid pipeline id"),
            load_id: LoadId::from_parts(std::time::UNIX_EPOCH, 1),
        };
        let mut opened = destination.open(&context).await.expect("the session opens");
        let mut writer = opened
            .session
            .writer(&table())
            .await
            .expect("the writer opens");
        // Writes with no flush asked for: the writer flushes as the destination's limit nears.
        for _ in 0..writes {
            writer
                .write(SegmentId(1), ids(rows))
                .await
                .expect("the write is staged");
        }
        let stats = writer.flush().await.expect("the flush answers");
        // The stats of the flushes the writer made itself are in those of the flush asked for.
        assert_eq!(stats.rows, writes * rows.unsigned_abs(), "{store}");
    }
}
