//! Credit sized to the frames a call sends: a receiver's window grows to two of the largest
//! frames it has taken, never past its own frame limit, and every frame's bytes come back; a
//! peer that ignores credit is held by HTTP/2's windows.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use rdlt_connector::serve::Served;
use rdlt_connector::wire::{TRANSPORT, v1};
use rdlt_connector::{
    ConnectorErrorKind, Partition, ReadRequest, Role, SegmentId, Source as _, SourceEvent,
    StreamName, destination_factory, partition_channel,
};
use rdlt_connector_reference::MemoryDestination;
use rdlt_host::{Connection, Options, RemoteSource};
use rdlt_wire::flow::Transport;
use rdlt_wire::limits::{CREDIT_FLOOR, Class, MIN_FRAME_BYTES};
use rdlt_wire::{Encoder, Limits};
use tokio_stream::wrappers::ReceiverStream;

use crate::sessions::{raw_session_within, table};
use crate::support::connectors::{Gate, Writes, Writing};
use crate::support::fake::{Granted, Polled};
use crate::support::{Fake, Fault, serve_fake};

/// Rows: the ids of a frame of seven megabytes, as the default coalescing target makes.
const ROWS: i64 = 875_000;

fn ids(count: i64) -> RecordBatch {
    let values: Vec<i64> = (0..count).collect();
    RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(values)) as _)])
        .expect("a valid batch")
}

fn length(frame: &impl rdlt_wire::prost::Message) -> u64 {
    u64::try_from(frame.encoded_len()).expect("a frame's length fits")
}

/// A write's frames after its start: a schema, three frames of [`ROWS`] ids, and a flush.
fn write_frames() -> Vec<v1::WriteFrame> {
    use v1::write_frame::Frame;
    let batch = ids(ROWS);
    let mut encoder = Encoder::default();
    let schema = Frame::Schema(v1::WriteSchema {
        version: 1,
        ipc_schema: encoder.schema(&batch.schema()).expect("the schema encodes"),
    });
    let mut sent = vec![schema];
    for _ in 0..3 {
        for frame in encoder.batch(&batch).expect("the batch encodes") {
            sent.push(Frame::Batch(v1::WriteBatch {
                segment: 1,
                data_header: frame.header,
                data_body: frame.body,
            }));
        }
    }
    sent.push(Frame::Flush(v1::Unit {}));
    sent.into_iter()
        .map(|frame| v1::WriteFrame { frame: Some(frame) })
        .collect()
}

#[tokio::test]
async fn a_served_write_grows_its_window_to_two_frames_and_answers_a_flush_with_credit() {
    use v1::write_ack::Ack;
    use v1::write_frame::Frame;
    let served = Served::new().with_destination(destination_factory::<MemoryDestination>());
    let (mut client, session) = raw_session_within(served, "credit_grows", Limits::default()).await;
    let sent = write_frames();
    let sizes: Vec<u64> = sent.iter().map(length).collect();
    let start = Frame::Start(v1::WriteStart {
        session,
        table: Some(v1::TableRef::from(&table())),
    });
    let (frames, receiver) = tokio::sync::mpsc::channel(sent.len() + 1);
    for frame in std::iter::once(v1::WriteFrame { frame: Some(start) }).chain(sent) {
        frames.send(frame).await.expect("the write is open");
    }
    drop(frames);
    let mut acks = client
        .write(ReceiverStream::new(receiver))
        .await
        .expect("the write starts");
    let mut answers = Vec::new();
    while let Some(ack) = tokio::time::timeout(Duration::from_secs(30), acks.message())
        .await
        .expect("the write answers")
        .expect("the write's answers arrive")
    {
        answers.push(ack.ack.expect("an answer"));
    }
    let (schema, frame, flush) = (sizes[0], sizes[1], sizes[4]);
    assert!(2 * frame > CREDIT_FLOOR);
    let credits: Vec<_> = answers
        .iter()
        .map(|answer| match answer {
            Ack::Credit(credit) => Some(credit.bytes),
            _ => None,
        })
        .collect();
    let grown = frame + (2 * frame - CREDIT_FLOOR);
    let expected = [CREDIT_FLOOR, schema, grown, frame, frame, flush];
    assert_eq!(credits[..6], expected.map(Some), "{answers:?}");
    // The flush's stats follow its credit, and end the answers.
    assert!(
        matches!(answers[6..], [Ack::Flushed(_)]),
        "{:?}",
        &answers[6..]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_remote_writer_sends_two_frames_ahead_of_what_its_destination_took() {
    static GATE: Gate = Gate::after(1);
    let served = Served::new().with_destination(Writes::factory(Writing::Gated(&GATE)));
    let options = Options::default();
    let mut writer = crate::flow::writer(served, Limits::default(), "credit_ahead", &options).await;
    let bounded = Duration::from_secs(30);
    // The first frame goes on the opening credit, the next two on the window of two frames it
    // grows, and the fourth on the second's credit, which the gate holds; the third waits
    // decoded for the writer.
    for segment in 1..=4 {
        tokio::time::timeout(bounded, writer.write(SegmentId(segment), ids(ROWS)))
            .await
            .expect("the write goes on the credit granted")
            .expect("the write is sent");
    }
    let staged = GATE.kept.lock().expect("the lock is not poisoned").len();
    assert_eq!(staged, 1);
    // A fifth waits: two frames are the window, and the writer took neither the third nor the
    // fourth.
    let fifth = tokio::time::timeout(
        Duration::from_millis(500),
        writer.write(SegmentId(5), ids(ROWS)),
    );
    assert!(fifth.await.is_err(), "a fifth frame went");
    GATE.open();
    tokio::time::timeout(bounded, writer.flush())
        .await
        .expect("the flush ends")
        .expect("the flush answers");
    let kept = GATE.kept.lock().expect("the lock is not poisoned");
    let segments: Vec<_> = kept.iter().map(|(segment, _)| *segment).collect();
    assert_eq!(segments, [1, 2, 3, 4]);
}

/// The schema frame of a column of ids, and three frames of seven megabytes of them.
fn large_frames() -> Vec<v1::ReadFrame> {
    use v1::read_frame::Frame;
    let batch = ids(ROWS);
    let mut encoder = Encoder::default();
    let schema = Frame::Schema(v1::SchemaFrame {
        schema_epoch: 1,
        ipc_schema: encoder.schema(&batch.schema()).expect("the schema encodes"),
    });
    let mut frames = vec![v1::ReadFrame {
        frame: Some(schema),
    }];
    for _ in 0..3 {
        for frame in encoder.batch(&batch).expect("the batch encodes") {
            let batch = Frame::Batch(v1::BatchFrame {
                schema_epoch: 1,
                kind: v1::BatchKind::Arrow as i32,
                data_header: frame.header,
                data_body: frame.body,
            });
            frames.push(v1::ReadFrame { frame: Some(batch) });
        }
    }
    frames
}

/// Limits whose data wire bound is the least a peer may set.
fn least() -> Limits {
    Limits {
        frame_bytes: MIN_FRAME_BYTES,
        ..Limits::default()
    }
}

/// Bytes: the most a frame of a read takes on the wire within [`least`].
fn bound() -> u64 {
    u64::try_from(least().decoding(Class::Data)).expect("the bound fits")
}

/// A frame of JSON of `bytes` on the wire.
fn json(bytes: u64) -> v1::ReadFrame {
    let framed = |length: u64| v1::ReadFrame {
        frame: Some(v1::read_frame::Frame::Json(v1::JsonFrame {
            data: vec![b'x'; usize::try_from(length).expect("fits")].into(),
        })),
    };
    let data = (bytes - 16..bytes)
        .rev()
        .find(|data| length(&framed(*data)) == bytes)
        .expect("a length frames to the bytes");
    framed(data)
}

/// Two frames at the data wire bound of [`least`], then one a byte beyond it.
fn bound_frames() -> Vec<v1::ReadFrame> {
    vec![json(bound()), json(bound()), json(bound() + 1)]
}

/// Reads the fake connector `fault` serves within `options`, taking events until `events` are
/// taken or the read ends: the read, which goes on.
async fn read(
    fault: Fault,
    options: &Options,
    events: usize,
) -> tokio::task::JoinHandle<rdlt_connector::Result<()>> {
    let connection = Connection::connect(
        serve_fake(Fake(fault)),
        Role::Source,
        &serde_json::json!({}),
        *options,
    )
    .await
    .expect("the fake handshakes");
    let source = RemoteSource::new(connection);
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(4).expect("not zero"));
    let request = ReadRequest::new(
        StreamName::new("items").expect("a valid stream name"),
        Partition::single(),
        None,
    );
    let reading = tokio::spawn(async move { source.read(request, sink).await });
    for _ in 0..events {
        let event = tokio::time::timeout(Duration::from_secs(30), feed.recv()).await;
        match event.expect("an event arrives") {
            Some(SourceEvent::Push(_)) => {}
            Some(other) => panic!("an event other than a push: {other:?}"),
            None => break,
        }
    }
    reading
}

/// Waits until `granted` holds `count` credits, and answers them.
async fn credits(granted: &Granted, count: usize) -> Vec<u64> {
    let waited = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let credits = granted.lock().expect("the lock is not poisoned").clone();
            if credits.len() >= count {
                return credits;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    waited.await.expect("the credits arrive")
}

#[tokio::test(flavor = "multi_thread")]
async fn a_hosts_read_grows_its_window_to_two_frames() {
    static GRANTED: Granted = Granted::new(Vec::new());
    let frames = large_frames();
    let (schema, frame) = (length(&frames[0]), length(&frames[1]));
    let options = Options::default();
    let reading = read(Fault::Granted(large_frames, &GRANTED), &options, 3).await;
    let grown = frame + (2 * frame - options.read_floor);
    assert_eq!(
        credits(&GRANTED, 5).await,
        [options.read_floor, schema, grown, frame, frame]
    );
    assert!(!reading.is_finished(), "the read ended");
    reading.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_hosts_window_stops_at_its_frame_limit_and_a_frame_beyond_it_is_refused_ungranted() {
    static GRANTED: Granted = Granted::new(Vec::new());
    let options = Options {
        limits: least(),
        ..Options::default()
    };
    let reading = read(Fault::Granted(bound_frames, &GRANTED), &options, 3).await;
    let ended = tokio::time::timeout(Duration::from_secs(30), reading).await;
    let error = ended
        .expect("the read ends")
        .expect("the read does not panic")
        .expect_err("the read is refused");
    assert_eq!(
        (error.kind(), error.code()),
        (ConnectorErrorKind::Internal, Some(TRANSPORT)),
        "{error}"
    );
    let grown = bound() + (bound() - CREDIT_FLOOR);
    assert_eq!(credits(&GRANTED, 3).await, [CREDIT_FLOOR, grown, bound()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_the_engine_does_not_take_holds_at_most_its_stream_window_in_transport() {
    static POLLED: Polled = Polled::new();
    let options = Options {
        heartbeat: Duration::from_millis(50),
        ..Options::default()
    };
    let connection = Connection::connect(
        serve_fake(Fake(Fault::Floods(&POLLED))),
        Role::Source,
        &serde_json::json!({}),
        options,
    )
    .await
    .expect("the fake handshakes");
    let source = RemoteSource::new(connection);
    // The engine takes one event, which waits in the sink, and no more.
    let (sink, _feed) = partition_channel(NonZeroUsize::new(1).expect("not zero"));
    let request = ReadRequest::new(
        StreamName::new("items").expect("a valid stream name"),
        Partition::single(),
        None,
    );
    let reading = tokio::spawn(async move { source.read(request, sink).await });
    tokio::time::timeout(
        Duration::from_secs(10),
        POLLED.settled(Duration::from_millis(200)),
    )
    .await
    .expect("the transport stops taking frames");
    assert!(!reading.is_finished(), "the read ended");
    let transport = Transport::default();
    let frame = length(&crate::support::fake::mebibyte().1);
    // What left the connector: the frame the sink holds, a frame waiting for its room, the
    // call's stream window, and on the connector the frame its encoder holds and what HTTP/2
    // buffers to send, at most 400 KiB.
    let held = POLLED.bytes();
    let bound = u64::from(transport.stream_window) + 3 * frame + 400 * 1024;
    assert!(
        held <= bound,
        "{held} bytes left the connector, above {bound}"
    );
    reading.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_whose_connector_does_not_read_fails_once_its_transport_window_fills() {
    let options = Options {
        deadlines: rdlt_host::Deadlines {
            write_ack: Duration::from_millis(300),
            ..rdlt_host::Deadlines::default()
        },
        ..Options::default()
    };
    // Credit for a gigabyte, of which the connector reads nothing: its frames fill the call's
    // window and the host's buffers, and the next send waits past its deadline.
    let mut writer = crate::flow::trickled(Fault::Hoards(1 << 30), &options).await;
    let written = tokio::time::timeout(Duration::from_secs(30), async {
        for segment in 1..=64 {
            writer.write(SegmentId(segment), ids(131_072)).await?;
        }
        Ok::<_, rdlt_connector::ConnectorError>(())
    })
    .await
    .expect("the writer does not hang past its deadline");
    assert_eq!(
        written.expect_err("the write fails").code(),
        Some(rdlt_host::DEADLINE_EXCEEDED)
    );
}
