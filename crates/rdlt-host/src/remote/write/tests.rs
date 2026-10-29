mod cancelled;

use std::sync::Arc;
use std::time::Duration;

use arrow_array::types::Int8Type;
use arrow_array::{Array as _, ArrayRef, DictionaryArray, RecordBatch, StringArray};
use rdlt_connector::wire::v1;
use rdlt_connector::wire::v1::write_frame::Frame;
use rdlt_connector::{ConnectorErrorKind, DestinationWriter as _, SegmentId, WriteStats};
use rdlt_wire::stream::{Call, End, FrameReader, HEAD_BYTES, Kind, Outgoing, Received};
use rdlt_wire::{Decoder, Encoder, IpcFrame, Limits};
use tokio::io::{DuplexStream, ReadHalf, WriteHalf};
use tokio_util::sync::CancellationToken;

use super::RemoteWriter;
use crate::network::Stream;
use crate::remote::checked::Checked;
use crate::remote::clients::Clients;
use crate::remote::opened::Carrier;
use crate::remote::{CONNECTOR_LOST, Connection, DEADLINE_EXCEEDED, Deadlines, Options};

/// The connector's end of a write's stream: the frames the writer sends, and its answers.
struct Connector {
    frames: FrameReader<ReadHalf<DuplexStream>>,
    acks: WriteHalf<DuplexStream>,
}

impl Connector {
    /// The writer's next frame, within a second.
    async fn frame(&mut self) -> Received {
        tokio::time::timeout(Duration::from_secs(1), self.frames.next())
            .await
            .expect("a frame arrives in time")
            .expect("the frame arrives whole")
            .expect("the writer is open")
    }

    /// The message the writer's next frame holds, within a second.
    async fn message(&mut self) -> Frame {
        let frame = self.frame().await;
        let form = Call::Write
            .form(End::Host)
            .expect("a write's host sends messages");
        let message: v1::WriteFrame = frame.message(form, 1 << 20).expect("the message decodes");
        message.frame.expect("a frame")
    }

    /// Answers with `ack`.
    async fn answer(&mut self, ack: v1::write_ack::Ack) {
        let answer = v1::WriteAck { ack: Some(ack) };
        let mut frame = Outgoing::message(None, &answer).expect("an answer fits a head");
        frame.write(&mut self.acks).await.expect("the writer reads");
    }
}

/// A writer over a stream holding `buffered` bytes each way, each wait for an answer due within
/// a second, and the connector's end of it.
fn fixture(buffered: usize) -> (RemoteWriter, Connector) {
    let options = Options {
        deadlines: Deadlines {
            write_ack: Duration::from_secs(1),
            ..Deadlines::default()
        },
        ..Options::default()
    };
    let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
    let checked = Checked::new(channel.clone(), options.limits);
    let connection = Arc::new(Connection {
        client: Clients::new(&checked, &options.limits),
        spec: v1::ConnectorSpec::default(),
        peer: options.limits,
        options,
        lost: CancellationToken::new(),
        spent: CancellationToken::new(),
        carrier: Carrier::Called(channel),
    });
    let (host, connector) = tokio::io::duplex(buffered);
    let (acks, frames) = tokio::io::split(Box::new(host) as Box<dyn Stream>);
    let limits = options.limits;
    let writer = RemoteWriter {
        connection,
        acks: FrameReader::new(acks, Call::Write.bounds(End::Connector, &limits)),
        frames,
        encoder: Encoder::default(),
        schema: None,
        version: 1,
        staged: 0,
        flushed: WriteStats::default(),
        unanswered: false,
        pending: None,
        torn: false,
    };
    let (read, write) = tokio::io::split(connector);
    let connector = Connector {
        frames: FrameReader::new(read, Call::Write.bounds(End::Host, &limits)),
        acks: write,
    };
    (writer, connector)
}

/// A flush's frame.
fn flush() -> Outgoing {
    let flush = v1::WriteFrame {
        frame: Some(Frame::Flush(v1::Unit {})),
    };
    Outgoing::message(None, &flush).expect("a flush fits a head")
}

/// Bytes: a flush's frame, head and body.
fn flush_bytes() -> usize {
    HEAD_BYTES + flush().len()
}

/// Fails the test unless `ended` failed as a call past its deadline, which is also how a writer
/// whose flush went unanswered, or whose frame was cut part way, refuses.
fn deadline_exceeded<T: std::fmt::Debug>(ended: rdlt_connector::Result<T>) {
    let error = ended.expect_err("the call fails");
    assert_eq!(
        (error.kind(), error.code()),
        (ConnectorErrorKind::Transient, Some(DEADLINE_EXCEEDED))
    );
}

#[tokio::test(start_paused = true)]
async fn a_frame_the_connector_takes_none_of_in_time_is_dropped_and_the_writer_goes_on() {
    // The stream holds one flush, so the second waits before any of it goes.
    let (mut writer, mut connector) = fixture(flush_bytes());
    writer
        .send(flush(), false)
        .await
        .expect("the first frame fits");
    let late = tokio::time::timeout(Duration::from_secs(2), writer.send(flush(), false))
        .await
        .expect("the write-ack deadline ends the send");
    deadline_exceeded(late);
    assert!(matches!(connector.message().await, Frame::Flush(_)));
    // The retry's schema, dictionary and batch, each taken as it comes.
    let taking = async {
        let first = connector.message().await;
        connector.frame().await;
        connector.frame().await;
        first
    };
    let retried = async { tokio::join!(writer.write(SegmentId(1), keyed("alpha")), taking) };
    let (written, first) = tokio::time::timeout(Duration::from_secs(2), retried)
        .await
        .expect("the retry ends");
    written.expect("the retry goes");
    assert!(
        matches!(first, Frame::Schema(_)),
        "the dropped frame never went"
    );
}

#[tokio::test(start_paused = true)]
async fn a_frame_cut_part_way_leaves_the_writer_refusing_every_call() {
    // The stream holds part of a flush.
    let (mut writer, _connector) = fixture(flush_bytes() - 1);
    let late = tokio::time::timeout(Duration::from_secs(2), writer.send(flush(), false))
        .await
        .expect("the write-ack deadline ends the send");
    deadline_exceeded(late);
    let written = tokio::time::timeout(
        Duration::from_millis(20),
        writer.write(SegmentId(1), keyed("alpha")),
    )
    .await
    .expect("the writer refuses at once");
    deadline_exceeded(written);
    let flushed = tokio::time::timeout(Duration::from_millis(20), writer.flush())
        .await
        .expect("the writer refuses at once");
    deadline_exceeded(flushed);
}

#[tokio::test(start_paused = true)]
async fn a_connector_lost_while_a_frame_waits_for_room_fails_the_send() {
    let (mut writer, _connector) = fixture(flush_bytes());
    writer
        .send(flush(), false)
        .await
        .expect("the first frame fits");
    let lost = writer.connection.lost.clone();
    let losing = async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        lost.cancel();
    };
    let sending = async { tokio::join!(writer.send(flush(), false), losing) };
    let (sent, ()) = tokio::time::timeout(Duration::from_secs(2), sending)
        .await
        .expect("the loss ends the send");
    let error = sent.expect_err("the connector is lost");
    assert_eq!(
        (error.kind(), error.code()),
        (ConnectorErrorKind::Transient, Some(CONNECTOR_LOST))
    );
}

#[tokio::test(start_paused = true)]
async fn a_write_its_connector_ended_fails_with_its_error() {
    let (mut writer, mut connector) = fixture(1 << 16);
    // The connector ended the write: its last answer says why, and it takes nothing more.
    let refused = v1::Error {
        kind: v1::ErrorKind::Data as i32,
        message: "the table is gone".to_owned(),
        code: Some("table_gone".to_owned()),
        ..v1::Error::default()
    };
    connector.answer(v1::write_ack::Ack::Error(refused)).await;
    drop(connector);
    let sending = tokio::time::timeout(Duration::from_secs(2), writer.send(flush(), false));
    let error = sending
        .await
        .expect("the write's end ends the send")
        .expect_err("the connector ended the write");
    assert_eq!(
        (error.kind(), error.code()),
        (ConnectorErrorKind::Data, Some("table_gone"))
    );
}

#[tokio::test(start_paused = true)]
async fn a_flush_whose_connector_ends_the_stream_unanswered_fails_as_the_connector_lost() {
    let (mut writer, mut connector) = fixture(1 << 16);
    let flushing = tokio::time::timeout(Duration::from_secs(2), writer.flush());
    let ending = async {
        assert!(matches!(connector.message().await, Frame::Flush(_)));
        // A connector that crashed or lost its connection ends the stream as one that closes it.
        drop(connector);
    };
    let (flushed, ()) = tokio::join!(flushing, ending);
    let error = flushed
        .expect("the stream's end ends the flush")
        .expect_err("the flush is unanswered");
    assert_eq!(
        (error.kind(), error.code()),
        (ConnectorErrorKind::Transient, Some(CONNECTOR_LOST))
    );
}

/// A batch of one dictionary column whose first value is `first`.
fn keyed(first: &str) -> RecordBatch {
    let values: DictionaryArray<Int8Type> = [first, "beta"].into_iter().collect();
    RecordBatch::try_from_iter([("value", Arc::new(values) as ArrayRef)]).expect("a batch")
}

/// The first value of `batch`'s dictionary column.
fn first_value(batch: &RecordBatch) -> String {
    let column = batch
        .column(0)
        .as_any()
        .downcast_ref::<DictionaryArray<Int8Type>>()
        .expect("a dictionary column");
    let values = column
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("text values");
    let key = usize::try_from(column.keys().value(0)).expect("a key of zero or more");
    values.value(key).to_owned()
}

/// What `decoder` makes of `frame`, a frame of a write's host, as the connector decodes it: a
/// batch, or nothing where it is a schema or a dictionary, or why it does not decode; `None`
/// where it is a flush.
fn decoded(decoder: &mut Decoder, frame: Received) -> Option<Result<Option<RecordBatch>, String>> {
    let outcome = match frame.head.kind {
        Kind::Batch { .. } => IpcFrame::framed(frame.body).and_then(|frame| decoder.frame(&frame)),
        Kind::Message { .. } => {
            let form = Call::Write
                .form(End::Host)
                .expect("a write's host sends messages");
            let message: v1::WriteFrame = frame.message(form, 1 << 20).expect("a message");
            match message.frame.expect("a frame") {
                Frame::Schema(schema) => decoder.schema(&schema.ipc_schema).map(|_| None),
                Frame::Flush(_) => return None,
            }
        }
        Kind::Json => panic!("a write sends no JSON"),
    };
    Some(outcome.map_err(|error| error.to_string()))
}

#[tokio::test(start_paused = true)]
async fn a_write_s_frames_decode_as_the_batch_written() {
    let (mut writer, mut connector) = fixture(1 << 16);
    tokio::time::timeout(
        Duration::from_secs(2),
        writer.write(SegmentId(1), keyed("alpha")),
    )
    .await
    .expect("the write ends")
    .expect("the write goes");
    let mut decoder = Decoder::new(Limits::default());
    let mut batches = Vec::new();
    for _ in 0..3 {
        let frame = connector.frame().await;
        let taken = decoded(&mut decoder, frame).expect("no flush");
        if let Some(batch) = taken.expect("each frame decodes") {
            batches.push(first_value(&batch));
        }
    }
    assert_eq!(batches, ["alpha"]);
}

fn stats(rows: u64) -> v1::write_ack::Ack {
    v1::write_ack::Ack::Flushed(v1::WriteStats { rows, bytes: rows })
}

#[tokio::test(start_paused = true)]
async fn a_writer_whose_flush_went_unanswered_refuses_every_later_call() {
    // The flush dropped by its caller, then one that passed its own deadline.
    for passes_deadline in [false, true] {
        let (mut writer, mut connector) = fixture(1 << 16);
        if passes_deadline {
            let ended = tokio::time::timeout(Duration::from_secs(2), writer.flush())
                .await
                .expect("the flush ends at its own deadline");
            deadline_exceeded(ended);
        } else {
            let dropped = tokio::time::timeout(Duration::from_millis(20), writer.flush()).await;
            assert!(dropped.is_err(), "the flush waits for its stats");
        }
        assert!(matches!(connector.message().await, Frame::Flush(_)));
        // Its stats arrive late: taken by a later flush, they would be counted as that flush's.
        connector.answer(stats(7)).await;
        let flushed = tokio::time::timeout(Duration::from_millis(20), writer.flush())
            .await
            .expect("the writer refuses at once");
        deadline_exceeded(flushed);
        let written = tokio::time::timeout(
            Duration::from_millis(20),
            writer.write(SegmentId(1), keyed("alpha")),
        )
        .await
        .expect("the writer refuses at once");
        deadline_exceeded(written);
        let sent = tokio::time::timeout(Duration::from_millis(20), connector.frames.next()).await;
        assert!(sent.is_err(), "a refused writer sends nothing");
    }
}

#[tokio::test(start_paused = true)]
async fn a_write_dropped_while_its_own_flush_waits_refuses_the_writer() {
    let (mut writer, mut connector) = fixture(1 << 16);
    // What the connector has staged is at its limit: the write's first frame flushes first.
    writer.staged = writer.connection.peer.staged_bytes();
    let writing = async {
        tokio::join!(writer.write(SegmentId(1), keyed("alpha")), async {
            (connector.message().await, connector.message().await)
        })
    };
    let dropped = tokio::time::timeout(Duration::from_millis(20), writing).await;
    assert!(dropped.is_err(), "the write's flush waits for its stats");
    let written = tokio::time::timeout(
        Duration::from_millis(20),
        writer.write(SegmentId(1), keyed("alpha")),
    )
    .await
    .expect("the writer refuses at once");
    deadline_exceeded(written);
}

#[tokio::test(start_paused = true)]
async fn a_flush_dropped_before_any_of_it_went_leaves_the_writer_usable() {
    // The stream is full, so the flush waits before any of it goes.
    let (mut writer, mut connector) = fixture(flush_bytes());
    writer
        .send(flush(), false)
        .await
        .expect("the first frame fits");
    let dropped = tokio::time::timeout(Duration::from_millis(20), writer.flush()).await;
    assert!(dropped.is_err(), "the flush waits for room");
    assert!(matches!(connector.message().await, Frame::Flush(_)));
    let answering = async {
        let sent = connector.message().await;
        connector.answer(stats(3)).await;
        sent
    };
    let flushing = async { tokio::join!(writer.flush(), answering) };
    let (flushed, sent) = tokio::time::timeout(Duration::from_millis(20), flushing)
        .await
        .expect("the flush goes");
    assert!(matches!(sent, Frame::Flush(_)));
    assert_eq!(flushed.expect("the flush is answered").rows, 3);
}

#[test]
fn any_answer_a_destination_sends_decodes_within_what_its_frame_is_charged() {
    use crate::remote::read::reservation;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime");
    let lengths = [0, 1, 255, 4_096, 65_536];
    let answers = lengths.into_iter().flat_map(|length| {
        let text = "e".repeat(length);
        [
            stats(u64::try_from(length).expect("a length")),
            v1::write_ack::Ack::Error(v1::Error {
                kind: v1::ErrorKind::Data as i32,
                message: text.clone(),
                code: Some("limit_exceeded".to_owned()),
                retry_after: Some(v1::Duration {
                    seconds: 1,
                    nanos: 2,
                }),
                limit: Some(v1::LimitExceeded {
                    name: text,
                    limit: u64::MAX,
                    actual: u64::MAX,
                }),
            }),
        ]
    });
    for ack in answers {
        let sent = v1::WriteAck { ack: Some(ack) };
        let decoded = runtime.block_on(async {
            let (mut connector, host) = tokio::io::duplex(1 << 20);
            let mut frame = Outgoing::message(None, &sent).expect("an answer fits a head");
            frame
                .write(&mut connector)
                .await
                .expect("the answer is taken");
            let limits = Limits::default();
            let mut answers = FrameReader::new(host, Call::Write.bounds(End::Connector, &limits));
            let received = answers
                .next()
                .await
                .expect("the answer arrives whole")
                .expect("an answer");
            let bound = usize::try_from(reservation(&received.head, 0)).expect("a bound");
            let form = Call::Write
                .form(End::Connector)
                .expect("a write's connector answers");
            received.message::<v1::WriteAck>(form, bound)
        });
        assert_eq!(decoded.ok(), Some(sent));
    }
}
