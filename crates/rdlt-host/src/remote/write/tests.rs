mod cancelled;

use std::sync::Arc;
use std::time::Duration;

use arrow_array::types::Int8Type;
use arrow_array::{Array as _, ArrayRef, DictionaryArray, RecordBatch, StringArray};
use rdlt_connector::wire::v1;
use rdlt_connector::wire::v1::write_frame::Frame;
use rdlt_connector::{ConnectorErrorKind, DestinationWriter as _, SegmentId, WriteStats};
use rdlt_wire::bounded::{Bounded, Bounds};
use rdlt_wire::flow::Spending;
use rdlt_wire::limits::Class;
use rdlt_wire::plane::{Incoming, Outgoing};
use rdlt_wire::{Decoder, Encoder, IpcFrame, Limits};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use super::RemoteWriter;
use crate::remote::checked::Checked;
use crate::remote::clients::Clients;
use crate::remote::{CONNECTOR_LOST, Connection, DEADLINE_EXCEEDED, Deadlines, Options};

/// A writer over no connector: what it sends queues in `frames`, which holds one frame, and
/// its answers are what `acks` sends.
struct Fixture {
    writer: RemoteWriter,
    frames: mpsc::Receiver<v1::WriteFrame>,
    acks: mpsc::Sender<v1::WriteAck>,
}

/// A writer with `credit` bytes of credit, each wait for an answer due within a second.
fn fixture(credit: u64) -> Fixture {
    let options = Options {
        deadlines: Deadlines {
            write_ack: Duration::from_secs(1),
            ..Deadlines::default()
        },
        ..Options::default()
    };
    let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
    let checked = Checked::new(channel, options.limits);
    let connection = Arc::new(Connection {
        client: Clients::new(&checked, &options.limits),
        spec: v1::ConnectorSpec::default(),
        peer: options.limits,
        options,
        lost: CancellationToken::new(),
        spent: CancellationToken::new(),
    });
    let (acks, answers) = mpsc::channel(8);
    let most = options.limits.largest();
    let body = tonic::body::Body::new(Outgoing::request(ReceiverStream::new(answers), most));
    let bounds = Bounds::of(
        &options.limits,
        Class::of_answer("Write"),
        rdlt_wire::scan::response("Write"),
    );
    let (sender, frames) = mpsc::channel(1);
    let mut spending = Spending::default();
    if credit > 0 {
        spending.grant(credit).expect("a credit of some bytes");
    }
    let writer = RemoteWriter {
        connection,
        frames: sender,
        acks: Incoming::request(Bounded::new(body, bounds, None)),
        credit: spending,
        encoder: Encoder::default(),
        schema: None,
        version: 1,
        staged: 0,
        flushed: WriteStats::default(),
        unanswered: false,
    };
    Fixture {
        writer,
        frames,
        acks,
    }
}

fn flush() -> v1::WriteFrame {
    v1::WriteFrame {
        frame: Some(Frame::Flush(v1::Unit {})),
    }
}

fn credit(bytes: u64) -> v1::WriteAck {
    v1::WriteAck {
        ack: Some(v1::write_ack::Ack::Credit(v1::Credit { bytes })),
    }
}

/// The next `count` frames the writer queued, each within a second.
async fn queued(frames: &mut mpsc::Receiver<v1::WriteFrame>, count: usize) -> Vec<Frame> {
    let mut queued = Vec::with_capacity(count);
    for _ in 0..count {
        let frame = tokio::time::timeout(Duration::from_secs(1), frames.recv())
            .await
            .expect("a frame is queued in time")
            .expect("the writer is open");
        queued.push(frame.frame.expect("a frame"));
    }
    queued
}

#[tokio::test(start_paused = true)]
async fn a_frame_its_deadline_kept_from_the_queue_spends_no_credit_and_goes_when_retried() {
    let Fixture {
        mut writer,
        mut frames,
        acks: _acks,
    } = fixture(1);
    writer.frames.try_send(flush()).expect("room for one frame");
    let sending = tokio::time::timeout(
        Duration::from_secs(2),
        writer.send(Frame::Flush(v1::Unit {})),
    );
    let late = sending
        .await
        .expect("the write-ack deadline ends the send")
        .expect_err("the queue stays full");
    assert_eq!(late.code(), Some(DEADLINE_EXCEEDED));
    assert_eq!(
        writer.credit.credit(),
        1,
        "a frame never queued spent credit"
    );
    queued(&mut frames, 1).await;
    let retried = tokio::time::timeout(
        Duration::from_secs(2),
        writer.send(Frame::Flush(v1::Unit {})),
    );
    retried
        .await
        .expect("the retry ends")
        .expect("the retry goes on the credit left");
    assert!(matches!(
        queued(&mut frames, 1).await[..],
        [Frame::Flush(_)]
    ));
}

#[tokio::test(start_paused = true)]
async fn a_connector_lost_while_a_frame_waits_for_room_spends_no_credit() {
    let Fixture {
        mut writer,
        frames: _frames,
        acks: _acks,
    } = fixture(1);
    writer.frames.try_send(flush()).expect("room for one frame");
    let lost = writer.connection.lost.clone();
    let losing = async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        lost.cancel();
    };
    let sending = async { tokio::join!(writer.send(Frame::Flush(v1::Unit {})), losing) };
    let (sent, ()) = tokio::time::timeout(Duration::from_secs(2), sending)
        .await
        .expect("the loss ends the send");
    let error = sent.expect_err("the connector is lost");
    assert_eq!(
        (error.kind(), error.code()),
        (ConnectorErrorKind::Transient, Some(CONNECTOR_LOST))
    );
    assert_eq!(
        writer.credit.credit(),
        1,
        "a frame never queued spent credit"
    );
}

#[tokio::test(start_paused = true)]
async fn a_write_its_connector_ended_fails_with_its_error_and_spends_no_credit() {
    let Fixture {
        mut writer,
        frames,
        acks,
    } = fixture(1);
    // The connector ended the write: the call's frames go nowhere, and its last answer says why.
    drop(frames);
    let refused = v1::Error {
        kind: v1::ErrorKind::Data as i32,
        message: "the table is gone".to_owned(),
        code: Some("table_gone".to_owned()),
        ..v1::Error::default()
    };
    let answer = v1::WriteAck {
        ack: Some(v1::write_ack::Ack::Error(refused)),
    };
    acks.send(answer).await.expect("the answers are read");
    let sending = tokio::time::timeout(
        Duration::from_secs(2),
        writer.send(Frame::Flush(v1::Unit {})),
    );
    let error = sending
        .await
        .expect("the write's end ends the send")
        .expect_err("the connector ended the write");
    assert_eq!(
        (error.kind(), error.code()),
        (ConnectorErrorKind::Data, Some("table_gone"))
    );
    assert_eq!(
        writer.credit.credit(),
        1,
        "a frame never queued spent credit"
    );
}

#[tokio::test(start_paused = true)]
async fn a_send_dropped_while_it_waits_for_credit_goes_on_the_credit_granted_meanwhile() {
    let Fixture {
        mut writer,
        mut frames,
        acks,
    } = fixture(0);
    let dropped = tokio::time::timeout(
        Duration::from_millis(20),
        writer.send(Frame::Flush(v1::Unit {})),
    );
    assert!(dropped.await.is_err(), "no credit is granted");
    acks.send(credit(64)).await.expect("the answers are read");
    let retried = tokio::time::timeout(
        Duration::from_secs(2),
        writer.send(Frame::Flush(v1::Unit {})),
    );
    retried
        .await
        .expect("the retry ends")
        .expect("the retry goes on the credit granted");
    assert!(matches!(
        queued(&mut frames, 1).await[..],
        [Frame::Flush(_)]
    ));
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

/// The batch `frames` decode to with `decoder`, as the connector would decode them.
fn decoded(decoder: &mut Decoder, frames: &[Frame]) -> RecordBatch {
    let mut last = None;
    for frame in frames {
        match frame {
            Frame::Schema(schema) => {
                decoder
                    .schema(&schema.ipc_schema)
                    .expect("the schema decodes");
            }
            Frame::Batch(batch) => {
                let frame = IpcFrame::new(batch.data_header.clone(), batch.data_body.clone());
                last = decoder.frame(&frame).expect("the frame decodes").or(last);
            }
            other => panic!("not a frame of a batch: {other:?}"),
        }
    }
    last.expect("a batch is decoded")
}

/// Writes `batch` while taking its three frames from `frames`, all within two seconds.
async fn written(
    writer: &mut RemoteWriter,
    frames: &mut mpsc::Receiver<v1::WriteFrame>,
    batch: RecordBatch,
) -> Vec<Frame> {
    let writing = async { tokio::join!(writer.write(SegmentId(1), batch), queued(frames, 3)) };
    let (written, sent) = tokio::time::timeout(Duration::from_secs(2), writing)
        .await
        .expect("the write ends");
    written.expect("the write is written");
    sent
}

#[tokio::test(start_paused = true)]
async fn a_dictionary_write_dropped_at_any_frame_sends_its_schema_and_dictionary_again() {
    // The frames the queue gives up before the write waits: none, so it waits at its schema;
    // one, so at its dictionary; two, so at its batch.
    for taken in 0..3 {
        let Fixture {
            mut writer,
            mut frames,
            acks: _acks,
        } = fixture(1 << 20);
        // A frame already queued: the write's frames wait behind it.
        writer.frames.try_send(flush()).expect("room for one frame");
        let writing = async {
            tokio::join!(
                writer.write(SegmentId(1), keyed("alpha")),
                queued(&mut frames, taken)
            )
        };
        let dropped = tokio::time::timeout(Duration::from_millis(20), writing).await;
        assert!(dropped.is_err(), "the write waits after {taken} frames");
        queued(&mut frames, 1).await;
        let sent = written(&mut writer, &mut frames, keyed("alpha")).await;
        assert!(
            matches!(sent[0], Frame::Schema(_)),
            "after {taken}: {sent:?}"
        );
        let batch = decoded(&mut Decoder::new(Limits::default()), &sent);
        assert_eq!(first_value(&batch), "alpha", "after {taken}");
    }
}

fn stats(rows: u64) -> v1::WriteAck {
    v1::WriteAck {
        ack: Some(v1::write_ack::Ack::Flushed(v1::WriteStats {
            rows,
            bytes: rows,
        })),
    }
}

/// Fails the test unless `ended` failed as a call past its deadline, which is also how a writer
/// whose flush went unanswered refuses.
fn deadline_exceeded<T: std::fmt::Debug>(ended: rdlt_connector::Result<T>) {
    let error = ended.expect_err("the call fails");
    assert_eq!(
        (error.kind(), error.code()),
        (ConnectorErrorKind::Transient, Some(DEADLINE_EXCEEDED))
    );
}

#[tokio::test(start_paused = true)]
async fn a_writer_whose_flush_went_unanswered_refuses_every_later_call() {
    // The flush dropped by its caller, then one that passed its own deadline.
    for passes_deadline in [false, true] {
        let Fixture {
            mut writer,
            mut frames,
            acks,
        } = fixture(1 << 20);
        if passes_deadline {
            let ended = tokio::time::timeout(Duration::from_secs(2), writer.flush())
                .await
                .expect("the flush ends at its own deadline");
            deadline_exceeded(ended);
        } else {
            let dropped = tokio::time::timeout(Duration::from_millis(20), writer.flush()).await;
            assert!(dropped.is_err(), "the flush waits for its stats");
        }
        assert!(matches!(
            queued(&mut frames, 1).await[..],
            [Frame::Flush(_)]
        ));
        // Its stats arrive late: taken by a later flush, they would be counted as that flush's.
        tokio::time::timeout(Duration::from_millis(20), acks.send(stats(7)))
            .await
            .expect("the answers have room")
            .expect("the answers are read");
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
        assert!(frames.try_recv().is_err(), "a refused writer sends nothing");
    }
}

#[tokio::test(start_paused = true)]
async fn a_write_dropped_while_its_own_flush_waits_refuses_the_writer() {
    let Fixture {
        mut writer,
        mut frames,
        acks: _acks,
    } = fixture(1 << 20);
    // What the connector has staged is at its limit: the write's first frame flushes first.
    writer.staged = writer.connection.peer.staged_bytes();
    let writing = async {
        tokio::join!(
            writer.write(SegmentId(1), keyed("alpha")),
            queued(&mut frames, 2)
        )
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
    assert!(frames.try_recv().is_err(), "a refused writer sends nothing");
}
