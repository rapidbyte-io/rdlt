use std::collections::VecDeque;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use bytes::Bytes;
use rdlt_wire::limits::MIN_FRAME_BYTES;
use rdlt_wire::stream::{Call, End, FrameReader, HEAD_BYTES, Head, Kind, Outgoing, Rows};
use rdlt_wire::{Cut, Encoder, Limits};
use tokio::io::{AsyncWriteExt as _, DuplexStream, ReadHalf, WriteHalf};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::writing;
use crate::destination::{DestinationWriter, WriteStats};
use crate::error::{ConnectorError, Result};
use crate::id::SegmentId;
use crate::serve::live::{Live, gone};
use crate::serve::stream::Frames;
use crate::spec::BoxFuture;
use crate::wire::v1;
use crate::wire::v1::write_ack::Ack;
use crate::wire::v1::write_frame::Frame;

/// Bounds every wait of a test: a write that hangs fails its test.
const WAIT: Duration = Duration::from_secs(60);

/// How long a test lets a task that should end at once go on before it fails.
const HOUR: Duration = Duration::from_mins(60);

/// Bytes: what the stream between a test's host and its write holds in each direction.
const BUFFERED: usize = 64 * 1024;

/// What a recording writer does when it is called.
#[derive(Clone, Copy, Debug)]
enum Turn {
    /// Finishes after this long.
    Takes(Duration),
    /// Fails.
    Fails,
    /// Panics.
    Panics,
}

/// What a recording writer was asked to do, in order.
#[derive(Clone, Debug, Default)]
struct Log(Arc<Mutex<Vec<String>>>);

impl Log {
    fn push(&self, event: String) {
        self.0.lock().expect("the log is not poisoned").push(event);
    }

    fn events(&self) -> Vec<String> {
        self.0.lock().expect("the log is not poisoned").clone()
    }
}

/// A writer that logs each write as `write segment:rows` and each flush as `flush`, each call
/// taking the next of its turns, or none once they run out.
struct Recording {
    log: Log,
    turns: VecDeque<Turn>,
    /// Rows: written since the last flush.
    rows: u64,
}

impl Recording {
    fn new(turns: impl IntoIterator<Item = Turn>) -> (Self, Log) {
        let log = Log::default();
        let writer = Self {
            log: log.clone(),
            turns: turns.into_iter().collect(),
            rows: 0,
        };
        (writer, log)
    }

    async fn turn(&mut self) -> Result<()> {
        match self.turns.pop_front() {
            None => Ok(()),
            Some(Turn::Takes(wait)) => {
                tokio::time::sleep(wait).await;
                Ok(())
            }
            Some(Turn::Fails) => Err(ConnectorError::data("the write was refused")),
            Some(Turn::Panics) => panic!("the writer held row 4417 of the ledger"),
        }
    }
}

impl DestinationWriter for Recording {
    fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.turn().await?;
            self.log
                .push(format!("write {}:{}", segment.0, batch.num_rows()));
            self.rows += u64::try_from(batch.num_rows()).expect("rows fit");
            Ok(())
        })
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<WriteStats>> {
        Box::pin(async move {
            self.turn().await?;
            self.log.push("flush".to_owned());
            let rows = std::mem::take(&mut self.rows);
            Ok(WriteStats { rows, bytes: 0 })
        })
    }
}

fn ids(rows: i64) -> RecordBatch {
    let ids: Int64Array = (0..rows).collect();
    RecordBatch::try_from_iter([("id", Arc::new(ids) as _)]).expect("a valid batch")
}

/// The frame of a message the host sends on a write.
fn message(frame: Frame) -> Outgoing {
    let message = v1::WriteFrame { frame: Some(frame) };
    Outgoing::message(None, &message).expect("a message fits a head")
}

fn schema() -> Outgoing {
    let ipc_schema = Encoder::default()
        .schema(&ids(0).schema())
        .expect("the schema encodes");
    message(Frame::Schema(v1::WriteSchema {
        version: 1,
        ipc_schema,
    }))
}

/// The frame of a batch of `rows` ids for `segment`.
fn batch(segment: u64, rows: i64) -> Outgoing {
    let mut encoder = Encoder::default();
    let batch = ids(rows);
    encoder.schema(&batch.schema()).expect("the schema encodes");
    let mut cut = Cut::new(batch, Limits::default());
    let mut frames = encoder
        .piece(&mut cut)
        .expect("the batch encodes")
        .expect("the batch has rows");
    assert_eq!(frames.len(), 1, "ids need no dictionary");
    let frame = frames.remove(0);
    let kind = Kind::Batch {
        rows: Rows::Arrow,
        dictionary: false,
        tag: segment,
    };
    Outgoing::new(kind, frame.into_pieces()).expect("a batch fits a head")
}

/// A batch frame of `bytes` that does not decode.
fn garbage(bytes: usize) -> Outgoing {
    let kind = Kind::Batch {
        rows: Rows::Arrow,
        dictionary: false,
        tag: 1,
    };
    Outgoing::new(kind, vec![Bytes::from(vec![0; bytes])]).expect("a batch fits a head")
}

fn flush() -> Outgoing {
    message(Frame::Flush(v1::Unit {}))
}

/// The host's end of a write: what it sends, and the answers it reads.
struct Host {
    answers: FrameReader<ReadHalf<DuplexStream>>,
    frames: WriteHalf<DuplexStream>,
}

impl Host {
    /// Sends `frame` whole.
    async fn send(&mut self, mut frame: Outgoing) {
        tokio::time::timeout(WAIT, frame.write(&mut self.frames))
            .await
            .expect("the write takes the frame")
            .expect("the write is open");
    }

    /// The write's answers, up to its end or the `most`th.
    async fn answers(&mut self, most: usize) -> Vec<Ack> {
        let mut all = Vec::new();
        let form = Call::Write
            .form(End::Connector)
            .expect("a write's connector answers");
        while all.len() < most
            && let Some(answer) = tokio::time::timeout(WAIT, self.answers.next())
                .await
                .expect("the write answers or ends")
                .expect("every answer arrives whole")
        {
            let answer: v1::WriteAck = answer.message(form, 1 << 20).expect("an answer decodes");
            all.push(answer.ack.expect("an answer carries an ack"));
        }
        all
    }
}

/// A write to `writer` within `limits`, its opening read: the host's end, and the write.
async fn served(
    limits: Limits,
    writer: impl DestinationWriter + 'static,
) -> (Host, JoinHandle<()>) {
    let (mut host, connector) = tokio::io::duplex(BUFFERED);
    let mut opening = Outgoing::message(Some(Call::Write), &v1::WriteStart::default())
        .expect("a start fits a head");
    opening.write(&mut host).await.expect("the opening is sent");
    let (read, write) = tokio::io::split(connector);
    let window = rdlt_wire::bounded::Window::new(4 * limits.largest());
    let mut frames = Frames::new(read, limits, window);
    tokio::time::timeout(WAIT, frames.opening())
        .await
        .expect("the opening arrives")
        .expect("the opening reads")
        .expect("the host opened a call");
    let writing = tokio::spawn(writing(Box::new(writer), frames, write, limits));
    let (answers, frames) = tokio::io::split(host);
    let answers = FrameReader::new(answers, Call::Write.bounds(End::Connector, &limits));
    (Host { answers, frames }, writing)
}

/// The answers of a write of `sent` to `writer` within `limits`, each sent the wait `waits`
/// holds for it after the frame before it, up to the write's end or the `most`th; then the host
/// goes, and the write ends.
async fn answers(
    limits: Limits,
    writer: Recording,
    sent: Vec<Outgoing>,
    waits: Vec<Duration>,
    most: usize,
) -> Vec<Ack> {
    let (mut host, writing) = served(limits, writer).await;
    let waits = waits.into_iter().chain(std::iter::repeat(Duration::ZERO));
    for (frame, wait) in sent.into_iter().zip(waits) {
        tokio::time::sleep(wait).await;
        host.send(frame).await;
    }
    let answers = host.answers(most).await;
    drop(host);
    tokio::time::timeout(HOUR, writing)
        .await
        .expect("the write ends once its host goes")
        .expect("the write does not panic");
    answers
}

/// A writer whose writes and flushes wait for ever once begun, telling `begun` of the first, and
/// counted in its `Live` while it lives.
struct Stuck {
    begun: Option<oneshot::Sender<()>>,
    _live: Live,
}

impl Stuck {
    async fn waits(&mut self) {
        if let Some(begun) = self.begun.take() {
            begun.send(()).ok();
        }
        std::future::pending::<()>().await;
    }
}

impl DestinationWriter for Stuck {
    fn write(&mut self, _: SegmentId, _: RecordBatch) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.waits().await;
            Ok(())
        })
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<WriteStats>> {
        Box::pin(async move {
            self.waits().await;
            Ok(WriteStats { rows: 0, bytes: 0 })
        })
    }
}

/// Waits of up to 30 milliseconds, the same for the same `seed`.
fn waits(seed: u64, count: usize) -> Vec<Duration> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..count)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            Duration::from_millis(state % 31)
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn frames_are_written_in_order_and_a_flush_follows_them() {
    let limits = Limits::default();
    let sent = || {
        vec![
            schema(),
            batch(1, 10),
            batch(1, 20),
            batch(2, 30),
            flush(),
            batch(2, 40),
        ]
    };
    for seed in 0..32 {
        let turns = waits(seed, 5).into_iter().map(Turn::Takes);
        let (writer, log) = Recording::new(turns);
        let answers = answers(limits, writer, sent(), waits(seed + 100, 6), 1).await;
        assert_eq!(
            answers,
            [Ack::Flushed(v1::WriteStats { rows: 60, bytes: 0 })],
            "seed {seed}"
        );
        // The batch after the flush may or may not be written before the host goes.
        let events = log.events();
        assert_eq!(
            events[..4],
            ["write 1:10", "write 1:20", "write 2:30", "flush"],
            "seed {seed}"
        );
        assert!(events.len() <= 5, "seed {seed}: {events:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_frame_that_does_not_decode_fails_the_write_after_the_frames_before_it() {
    let sent = vec![
        schema(),
        batch(1, 10),
        batch(1, 20),
        garbage(64),
        batch(1, 30),
    ];
    let (writer, log) = Recording::new([Turn::Takes(Duration::from_millis(50)); 2]);
    let answers = answers(Limits::default(), writer, sent, Vec::new(), usize::MAX).await;
    assert_eq!(log.events(), ["write 1:10", "write 1:20"]);
    let [Ack::Error(error)] = answers.as_slice() else {
        panic!("the write fails with the frame's error alone: {answers:?}");
    };
    assert_eq!(
        error.code.as_deref(),
        Some(crate::wire::MALFORMED_FRAME),
        "{error:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_writer_that_fails_ends_the_write_with_its_error_alone() {
    // The engine sends three frames and then nothing more, without ending the write.
    let sent = vec![schema(), batch(1, 10), batch(1, 20)];
    let (writer, log) = Recording::new([Turn::Takes(Duration::ZERO), Turn::Fails]);
    let answers = answers(Limits::default(), writer, sent, Vec::new(), usize::MAX).await;
    assert_eq!(log.events(), ["write 1:10"]);
    let [Ack::Error(error)] = answers.as_slice() else {
        panic!("the write fails with the writer's error alone: {answers:?}");
    };
    assert_eq!(error.kind(), v1::ErrorKind::Data, "{error:?}");
}

#[tokio::test(start_paused = true)]
async fn a_writer_that_panics_fails_the_write_as_internal_telling_neither_its_message_nor_task() {
    let sent = vec![schema(), batch(1, 10), batch(1, 20)];
    let (writer, log) = Recording::new([Turn::Takes(Duration::ZERO), Turn::Panics]);
    let answers = answers(Limits::default(), writer, sent, Vec::new(), usize::MAX).await;
    assert_eq!(log.events(), ["write 1:10"]);
    let [Ack::Error(error)] = answers.as_slice() else {
        panic!("the write fails with the panic's error alone: {answers:?}");
    };
    assert_eq!(error.kind(), v1::ErrorKind::Internal, "{error:?}");
    assert!(
        !error.message.contains("4417") && !error.message.contains("task"),
        "the host is told {error:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn the_staged_bound_refuses_a_frame_before_it_decodes() {
    // Four frames of nearly the least frame limit fill the sixteen mebibytes a write may stage
    // between flushes; a fifth, which would not decode, is refused for its size instead.
    let limits = Limits {
        frame_bytes: MIN_FRAME_BYTES,
        ..Limits::default()
    };
    let rows = i64::try_from(MIN_FRAME_BYTES * 15 / 16 / 8).expect("fits");
    let size = batch(1, rows).len() - HEAD_BYTES;
    let mut sent = vec![schema()];
    sent.extend((0..4).map(|_| batch(1, rows)));
    sent.extend([garbage(size), flush()]);
    let (writer, log) = Recording::new([Turn::Takes(Duration::from_millis(10)); 4]);
    let answers = answers(limits, writer, sent, Vec::new(), usize::MAX).await;
    let written = format!("write 1:{rows}");
    assert_eq!(log.events(), vec![written; 4]);
    let [Ack::Error(error)] = answers.as_slice() else {
        panic!("the write is refused: {answers:?}");
    };
    assert_eq!(error.code.as_deref(), Some("limit_exceeded"), "{error:?}");
    let limit = error.limit.as_ref().expect("a refusal names its limit");
    assert_eq!(limit.name, "staged bytes");
}

#[tokio::test(start_paused = true)]
async fn a_frame_beyond_the_frame_limit_fails_the_write_with_its_refusal_before_its_body() {
    let limits = Limits::default();
    let (writer, log) = Recording::new([Turn::Takes(Duration::from_millis(50)); 2]);
    let (mut host, writing) = served(limits, writer).await;
    for frame in [schema(), batch(1, 10), batch(1, 20)] {
        host.send(frame).await;
    }
    // A head declaring a frame beyond the limit, and none of its body.
    let kind = Kind::Batch {
        rows: Rows::Arrow,
        dictionary: false,
        tag: 1,
    };
    let bound = Call::Write.bounds(End::Host, &limits).batch;
    let beyond = bound.expect("a write's host sends batches").bytes + 1;
    let head = Head::new(kind, beyond).encode();
    host.frames
        .write_all(&head)
        .await
        .expect("the head is sent");
    let answers = host.answers(usize::MAX).await;
    tokio::time::timeout(HOUR, writing)
        .await
        .expect("the write ends")
        .expect("the write does not panic");
    assert_eq!(log.events(), ["write 1:10", "write 1:20"]);
    let [Ack::Error(refused)] = answers.as_slice() else {
        panic!("the write fails with the frame's refusal: {answers:?}");
    };
    assert_eq!(
        refused.code.as_deref(),
        Some("limit_exceeded"),
        "{refused:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_write_whose_host_goes_within_or_between_frames_ends_unanswered_and_drops_its_writer() {
    let limits = Limits::default();
    for within in [false, true] {
        let live = Arc::new(AtomicUsize::new(0));
        let (begun, beginning) = oneshot::channel();
        let writer = Stuck {
            begun: Some(begun),
            _live: Live::new(&live),
        };
        let (mut host, writing) = served(limits, writer).await;
        for frame in [schema(), batch(1, 10)] {
            host.send(frame).await;
        }
        tokio::time::timeout(WAIT, beginning)
            .await
            .expect("the writer begins")
            .expect("the writer says so");
        if within {
            // A frame's head, and none of its body.
            let kind = Kind::Batch {
                rows: Rows::Arrow,
                dictionary: false,
                tag: 1,
            };
            let head = Head::new(kind, 1_000).encode();
            host.frames
                .write_all(&head)
                .await
                .expect("the head is sent");
        }
        host.frames.shutdown().await.ok();
        let answers = host.answers(usize::MAX).await;
        assert_eq!(answers, [], "within a frame: {within}");
        tokio::time::timeout(HOUR, writing)
            .await
            .expect("the write ends once its host goes")
            .expect("the write does not panic");
        tokio::time::timeout(WAIT, gone(&live))
            .await
            .expect("the writer is dropped with the write");
    }
}

#[tokio::test(start_paused = true)]
async fn a_writer_waiting_in_a_write_a_flush_or_for_a_frame_is_dropped_once_its_host_goes() {
    let limits = Limits::default();
    let cases: [fn() -> Vec<Outgoing>; 4] = [
        || vec![flush()],
        || vec![schema(), batch(1, 10)],
        || vec![schema()],
        // The writer waits in the first batch while the frames after it fill every stage.
        || vec![schema(), batch(1, 10), batch(2, 10), flush()],
    ];
    for (case, sent) in cases.into_iter().enumerate() {
        let live = Arc::new(AtomicUsize::new(0));
        let (begun, beginning) = oneshot::channel();
        let writer = Stuck {
            begun: Some(begun),
            _live: Live::new(&live),
        };
        let (mut host, writing) = served(limits, writer).await;
        for frame in sent() {
            host.send(frame).await;
        }
        if case == 2 {
            // The writer took the schema and waits for the next frame.
            tokio::time::sleep(Duration::from_secs(1)).await;
        } else {
            tokio::time::timeout(WAIT, beginning)
                .await
                .expect("the writer begins")
                .expect("the writer says so");
        }
        drop(host);
        tokio::time::timeout(HOUR, writing)
            .await
            .unwrap_or_else(|_| panic!("case {case}: the write ends once its host goes"))
            .expect("the write does not panic");
        tokio::time::timeout(WAIT, gone(&live))
            .await
            .expect("the writer is dropped with the write");
    }
}
