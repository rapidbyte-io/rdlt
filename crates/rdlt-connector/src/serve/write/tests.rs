use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use bytes::Bytes;
use rdlt_wire::bounded::{Bounded, Bounds};
use rdlt_wire::flow::Granting;
use rdlt_wire::limits::{CREDIT_FLOOR, Class, MIN_FRAME_BYTES};
use rdlt_wire::plane::{Incoming, Outgoing};
use rdlt_wire::prost::Message as _;
use rdlt_wire::tonic::body::Body;
use rdlt_wire::tonic::{Code, Status};
use rdlt_wire::{Encoder, Limits};
use tokio::time::Instant;
use tokio_stream::{Stream, StreamExt as _};

use super::writing;
use crate::destination::{DestinationWriter, WriteStats};
use crate::error::{ConnectorError, Result};
use crate::id::SegmentId;
use crate::spec::BoxFuture;
use crate::wire::v1;
use crate::wire::v1::write_ack::Ack;
use crate::wire::v1::write_frame::Frame;

/// Bounds every wait of a test: a write that hangs fails its test.
const WAIT: Duration = Duration::from_secs(60);

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
            Some(Turn::Panics) => panic!("the writer panicked"),
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

fn schema() -> v1::WriteFrame {
    let ipc_schema = Encoder::default()
        .schema(&ids(0).schema())
        .expect("the schema encodes");
    v1::WriteFrame {
        frame: Some(Frame::Schema(v1::WriteSchema {
            version: 1,
            ipc_schema,
        })),
    }
}

/// The frame of a batch of `rows` ids for `segment`.
fn batch(segment: u64, rows: i64) -> v1::WriteFrame {
    let mut encoder = Encoder::default();
    let batch = ids(rows);
    encoder.schema(&batch.schema()).expect("the schema encodes");
    let mut frames = encoder.batch(&batch).expect("the batch encodes");
    assert_eq!(frames.len(), 1, "ids need no dictionary");
    let frame = frames.remove(0);
    v1::WriteFrame {
        frame: Some(Frame::Batch(v1::WriteBatch {
            segment,
            data_header: frame.header,
            data_body: frame.body,
        })),
    }
}

/// A batch frame of `bytes` that does not decode.
fn garbage(bytes: usize) -> v1::WriteFrame {
    v1::WriteFrame {
        frame: Some(Frame::Batch(v1::WriteBatch {
            segment: 1,
            data_header: Bytes::from_static(b"not an IPC message"),
            data_body: Bytes::from(vec![0; bytes.saturating_sub(18)]),
        })),
    }
}

fn flush() -> v1::WriteFrame {
    v1::WriteFrame {
        frame: Some(Frame::Flush(v1::Unit {})),
    }
}

/// The credit a write within `limits` answers with: its opening credit, then a credit for each
/// of `frames` in turn.
fn credits(limits: &Limits, frames: &[v1::WriteFrame]) -> Vec<Ack> {
    let mut granting = Granting::new(CREDIT_FLOOR, limits);
    let opening = granting.opening();
    let taken = frames.iter().map(|frame| {
        let bytes = u64::try_from(frame.encoded_len()).expect("a length fits");
        granting.taken(bytes)
    });
    std::iter::once(opening)
        .chain(taken.collect::<Vec<_>>())
        .map(|bytes| Ack::Credit(v1::Credit { bytes }))
        .collect()
}

/// `frames`, each arriving `wait` after the write asks for it, counting those asked for.
fn arriving(
    frames: Vec<v1::WriteFrame>,
    waits: Vec<Duration>,
    asked: Arc<AtomicUsize>,
) -> impl Stream<Item = v1::WriteFrame> + Send + 'static {
    tokio_stream::iter(frames.into_iter().zip(waits)).then(move |(frame, wait)| {
        asked.fetch_add(1, Ordering::SeqCst);
        async move {
            tokio::time::sleep(wait).await;
            frame
        }
    })
}

/// The answers of a write of `frames` to `writer` within `limits`, up to its end: each an
/// answer, or the code of the status that ended it.
async fn answers(
    limits: Limits,
    writer: Recording,
    frames: impl Stream<Item = v1::WriteFrame> + Send + 'static,
) -> Vec<std::result::Result<Ack, Code>> {
    let body = Body::new(Outgoing::request(frames, limits.largest()));
    answered(limits, bounds(&limits), writer, body).await
}

/// The bounds a served write's request is held to within `limits`.
fn bounds(limits: &Limits) -> Bounds {
    Bounds::of(limits, Class::Data, rdlt_wire::scan::request("Write"))
}

/// The answers of a write of the frames `body` carries, held to `bounds`, as [`answers`] gives
/// them.
async fn answered(
    limits: Limits,
    bounds: Bounds,
    writer: Recording,
    body: Body,
) -> Vec<std::result::Result<Ack, Code>> {
    let incoming = Incoming::request(Bounded::new(body, bounds, None));
    let mut answers = writing(Box::new(writer), incoming, limits);
    let mut all = Vec::new();
    while let Some(answer) = tokio::time::timeout(WAIT, answers.next())
        .await
        .expect("the write ends")
    {
        let answer = answer.map(|answer| answer.ack.expect("an answer carries an ack"));
        all.push(answer.map_err(|status| status.code()));
    }
    all
}

/// A request's body of `frames`, each a gRPC message, which then fails with `status`.
struct Cut {
    frames: VecDeque<v1::WriteFrame>,
    status: Option<Status>,
}

impl http_body::Body for Cut {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<http_body::Frame<Bytes>, Status>>> {
        let Some(frame) = self.frames.pop_front() else {
            return Poll::Ready(self.status.take().map(Err));
        };
        let encoded = frame.encode_to_vec();
        let length = u32::try_from(encoded.len()).expect("a message's length fits");
        let mut message = vec![0];
        message.extend(length.to_be_bytes());
        message.extend(encoded);
        Poll::Ready(Some(Ok(http_body::Frame::data(Bytes::from(message)))))
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
    let sent = vec![
        schema(),
        batch(1, 10),
        batch(1, 20),
        batch(2, 30),
        flush(),
        batch(2, 40),
    ];
    let mut expected = credits(&limits, &sent);
    // The flush's stats follow its credit, and the credit of the batch after it follows them.
    expected.insert(6, Ack::Flushed(v1::WriteStats { rows: 60, bytes: 0 }));
    for seed in 0..32 {
        let turns = waits(seed, 5).into_iter().map(Turn::Takes);
        let (writer, log) = Recording::new(turns);
        let asked = Arc::default();
        let frames = arriving(sent.clone(), waits(seed + 100, sent.len()), asked);
        let answers = answers(limits, writer, frames).await;
        assert_eq!(
            log.events(),
            [
                "write 1:10",
                "write 1:20",
                "write 2:30",
                "flush",
                "write 2:40"
            ],
            "seed {seed}"
        );
        let answers: Vec<_> = answers.into_iter().map(Result::unwrap).collect();
        assert_eq!(answers, expected, "seed {seed}");
    }
}

#[tokio::test(start_paused = true)]
async fn the_next_frame_arrives_and_decodes_while_one_is_written() {
    // Each frame arrives 10 ms after it is asked for, and each write takes 10 ms: in turn, the
    // schema and four batches take 90 ms; each received while the frame before it is written,
    // 60.
    let sent = vec![
        schema(),
        batch(1, 10),
        batch(1, 20),
        batch(1, 30),
        batch(1, 40),
    ];
    let (writer, log) = Recording::new([Turn::Takes(Duration::from_millis(10)); 4]);
    let frames = arriving(sent, vec![Duration::from_millis(10); 5], Arc::default());
    let started = Instant::now();
    let answers = answers(Limits::default(), writer, frames).await;
    assert_eq!(started.elapsed(), Duration::from_millis(60));
    assert_eq!(log.events().len(), 4);
    assert!(answers.iter().all(Result::is_ok), "{answers:?}");
}

#[tokio::test(start_paused = true)]
async fn a_frame_that_does_not_decode_fails_the_write_after_the_frames_before_it() {
    let limits = Limits::default();
    let good = vec![schema(), batch(1, 10), batch(1, 20)];
    let mut sent = good.clone();
    sent.extend([garbage(64), batch(1, 30)]);
    let (writer, log) = Recording::new([Turn::Takes(Duration::from_millis(50)); 2]);
    let answers = answers(limits, writer, tokio_stream::iter(sent)).await;
    assert_eq!(log.events(), ["write 1:10", "write 1:20"]);
    let (error, credited) = answers.split_last().expect("the write answers");
    let credited: Vec<_> = credited.iter().cloned().map(Result::unwrap).collect();
    assert_eq!(credited, credits(&limits, &good));
    let Ok(Ack::Error(error)) = error else {
        panic!("the write fails with the frame's error: {error:?}");
    };
    assert_eq!(
        error.code.as_deref(),
        Some(crate::wire::MALFORMED_FRAME),
        "{error:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_writer_that_fails_ends_the_write_and_no_credit_follows_its_error() {
    // The engine sends three frames and then nothing more, without ending the write.
    let sent = vec![schema(), batch(1, 10), batch(1, 20)];
    let asked = Arc::new(AtomicUsize::new(0));
    let frames =
        arriving(sent, vec![Duration::ZERO; 3], Arc::clone(&asked)).chain(tokio_stream::pending());
    let (writer, log) = Recording::new([Turn::Takes(Duration::ZERO), Turn::Fails]);
    let answers = answers(Limits::default(), writer, frames).await;
    assert_eq!(log.events(), ["write 1:10"]);
    let error = answers
        .iter()
        .position(|answer| matches!(answer, Ok(Ack::Error(_))))
        .expect("the write fails");
    assert_eq!(error, answers.len() - 1, "{answers:?}");
}

#[tokio::test(start_paused = true)]
async fn a_writer_that_panics_fails_the_write_as_internal() {
    let sent = vec![schema(), batch(1, 10), batch(1, 20)];
    let frames = tokio_stream::iter(sent).chain(tokio_stream::pending());
    let (writer, log) = Recording::new([Turn::Takes(Duration::ZERO), Turn::Panics]);
    let answers = answers(Limits::default(), writer, frames).await;
    assert_eq!(log.events(), ["write 1:10"]);
    let (last, before) = answers.split_last().expect("the write answers");
    assert_eq!(*last, Err(Code::Internal));
    assert!(
        before
            .iter()
            .all(|answer| matches!(answer, Ok(Ack::Credit(_))))
    );
}

#[tokio::test(start_paused = true)]
async fn the_staged_bound_refuses_a_frame_before_it_decodes_with_one_waiting() {
    // Four frames of nearly the least frame limit fill the sixteen mebibytes a write may stage
    // between flushes; a fifth, which would not decode, is refused for its size instead, while
    // the fourth waits for the writer.
    let limits = Limits {
        frame_bytes: MIN_FRAME_BYTES,
        ..Limits::default()
    };
    let rows = i64::try_from(MIN_FRAME_BYTES * 15 / 16 / 8).expect("fits");
    let near = batch(1, rows);
    let Some(Frame::Batch(ref inner)) = near.frame else {
        unreachable!("a batch frame");
    };
    let size = inner.data_header.len() + inner.data_body.len();
    let mut sent = vec![schema()];
    sent.extend(std::iter::repeat_n(near.clone(), 4));
    sent.extend([garbage(size), flush()]);
    let (writer, log) = Recording::new([Turn::Takes(Duration::from_millis(10)); 4]);
    let answers = answers(limits, writer, tokio_stream::iter(sent)).await;
    let written = format!("write 1:{rows}");
    assert_eq!(log.events(), vec![written; 4]);
    let Some(Ok(Ack::Error(error))) = answers.last() else {
        panic!("the write is refused: {answers:?}");
    };
    assert_eq!(error.code.as_deref(), Some("limit_exceeded"), "{error:?}");
    let limit = error.limit.as_ref().expect("a refusal names its limit");
    assert_eq!(limit.name, "staged bytes");
}

#[tokio::test(start_paused = true)]
async fn a_frame_its_bounds_refuse_fails_the_write_with_its_refusal_after_the_frames_before_it() {
    let limits = Limits::default();
    let good = vec![schema(), batch(1, 10), batch(1, 20)];
    let mut sent = good.clone();
    sent.push(batch(1, 100_000));
    // Bounds that hold the small frames and refuse the large one, beyond the wire bound.
    let bounds = Bounds {
        wire: 64 * 1024,
        ..bounds(&limits)
    };
    let (writer, log) = Recording::new([Turn::Takes(Duration::from_millis(50)); 2]);
    let body = Body::new(Outgoing::request(
        tokio_stream::iter(sent),
        limits.largest(),
    ));
    let answers = answered(limits, bounds, writer, body).await;
    assert_eq!(log.events(), ["write 1:10", "write 1:20"]);
    let (refused, credited) = answers.split_last().expect("the write answers");
    let credited: Vec<_> = credited.iter().cloned().map(Result::unwrap).collect();
    assert_eq!(credited, credits(&limits, &good));
    let Ok(Ack::Error(refused)) = refused else {
        panic!("the write fails with the frame's refusal: {refused:?}");
    };
    assert_eq!(refused.code.as_deref(), Some(crate::wire::TRANSPORT));
    assert!(refused.message.contains("too large"), "{refused:?}");
}

#[tokio::test(start_paused = true)]
async fn a_request_whose_body_fails_fails_the_write_after_the_frames_before_it() {
    let limits = Limits::default();
    let body = Cut {
        frames: VecDeque::from([schema(), batch(1, 10)]),
        status: Some(Status::unknown("the stream was reset")),
    };
    let (writer, log) = Recording::new([]);
    let answers = answered(limits, bounds(&limits), writer, Body::new(body)).await;
    assert_eq!(log.events(), ["write 1:10"]);
    let Some(Ok(Ack::Error(failed))) = answers.last() else {
        panic!("the write fails with its body's error: {answers:?}");
    };
    assert_eq!(failed.code.as_deref(), Some(crate::wire::TRANSPORT));
    assert!(failed.message.contains("reset"), "{failed:?}");
}

#[tokio::test(start_paused = true)]
async fn a_request_its_host_cancels_ends_the_write_after_the_frames_before_it() {
    let limits = Limits::default();
    let body = Cut {
        frames: VecDeque::from([schema(), batch(1, 10)]),
        status: Some(Status::cancelled("the host cancelled the write")),
    };
    let (writer, log) = Recording::new([]);
    let answers = answered(limits, bounds(&limits), writer, Body::new(body)).await;
    assert_eq!(log.events(), ["write 1:10"]);
    let answers: Vec<_> = answers.into_iter().map(Result::unwrap).collect();
    assert_eq!(answers, credits(&limits, &[schema(), batch(1, 10)]));
}
