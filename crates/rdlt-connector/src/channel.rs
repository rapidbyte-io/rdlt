//! The byte-budgeted channel a source pushes records through.
//!
//! Backpressure is the contract here, and it is measured in BYTES, not
//! messages: a slow consumer parks the producer once the bytes sitting
//! unconsumed in the channel reach the budget, so peak memory stays capped
//! no matter how wide the rows are. Awaiting a push IS the flow control —
//! a source never polls, sleeps, or counts.
//!
//! Two layers live here. The generic core ([`byte_channel`],
//! [`ByteSender`]/[`ByteReceiver`], [`ByteSized`], [`Permitted`]) is the one
//! implementation of the byte-budget rule for the whole tree — the SPI
//! states the backpressure contract, so the SPI owns the code; a second
//! copy elsewhere would let a fix to the rule apply to one path only. The
//! records layer ([`records_channel`], [`RecordsOut`]/[`RecordsIn`],
//! [`PushPayload`]) is that core specialized to what sources push.

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{Semaphore, mpsc};

use crate::RecordBatch;
use crate::core::Cursor;

/// Secondary message-count cap on the records channel.
///
/// The byte budget is the real backpressure, but it prices a zero-byte
/// message at nothing — without a message cap, checkpoints could queue
/// without limit while never touching the budget.
const RECORDS_MESSAGE_CAPACITY: usize = 64;

/// The host closed the channel (cancellation, or a failure downstream).
/// A source that receives this should return promptly — it is an
/// instruction to stop, not an error to escalate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("records channel closed by host")]
pub struct ChannelClosed;

/// Values that can state their own in-memory footprint.
///
/// The budget is spent in exactly these units. Under-reporting weakens
/// backpressure; over-reporting throttles a healthy producer. Report what
/// the value actually holds.
pub trait ByteSized {
    /// Bytes this value occupies in memory.
    fn byte_size(&self) -> usize;
}

/// A received value still holding the budget it was sent under.
///
/// The permit travels WITH the value and is released only when this
/// wrapper drops — so the budget describes bytes that are queued OR
/// received-but-unprocessed, not bytes ever sent. A consumer that holds
/// one deliberately keeps the producer parked; that coupling is the
/// feature, not a leak.
#[derive(Debug)]
pub struct Permitted<T> {
    value: T,
    /// The value's metered footprint, captured ONCE at send — receivers
    /// that report byte totals read this instead of re-walking the
    /// value (round-7: the read-side twin of the LoadItem carry).
    bytes: usize,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl<T> Permitted<T> {
    /// Take the value, releasing its budget.
    pub fn into_value(self) -> T {
        self.value
    }

    /// Borrow the value; the budget stays held.
    pub fn value(&self) -> &T {
        &self.value
    }

    /// Split value from permit (the metered footprint riding along),
    /// for a receiver that re-wraps the value in its own type while
    /// keeping the budget spent.
    pub(crate) fn into_parts(self) -> (T, usize, Option<tokio::sync::OwnedSemaphorePermit>) {
        (self.value, self.bytes, self.permit)
    }
}

/// Sending half of a byte-budgeted channel.
#[derive(Debug)]
pub struct ByteSender<T> {
    messages: mpsc::Sender<Permitted<T>>,
    budget: Arc<Semaphore>,
    budget_total: usize,
}

// Written out by hand: a derived Clone would demand `T: Clone`, and the
// SENDER is cloneable regardless of what it carries.
impl<T> Clone for ByteSender<T> {
    fn clone(&self) -> Self {
        Self {
            messages: self.messages.clone(),
            budget: Arc::clone(&self.budget),
            budget_total: self.budget_total,
        }
    }
}

/// Receiving half of a byte-budgeted channel.
#[derive(Debug)]
pub struct ByteReceiver<T> {
    messages: mpsc::Receiver<Permitted<T>>,
    budget: Arc<Semaphore>,
}

impl<T: ByteSized> ByteSender<T> {
    /// Send, parking until the value's bytes fit inside the budget.
    ///
    /// A value larger than the ENTIRE budget must still pass: its request
    /// is capped at the budget total, so it degrades to "wait for the
    /// whole budget, then go" instead of waiting on permits that cannot
    /// exist. The `u32` saturation below matters only when the budget
    /// itself exceeds `u32::MAX` (semaphore permits are `u32`), where a
    /// saturated request is still the same drain-everything request.
    pub async fn send(&self, value: T) -> Result<(), ChannelClosed> {
        let bytes = value.byte_size();
        let requested = bytes.min(self.budget_total).try_into().unwrap_or(u32::MAX);
        // Zero-byte values skip the semaphore entirely: acquiring zero
        // permits would also succeed, but skipping states the intent —
        // markers are not budgeted, and must pass even on a zero budget.
        let permit = if requested > 0 {
            Some(
                Arc::clone(&self.budget)
                    .acquire_many_owned(requested)
                    .await
                    .map_err(|_| ChannelClosed)?,
            )
        } else {
            None
        };
        self.messages
            .send(Permitted {
                value,
                bytes,
                permit,
            })
            .await
            .map_err(|_| ChannelClosed)
    }
}

impl<T> ByteReceiver<T> {
    /// The next value, or `None` once every sender dropped and the queue
    /// drained.
    pub async fn recv(&mut self) -> Option<Permitted<T>> {
        self.messages.recv().await
    }

    /// Tell the producer to stop.
    ///
    /// Closing the message queue alone is not enough: a producer parked on
    /// the byte budget is waiting on the SEMAPHORE, and nothing would ever
    /// wake it. Closing the semaphore too is what turns "stop" into an
    /// event the producer observes from either wait.
    pub fn close(&mut self) {
        self.messages.close();
        self.budget.close();
    }
}

/// A byte-budgeted channel: `byte_budget` caps unconsumed bytes in flight;
/// `message_capacity` is the secondary cap that keeps zero-byte markers
/// from queueing without limit.
pub fn byte_channel<T>(
    byte_budget: usize,
    message_capacity: usize,
) -> (ByteSender<T>, ByteReceiver<T>) {
    let budget = Arc::new(Semaphore::new(byte_budget));
    let (messages, receiver) = mpsc::channel(message_capacity);
    (
        ByteSender {
            messages,
            budget: Arc::clone(&budget),
            budget_total: byte_budget,
        },
        ByteReceiver {
            messages: receiver,
            budget,
        },
    )
}

// ---- the records specialization -------------------------------------------

/// The shapes a source can push.
///
/// Three shapes because the cheapest representation differs by source: an
/// HTTP body is already JSON bytes, a database read is already Arrow, and
/// a checkpoint carries no rows at all. One forced representation would
/// mean parsing and re-serializing data that arrived in the right form.
#[derive(Debug)]
pub enum PushPayload {
    /// Raw JSON bytes — one document, an array of documents, or NDJSON.
    /// The host's shredder parses these straight into Arrow builders;
    /// [`RecordsOut::rows`] also lands here (serialized once) so a host
    /// has exactly one JSON ingest path.
    RawJson(Bytes),
    /// A source-native Arrow batch; bypasses the shredder (schema check
    /// only).
    Arrow(RecordBatch),
    /// "Every row pushed so far is complete up to this cursor."
    Checkpoint(Cursor),
}

impl ByteSized for PushPayload {
    /// What the payload actually holds. A checkpoint holds no rows, costs
    /// nothing, and must never be gated by the budget — a marker that
    /// could not enqueue would stall the commit it announces.
    ///
    /// The Arrow arm meters the batch's buffer tree itself rather than
    /// calling `RecordBatch::get_array_memory_size()`, because that
    /// method sums each buffer's `capacity()` — the whole underlying
    /// ALLOCATION, not the slice the buffer views. Arrow's IPC reader
    /// allocates an entire message body as ONE buffer and hands every
    /// column a zero-copy slice of it, so under capacity-summing a
    /// decoded batch charges the body once PER BUFFER (measured ≈10-17×
    /// its footprint), and a wire source burns its budget that many
    /// times too fast. Summing slice LENGTHS instead makes a decoded
    /// batch meter ≈ the body it decodes from.
    ///
    /// THE OTHER DIRECTION IS THE ACCEPTED TRADE (D-042-4, judged by
    /// measurement): a builder-built batch's buffers carry
    /// capacity-doubling slack (`capacity ≈ 1.4x len` in the recorded
    /// arithmetic, up to 2x worst case), and len-summing deliberately
    /// does NOT count it — the budget is a THROUGHPUT WINDOW bound,
    /// not a resident-set accountant. The T4 review named this
    /// len-vs-resident caveat explicitly, and T11's recorded session
    /// judged it on the benches' own measurement: peak RSS fell a net
    /// −31 MB across the five remote twins with the builder slack
    /// presented in the arithmetic. Do not "fix" this back toward
    /// capacity-summing without a new RSS measurement that overturns
    /// that record.
    fn byte_size(&self) -> usize {
        match self {
            PushPayload::RawJson(bytes) => bytes.len(),
            PushPayload::Arrow(batch) => arrow_batch_footprint(batch),
            PushPayload::Checkpoint(_) => 0,
        }
    }
}

/// The bytes an Arrow batch holds: the summed lengths of the distinct
/// buffer slices reachable from its columns (values, offsets, nulls, and
/// nested child data, recursively).
///
/// Distinctness is by exact slice identity — start pointer plus length.
/// Two disjoint slices of one allocation both count, because they hold
/// different bytes; the identical slice reachable twice (one `Arc`'d
/// array as two columns, a shared dictionary) counts once. Slices that
/// overlap without coinciding double-charge the overlap — accepted: the
/// budget's failure directions are asymmetric (over-counting narrows a
/// healthy window, under-counting uncaps memory), so ties break toward
/// counting. arrow 58 exposes no slice-footprint API, hence the walk.
///
/// Public because this is the ONE byte meter for Arrow batches wherever
/// a budget, commit policy, or report counts them: batches decoded from
/// an IPC stream (a remote connector's wire) hold zero-copy slices of
/// one message-body allocation, and `RecordBatch::get_array_memory_size`
/// capacity-sums that body once per buffer (measured ≈10-17x the true
/// footprint). A host metering batches by any other rule inflates its
/// accounting by exactly that factor.
pub fn arrow_batch_footprint(batch: &RecordBatch) -> usize {
    let mut seen = std::collections::HashSet::new();
    batch
        .columns()
        .iter()
        .map(|column| data_footprint(&column.to_data(), &mut seen))
        .sum()
}

/// One node of the walk: this array's own buffers (each trimmed to the
/// byte range the node's `offset`/`len` actually VIEW — round-7 fix: a
/// `RecordBatch::slice` chunk used to charge its parent's whole buffer,
/// so n chunks metered ~n× the parent), then its children.
///
/// The viewed range is computed per layout where the arithmetic is
/// cheap and exact — fixed-width values, boolean and validity bitmaps,
/// offset buffers, and variable-width data through its offsets window —
/// and falls back to the buffer's full length for the exotic layouts
/// (unions, dictionaries, run-ends, views), which errs only in the
/// OVER-count direction, the budget's safe side. A sliced List's child
/// likewise meters its full extent (trimming it would need the offsets
/// window applied to the child) — over-count again, accepted. Dedup
/// keys on the exact viewed slice (start pointer + viewed length), so
/// two chunks re-viewing one range still count it once per batch walk.
fn data_footprint(
    data: &arrow_data::ArrayData,
    seen: &mut std::collections::HashSet<(usize, usize)>,
) -> usize {
    use arrow_schema::DataType;

    let mut count = |start: usize, viewed: usize| -> usize {
        if viewed > 0 && seen.insert((start, viewed)) {
            viewed
        } else {
            0
        }
    };
    // The window `[start_byte, start_byte + viewed)` of `buffer` this
    // node views, clamped into the buffer.
    let mut count_window = |buffer: &arrow_buffer::Buffer, start_byte: usize, viewed: usize| {
        let start_byte = start_byte.min(buffer.len());
        let viewed = viewed.min(buffer.len() - start_byte);
        count(buffer.as_ptr() as usize + start_byte, viewed)
    };

    let (offset, len) = (data.offset(), data.len());
    // A bit window's byte span: from `first_bit`, `bits` wide.
    let bit_window = |first_bit: usize, bits: usize| -> (usize, usize) {
        let start = first_bit / 8;
        let end = (first_bit + bits).div_ceil(8);
        (start, end - start)
    };
    // The i32/i64 offsets window `[offset ..= offset + len]`, and the
    // data-byte range those offsets span. Arrow permits an EMPTY
    // offsets buffer for a zero-length variable-width array (arrow-data
    // 58's own validation: "An empty list-like array can have 0
    // offsets" — a shape foreign IPC writers like pyarrow produce), so
    // the window reads are guarded (round-10 fix: `values[offset]` on
    // that legal shape panicked inside the byte meter): no offsets,
    // nothing viewed.
    let offsets_i32 = |buffer: &arrow_buffer::Buffer| -> (usize, usize, usize, usize) {
        let values: &[i32] = buffer.typed_data();
        let (Some(&first), Some(&last)) = (values.get(offset), values.get(offset + len)) else {
            return (0, 0, 0, 0);
        };
        (
            offset * 4,
            (len + 1) * 4,
            first as usize,
            (last - first) as usize,
        )
    };
    let offsets_i64 = |buffer: &arrow_buffer::Buffer| -> (usize, usize, usize, usize) {
        let values: &[i64] = buffer.typed_data();
        let (Some(&first), Some(&last)) = (values.get(offset), values.get(offset + len)) else {
            return (0, 0, 0, 0);
        };
        (
            offset * 8,
            (len + 1) * 8,
            first as usize,
            (last - first) as usize,
        )
    };

    let buffers = data.buffers();
    let mut total = 0;
    match data.data_type() {
        DataType::Boolean => {
            let (start, viewed) = bit_window(offset, len);
            total += count_window(&buffers[0], start, viewed);
        }
        DataType::Utf8 | DataType::Binary => {
            let (off_start, off_len, data_start, data_len) = offsets_i32(&buffers[0]);
            total += count_window(&buffers[0], off_start, off_len);
            total += count_window(&buffers[1], data_start, data_len);
        }
        DataType::LargeUtf8 | DataType::LargeBinary => {
            let (off_start, off_len, data_start, data_len) = offsets_i64(&buffers[0]);
            total += count_window(&buffers[0], off_start, off_len);
            total += count_window(&buffers[1], data_start, data_len);
        }
        DataType::List(_) | DataType::Map(_, _) => {
            let (off_start, off_len, _, _) = offsets_i32(&buffers[0]);
            total += count_window(&buffers[0], off_start, off_len);
        }
        DataType::LargeList(_) => {
            let (off_start, off_len, _, _) = offsets_i64(&buffers[0]);
            total += count_window(&buffers[0], off_start, off_len);
        }
        DataType::FixedSizeBinary(width) => {
            let width = *width as usize;
            total += count_window(&buffers[0], offset * width, len * width);
        }
        // Structs and fixed-size lists carry no buffers of their own;
        // their children recurse below.
        DataType::Struct(_) | DataType::FixedSizeList(_, _) | DataType::Null => {}
        other => match other.primitive_width() {
            // Fixed-width values: exactly the viewed cells.
            Some(width) => {
                total += count_window(&buffers[0], offset * width, len * width);
            }
            // The documented fallback (unions, dictionaries, run-ends,
            // views): full buffer lengths — over-counts a sliced view,
            // never under.
            None => {
                for buffer in buffers {
                    total += count_window(buffer, 0, buffer.len());
                }
            }
        },
    }
    if let Some(nulls) = data.nulls() {
        let (start, viewed) = bit_window(nulls.offset(), nulls.len());
        total += count_window(nulls.buffer(), start, viewed);
    }
    total
        + data
            .child_data()
            .iter()
            .map(|child| data_footprint(child, seen))
            .sum::<usize>()
}

/// What arrived from a source, still holding its byte-budget permit; the
/// budget releases when this drops.
#[derive(Debug)]
pub struct SourcePush {
    /// What was pushed.
    pub payload: PushPayload,
    /// The payload's metered footprint — the number the byte budget
    /// charged, computed once at push. A host reporting read totals
    /// reads THIS rather than re-walking the payload.
    pub bytes: usize,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

/// The push handle a source holds for one read.
#[derive(Debug)]
pub struct RecordsOut {
    channel: ByteSender<PushPayload>,
}

impl RecordsOut {
    /// The fast path: JSON bytes already in hand (an HTTP body, a file
    /// segment) go through untouched.
    pub async fn raw_json(&mut self, bytes: Bytes) -> Result<(), ChannelClosed> {
        self.channel.send(PushPayload::RawJson(bytes)).await
    }

    /// Convenience for programmatically built rows: serialized here, once,
    /// to NDJSON, so hosts still see a single JSON ingest path.
    pub async fn rows(
        &mut self,
        rows: impl IntoIterator<Item = serde_json::Value>,
    ) -> Result<(), ChannelClosed> {
        let mut encoded = Vec::new();
        for row in rows {
            // Writing a `serde_json::Value` to a `Vec` cannot fail: the
            // writer is infallible and every `Value` is valid JSON
            // (`Number` refuses non-finite floats at construction).
            // Asserting that is honest; mapping the impossible failure to
            // `ChannelClosed` would tell the source the host hung up.
            serde_json::to_writer(&mut encoded, &row)
                .expect("a serde_json::Value serializes to a Vec infallibly");
            encoded.push(b'\n');
        }
        if encoded.is_empty() {
            return Ok(());
        }
        self.channel
            .send(PushPayload::RawJson(encoded.into()))
            .await
    }

    /// Push a source-native Arrow batch — the cheapest path for a source
    /// that already holds columnar data.
    pub async fn arrow(&mut self, batch: RecordBatch) -> Result<(), ChannelClosed> {
        self.channel.send(PushPayload::Arrow(batch)).await
    }

    /// Declare every row pushed so far complete up to `cursor`.
    pub async fn checkpoint(&mut self, cursor: Cursor) -> Result<(), ChannelClosed> {
        self.channel.send(PushPayload::Checkpoint(cursor)).await
    }
}

/// The receiving half a host holds (the engine, or a conformance harness).
#[derive(Debug)]
pub struct RecordsIn {
    channel: ByteReceiver<PushPayload>,
}

impl RecordsIn {
    /// The next push, or `None` when the source finished (dropped its
    /// [`RecordsOut`]). The permit rides inside the returned
    /// [`SourcePush`], so the budget stays spent until the host drops it.
    pub async fn recv(&mut self) -> Option<SourcePush> {
        self.channel.recv().await.map(|permitted| {
            let (payload, bytes, permit) = permitted.into_parts();
            SourcePush {
                payload,
                bytes,
                _permit: permit,
            }
        })
    }

    /// Tell the source to stop at its next push.
    pub fn close(&mut self) {
        self.channel.close();
    }
}

/// The push channel between a host and one `Source::read` call.
/// `byte_budget` caps in-flight bytes; the message count is secondary.
pub fn records_channel(byte_budget: usize) -> (RecordsOut, RecordsIn) {
    let (sender, receiver) = byte_channel(byte_budget, RECORDS_MESSAGE_CAPACITY);
    (
        RecordsOut { channel: sender },
        RecordsIn { channel: receiver },
    )
}

#[cfg(test)]
mod budget_tests {
    //! The budget counts BYTES HELD, at its exact boundary.
    use super::*;

    /// A payload whose only property is the footprint it claims.
    #[derive(Debug)]
    struct Weighted(usize);
    impl ByteSized for Weighted {
        fn byte_size(&self) -> usize {
            self.0
        }
    }

    /// Roomy enough that the message cap can never fire first — every test
    /// here is about the BYTE budget, and a message-count stall would be a
    /// different mechanism passing for it.
    const ROOMY: usize = 256;

    #[tokio::test]
    async fn the_budget_counts_bytes_not_messages() {
        let (sender, mut receiver) = byte_channel::<Weighted>(100, ROOMY);
        // Fifty small values pass though they exceed any small item count…
        for _ in 0..50 {
            sender.send(Weighted(2)).await.unwrap();
        }
        for _ in 0..50 {
            receiver.recv().await.unwrap();
        }
        // …while the second of two large values parks until the first is
        // received AND dropped.
        sender.send(Weighted(80)).await.unwrap();
        let parked = sender.send(Weighted(80));
        tokio::pin!(parked);
        tokio::select! {
            _ = &mut parked => panic!("the byte budget did not park the second large send"),
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        // Dropped, not bound: receiving alone does not release the budget.
        drop(receiver.recv().await.unwrap());
        parked.await.unwrap();
    }

    #[tokio::test]
    async fn a_send_at_exactly_the_budget_passes_and_the_next_waits() {
        let (sender, mut receiver) = byte_channel::<Weighted>(100, ROOMY);
        sender
            .send(Weighted(100))
            .await
            .expect("exactly the budget");
        let next = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            sender.send(Weighted(1)),
        )
        .await;
        assert!(next.is_err(), "a spent budget must park the next send");
        drop(receiver.recv().await.expect("the queued value"));
        sender.send(Weighted(1)).await.expect("budget released");
    }

    #[tokio::test]
    async fn a_value_larger_than_the_whole_budget_still_passes() {
        // Degrades to drain-the-budget rather than waiting for permits
        // that cannot exist.
        let (sender, mut receiver) = byte_channel::<Weighted>(16, ROOMY);
        sender.send(Weighted(1_000_000)).await.unwrap();
        assert_eq!(receiver.recv().await.unwrap().value().0, 1_000_000);
    }

    #[tokio::test]
    async fn receiving_without_dropping_keeps_the_budget_spent() {
        // The distinction `Permitted` exists for: receiving ≠ releasing.
        let (sender, mut receiver) = byte_channel::<Weighted>(100, ROOMY);
        sender
            .send(Weighted(100))
            .await
            .expect("exactly the budget");
        let held = receiver.recv().await.expect("the queued value");
        let parked = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            sender.send(Weighted(1)),
        )
        .await;
        assert!(
            parked.is_err(),
            "a received-but-held value still occupies the budget"
        );
        assert_eq!(held.into_value().0, 100);
        sender
            .send(Weighted(1))
            .await
            .expect("released by into_value");
    }
}

#[cfg(test)]
mod byte_size_tests {
    //! The Arrow arm meters what a batch actually HOLDS. The hard case is
    //! a batch decoded from an IPC stream: every column is a zero-copy
    //! slice of the one message-body allocation, so any metric that sums
    //! parent-allocation capacities charges that body once PER BUFFER.
    //!
    //! THE FIXTURE HERE IS THIS CRATE'S OWN (round-7 truthfulness fix:
    //! an earlier comment claimed it mirrored
    //! `rdlt_testkit::fixtures::ipc_fixture`, which had already
    //! drifted). testkit depends on THIS crate, so these unit tests
    //! cannot import the shared fixture; this one is shaped for the
    //! channel pins alone — wider (more buffers) so capacity-summing
    //! lands further outside every bound — and the two need not agree.
    use std::sync::Arc;

    use arrow::ipc::reader::StreamReader;
    use arrow::ipc::writer::StreamWriter;
    use arrow_array::{ArrayRef, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    const ROWS: usize = 4096;
    const INT_COLUMNS: usize = 8;
    const STRING_WIDTH: usize = 12;

    /// Exactly the bytes the rows require: 8 per int64 cell, plus the
    /// string payload. Offsets, nulls and padding sit on top of this, so
    /// it is a hard LOWER bound on any honest footprint.
    const ROW_PAYLOAD: usize = ROWS * INT_COLUMNS * 8 + ROWS * STRING_WIDTH;

    /// Ten buffers (eight int64 values, string offsets, string values) so
    /// a meter that charges the whole body allocation per buffer lands
    /// near 10x — far outside every bound asserted here.
    fn built_batch() -> RecordBatch {
        let mut fields: Vec<Field> = (0..INT_COLUMNS)
            .map(|i| Field::new(format!("n{i}"), DataType::Int64, false))
            .collect();
        fields.push(Field::new("s", DataType::Utf8, false));
        let mut columns: Vec<ArrayRef> = (0..INT_COLUMNS)
            .map(|i| {
                Arc::new(Int64Array::from_iter_values(
                    (0..ROWS as i64).map(|row| row + i as i64),
                )) as ArrayRef
            })
            .collect();
        columns.push(Arc::new(StringArray::from_iter_values(
            (0..ROWS).map(|row| format!("row-{row:07}!")),
        )));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("a well-formed batch")
    }

    /// One IPC round trip: the stream bytes out, the first decoded batch
    /// back. The stream length is the honest comparator — it carries the
    /// whole message body the decoded batch's buffers are slices of.
    fn ipc_round_trip(batch: &RecordBatch) -> (usize, RecordBatch) {
        let mut stream = Vec::new();
        let mut writer =
            StreamWriter::try_new(&mut stream, &batch.schema()).expect("stream writer");
        writer.write(batch).expect("write batch");
        writer.finish().expect("finish stream");
        drop(writer);
        let stream_len = stream.len();
        let mut reader =
            StreamReader::try_new(std::io::Cursor::new(stream), None).expect("stream reader");
        let decoded = reader
            .next()
            .expect("one batch in the stream")
            .expect("decodes");
        (stream_len, decoded)
    }

    #[test]
    fn byte_size_of_an_ipc_decoded_batch_is_near_the_body_it_decodes_from() {
        let (stream_len, decoded) = ipc_round_trip(&built_batch());
        let metered = PushPayload::Arrow(decoded).byte_size();
        assert!(
            metered >= ROW_PAYLOAD,
            "an honest footprint cannot undercut the raw row payload: \
             metered {metered}, payload {ROW_PAYLOAD}"
        );
        assert!(
            metered <= 2 * stream_len,
            "a decoded batch holds slices of ONE body allocation; metering \
             {metered} against a {stream_len}-byte stream means the body \
             was charged once per buffer, not once"
        );
    }

    /// THE SLICE PIN (round-7 fix): a batch cut into n chunks meters
    /// ≈ the parent ONCE across the chunks — each chunk charges only
    /// the byte range it views, so the sum tracks the parent (small
    /// per-chunk overlap at offsets/validity boundaries allowed), and
    /// never ~n× the parent as full-buffer accounting produced.
    #[test]
    fn slicing_a_batch_meters_the_parent_once_not_once_per_chunk() {
        let batch = built_batch();
        let whole = PushPayload::Arrow(batch.clone()).byte_size();
        let chunks = 4;
        let rows_per_chunk = ROWS / chunks;
        let sum: usize = (0..chunks)
            .map(|i| {
                PushPayload::Arrow(batch.slice(i * rows_per_chunk, rows_per_chunk)).byte_size()
            })
            .sum();
        assert!(
            sum >= whole / 2,
            "the chunks together must still account the parent's bytes: sum {sum}, whole {whole}"
        );
        assert!(
            sum <= whole + whole / 4,
            "n chunks must meter ≈ the parent once, never ~n×: sum {sum}, whole {whole} \
             (full-buffer accounting would land near {})",
            chunks * whole
        );
    }

    #[test]
    fn byte_size_of_a_builder_built_batch_stays_between_payload_and_double() {
        let metered = PushPayload::Arrow(built_batch()).byte_size();
        assert!(
            metered >= ROW_PAYLOAD,
            "the fix must not collapse an ordinary batch below its rows: \
             metered {metered}, payload {ROW_PAYLOAD}"
        );
        assert!(
            metered <= 2 * ROW_PAYLOAD,
            "a batch built from exact-length buffers meters near its \
             payload: metered {metered}, payload {ROW_PAYLOAD}"
        );
    }

    /// THE EMPTY-OFFSETS PIN (round-10 fix): Arrow permits a
    /// zero-length variable-width array to carry an EMPTY offsets
    /// buffer — arrow-data 58's own validation allows 0 offsets
    /// ("An empty list-like array can have 0 offsets") — and the
    /// unguarded window read (`values[offset]`) panicked on it inside
    /// the byte meter. The walk is driven on the RAW `ArrayData` here,
    /// deliberately: arrow-array 58's typed wrappers happen to
    /// normalize the empty buffer to a single `0` on construction
    /// (measured — `make_array` + `to_data` yields a 4-byte offsets
    /// buffer), so a batch-level fixture cannot carry the shape today,
    /// but the meter walks `child_data` trees and validated foreign
    /// constructions where nothing promises that normalization.
    #[test]
    fn an_empty_offsets_buffer_meters_zero_instead_of_panicking() {
        let data = arrow_data::ArrayData::try_new(
            DataType::Utf8,
            0,
            None,
            0,
            vec![
                arrow_buffer::Buffer::from_vec(Vec::<i32>::new()),
                arrow_buffer::Buffer::from_vec(Vec::<u8>::new()),
            ],
            vec![],
        )
        .expect("arrow validates an empty offsets buffer as legal for an empty array");
        assert_eq!(data.buffers()[0].len(), 0, "the raw shape under test");
        let mut seen = std::collections::HashSet::new();
        assert_eq!(
            data_footprint(&data, &mut seen),
            0,
            "no offsets, nothing viewed — the legal empty shape meters zero"
        );
    }

    #[test]
    fn byte_size_counts_a_shared_column_once() {
        // Two columns holding the SAME Arc'd array view the same bytes;
        // charging the budget twice for one allocation would over-throttle
        // exactly the way capacity-summing did.
        let column: ArrayRef = Arc::new(Int64Array::from_iter_values(0..ROWS as i64));
        let alone = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)])),
            vec![Arc::clone(&column)],
        )
        .expect("one-column batch");
        let shared = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Int64, false),
            ])),
            vec![Arc::clone(&column), column],
        )
        .expect("shared-column batch");
        assert_eq!(
            PushPayload::Arrow(shared).byte_size(),
            PushPayload::Arrow(alone).byte_size(),
            "the second column re-views bytes already counted"
        );
    }
}

#[cfg(test)]
mod records_tests {
    //! The records layer over the core: budget boundary, checkpoint
    //! exemption, and close-as-cancellation.
    use super::*;
    use std::time::Duration;

    /// Every wait in these tests is bounded: the failure mode under test
    /// is a HANG, and an unbounded assertion would report it as a timeout
    /// of the whole suite instead of a named failure.
    const BOUND: Duration = Duration::from_secs(5);

    #[tokio::test]
    async fn a_push_at_the_budget_passes_and_the_next_waits() {
        let (mut out, mut input) = records_channel(100);
        out.raw_json(Bytes::from(vec![b'x'; 100]))
            .await
            .expect("exactly the budget");
        let next = tokio::time::timeout(
            Duration::from_millis(50),
            out.raw_json(Bytes::from_static(b"y")),
        )
        .await;
        assert!(next.is_err(), "a spent budget must park the next push");
        drop(input.recv().await.expect("the queued push"));
        out.raw_json(Bytes::from_static(b"y"))
            .await
            .expect("budget released");
    }

    #[tokio::test]
    async fn a_checkpoint_passes_even_on_a_zero_budget() {
        // A checkpoint that could not enqueue would stall the commit it
        // announces — markers are never budgeted.
        let (mut out, mut input) = records_channel(0);
        tokio::time::timeout(
            BOUND,
            out.checkpoint(Cursor::new(serde_json::json!("watermark"))),
        )
        .await
        .expect("a zero-byte marker must not wait on the budget")
        .expect("channel open");
        assert!(input.recv().await.is_some(), "the marker arrives");
    }

    #[tokio::test]
    async fn close_wakes_a_push_parked_on_the_budget() {
        // A producer parked on the semaphore learns about the close too;
        // otherwise "stop" would only reach sources between pushes.
        let (mut out, mut input) = records_channel(8);
        out.raw_json(Bytes::from_static(b"12345678"))
            .await
            .expect("first push fits");
        let parked =
            tokio::spawn(async move { out.raw_json(Bytes::from_static(b"12345678")).await });
        tokio::time::sleep(Duration::from_millis(50)).await; // let it park
        input.close();
        let result = tokio::time::timeout(BOUND, parked)
            .await
            .expect("close must wake the parked producer")
            .expect("task joins");
        assert_eq!(result, Err(ChannelClosed), "the woken producer is told why");
    }

    #[tokio::test]
    async fn close_refuses_further_pushes() {
        let (mut out, mut input) = records_channel(1024);
        input.close();
        let refused = tokio::time::timeout(BOUND, out.raw_json(Bytes::from_static(b"{\"row\":1}")))
            .await
            .expect("a closed channel answers promptly");
        assert_eq!(refused, Err(ChannelClosed));
    }

    #[tokio::test]
    async fn rows_serialize_once_to_ndjson_and_an_empty_iterator_is_a_no_op() {
        let (mut out, mut input) = records_channel(1024);
        out.rows([serde_json::json!({"id": 1}), serde_json::json!({"id": 2})])
            .await
            .expect("rows push");
        let push = input.recv().await.expect("one message for the batch");
        match push.payload {
            PushPayload::RawJson(bytes) => {
                assert_eq!(&bytes[..], b"{\"id\":1}\n{\"id\":2}\n");
            }
            other => panic!("rows land as RawJson, got {other:?}"),
        }
        // No rows, no message: the host must not see an empty push.
        out.rows([]).await.expect("empty push is a no-op");
        drop(out);
        assert!(input.recv().await.is_none(), "nothing further arrived");
    }
}
