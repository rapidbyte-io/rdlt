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

/// Depth cap on every recursive walk over connector-supplied nesting —
/// this crate's byte meter, the engine's schema mapping and batch
/// lowering, and the engine's JSONL ingest parser all share the ONE
/// constant, so the front door and every walk behind it agree on one
/// bound.
///
/// The threat: nesting depth is entirely CONNECTOR-controlled (a
/// structured stream declares its own schema, and IPC-built child data
/// skips arrow's validator), and a recursive walk without a cap turns a
/// deeply nested declaration into a stack overflow — which is an ABORT
/// of the host process, not a catchable panic, so no task containment
/// absorbs it. 64 is orders of magnitude beyond any real schema, so
/// reaching the cap is diagnostic of a hostile or broken connector,
/// never of data.
pub const MAX_ARROW_DEPTH: usize = 64;

/// The largest row count one pushed batch may carry.
///
/// Row count is an independent memory dimension from encoded bytes: a
/// boolean column can describe eight rows per byte, while the engine must
/// still build offsets, lineage, and the constant load-id value for every
/// row. Capping rows at the shared ingress vocabulary keeps a small valid
/// frame from amplifying into multi-gigabyte allocations downstream.
pub const MAX_RECORD_BATCH_ROWS: usize = 1_000_000;

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

/// The i32/i64 offsets window `[offset ..= offset + len]` of a
/// variable-width array's offsets buffer, and the data-byte range
/// those offsets span — decoded from the buffer's RAW BYTES, never
/// `typed_data` (round-13 fix: `typed_data` ASSERTS an exact
/// length-multiple and alignment, so a merely-large-enough offsets
/// buffer — 13 bytes for a 2-element array, legal by Arrow's minimum-
/// size rule and reachable through IPC-built child data, which skips
/// the validator — panicked the meter inside `send`). Only the exact
/// window the array views is read: the first and last offsets come out
/// of it as native-endian fixed-width chunks (what `typed_data` read,
/// minus its asserts), and a buffer too short for even the window —
/// the legal empty-offsets shape included (round-10) — falls back to
/// charging `usize::MAX`, which [`data_footprint`]'s window counter
/// clamps to each buffer's real length: over-count, the budget's safe
/// side, NEVER a panic (the meter's contract; the subtraction
/// saturates for the same reason).
fn offsets_window(
    buffer: &arrow_buffer::Buffer,
    offset: usize,
    len: usize,
    width: usize,
    decode: fn(&[u8]) -> usize,
) -> (usize, usize, usize, usize) {
    // Saturating (047 L2, symmetric with the round-13 hardening below):
    // IPC-skewed `offset`/`len` can overflow the window arithmetic, and
    // an unchecked multiply panicked DEBUG builds inside `send` — the
    // saturated bound lands in the same `None` over-count arm release
    // builds only ever reached by wrapping luck.
    let start = offset.saturating_mul(width);
    let end = offset
        .saturating_add(len)
        .saturating_add(1)
        .saturating_mul(width);
    match buffer.as_slice().get(start..end) {
        // A successful get always holds >= one `width` chunk
        // (end - start = (len + 1) * width).
        Some(window) => (
            start,
            (len + 1) * width,
            decode(&window[..width]),
            decode(&window[window.len() - width..]).saturating_sub(decode(&window[..width])),
        ),
        None => (0, usize::MAX, 0, usize::MAX),
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
    // Saturating: a column nested past [`MAX_ARROW_DEPTH`] charges
    // `usize::MAX`, and the sum must carry that over-count through
    // rather than overflow.
    batch.columns().iter().fold(0usize, |total, column| {
        total.saturating_add(data_footprint(&column.to_data(), &mut seen, 0))
    })
}

/// A bit window's byte span: from `first_bit`, `bits` wide — for boolean
/// values and validity bitmaps, whose `offset`/`len` come from IPC-decoded
/// data that skips the validator.
///
/// Checked (047 round 2, the L2 siblings): unchecked, a skewed window
/// panicked debug builds inside `send`, and in release it WRAPPED — a
/// wrapped-small span can land inside the buffer and UNDER-count, the
/// budget's unsafe direction. Overflow falls back to charge-everything,
/// which the caller clamps to the buffer's real length: over-count, the
/// budget's safe side, same posture as [`offsets_window`]'s fallback.
fn bit_window(first_bit: usize, bits: usize) -> (usize, usize) {
    match first_bit.checked_add(bits) {
        Some(end_bits) => {
            let start = first_bit / 8;
            (start, end_bits.div_ceil(8) - start)
        }
        None => (0, usize::MAX),
    }
}

/// The byte span `len` fixed-width cells view from cell `offset` — the
/// FixedSizeBinary and primitive-width arms' arithmetic. Checked for the
/// same reason and with the same fallback as [`bit_window`].
fn cell_window(offset: usize, len: usize, width: usize) -> (usize, usize) {
    match (offset.checked_mul(width), len.checked_mul(width)) {
        (Some(start), Some(viewed)) => (start, viewed),
        _ => (0, usize::MAX),
    }
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
    depth: usize,
) -> usize {
    use arrow_schema::DataType;

    // THE DEPTH CAP (047 M1): nesting is connector-controlled and a stack
    // overflow is an abort. The meter must never error the send path, so
    // past the cap it stops and charges `usize::MAX` — the over-count
    // direction, the budget's safe side, exactly like the short-buffer
    // fallback above: `send` clamps the request to the whole budget, so a
    // hostile batch degrades to drain-the-budget-and-go, never to a
    // deeper frame.
    if depth > MAX_ARROW_DEPTH {
        return usize::MAX;
    }

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
    let offsets_i32 = |buffer: &arrow_buffer::Buffer| -> (usize, usize, usize, usize) {
        offsets_window(buffer, offset, len, 4, |chunk| {
            i32::from_ne_bytes(chunk.try_into().expect("a 4-byte chunk")) as usize
        })
    };
    let offsets_i64 = |buffer: &arrow_buffer::Buffer| -> (usize, usize, usize, usize) {
        offsets_window(buffer, offset, len, 8, |chunk| {
            i64::from_ne_bytes(chunk.try_into().expect("an 8-byte chunk")) as usize
        })
    };
    let buffers = data.buffers();
    let mut total = 0;
    match data.data_type() {
        DataType::Boolean => {
            let (start, viewed) = bit_window(offset, len);
            total += count_window(&buffers[0], start, viewed);
        }
        DataType::Utf8 | DataType::Binary => {
            let (off_start, off_len, _, _) = offsets_i32(&buffers[0]);
            total += count_window(&buffers[0], off_start, off_len);
            // An IPC-decoded values buffer may legally be larger than the
            // offsets reference. The whole allocation remains resident, so
            // charging only `last_offset - first_offset` lets a valid frame
            // retain almost 64 MiB while consuming almost no budget. Charge
            // the complete values buffer; slices can consequently over-count
            // a shared allocation, which is the budget's safe direction.
            total += count_window(&buffers[1], 0, buffers[1].len());
        }
        DataType::LargeUtf8 | DataType::LargeBinary => {
            let (off_start, off_len, _, _) = offsets_i64(&buffers[0]);
            total += count_window(&buffers[0], off_start, off_len);
            total += count_window(&buffers[1], 0, buffers[1].len());
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
            let (start, viewed) = cell_window(offset, len, *width as usize);
            total += count_window(&buffers[0], start, viewed);
        }
        // Structs and fixed-size lists carry no buffers of their own;
        // their children recurse below.
        DataType::Struct(_) | DataType::FixedSizeList(_, _) | DataType::Null => {}
        other => match other.primitive_width() {
            // Fixed-width values: exactly the viewed cells.
            Some(width) => {
                let (start, viewed) = cell_window(offset, len, width);
                total += count_window(&buffers[0], start, viewed);
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
    // Saturating for the same reason as the batch-level fold: a child past
    // the depth cap charges `usize::MAX`.
    data.child_data().iter().fold(total, |total, child| {
        total.saturating_add(data_footprint(child, seen, depth + 1))
    })
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

    /// Fixed-width slices keep their precise window accounting. Variable-width
    /// values deliberately charge their full backing allocation, so this
    /// fixture (which contains one string column) may over-count across chunks;
    /// the important invariant is that it never under-counts the parent.
    #[test]
    fn slicing_a_batch_never_undercounts_the_parent() {
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
            sum <= chunks * whole,
            "no chunk may charge more than the complete parent: sum {sum}, whole {whole}"
        );
    }

    /// 2H3: Arrow permits a values buffer to contain bytes no offset
    /// references. Those bytes still remain resident in the decoded batch and
    /// therefore belong to the byte budget in full.
    #[test]
    fn an_unreferenced_variable_width_buffer_is_charged_in_full() {
        let retained = 1024 * 1024;
        let data = arrow_data::ArrayData::try_new(
            DataType::Utf8,
            1,
            None,
            0,
            vec![
                arrow_buffer::Buffer::from_vec(vec![0i32, 0]),
                arrow_buffer::Buffer::from_vec(vec![b'x'; retained]),
            ],
            vec![],
        )
        .expect("zero offsets may legally leave the values buffer unreferenced");
        let array = arrow_array::make_array(data);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)])),
            vec![array],
        )
        .expect("matching batch");

        let metered = arrow_batch_footprint(&batch);
        assert!(
            metered >= retained,
            "all {retained} resident values bytes must be charged, got {metered}"
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
            data_footprint(&data, &mut seen, 0),
            0,
            "no offsets, nothing viewed — the legal empty shape meters zero"
        );
    }

    /// THE OVERSIZED-OFFSETS PIN (round-13 fix): Arrow sizes an
    /// offsets buffer by MINIMUM (>= (len+1)*4), so a 13-byte buffer
    /// for a 2-element array can reach the meter through IPC-built
    /// child data (which skips the validator — arrow's own `try_new`
    /// validation ASSERTS on this shape even before the meter would,
    /// measured, which is why this pin drives the window fn directly).
    /// `typed_data` panicked on it; the window read must answer the
    /// true footprint, never panic.
    #[test]
    fn an_oversized_offsets_buffer_meters_without_panicking() {
        let decode_i32 =
            |chunk: &[u8]| i32::from_ne_bytes(chunk.try_into().expect("a 4-byte chunk")) as usize;
        // [0, 1, 3] as native-endian i32 plus one trailing byte:
        // 13 bytes, 4-aligned via the slice of a Vec<i32> allocation.
        let backing = arrow_buffer::Buffer::from_vec(vec![0i32, 1, 3, 0]);
        let oversized = backing.slice_with_length(0, 13);
        assert_eq!(
            offsets_window(&oversized, 0, 2, 4, decode_i32),
            (0, 12, 0, 3),
            "the exact window: 3 offsets x 4 bytes viewed, data bytes 0..3 spanned — \
             the trailing byte beyond the window is never read, and nothing panics"
        );

        // The round-10 legal empty-offsets shape stays a non-panicking
        // fallback: too short for even one offset, charge-everything
        // (the caller clamps MAX to the real buffer lengths).
        let empty = arrow_buffer::Buffer::from_vec(Vec::<i32>::new());
        assert_eq!(
            offsets_window(&empty, 0, 0, 4, decode_i32),
            (0, usize::MAX, 0, usize::MAX)
        );
    }

    /// 047 M1: the meter walks connector-controlled nesting, and a stack
    /// overflow is an ABORT no task containment can absorb. Past
    /// [`MAX_ARROW_DEPTH`] the walk stops and charges `usize::MAX` — the
    /// over-count direction, the budget's safe side (`send` clamps it to
    /// the whole budget: drain-and-go, maximal throttle, never an error
    /// on the send path and never a deeper frame).
    #[test]
    fn a_batch_nested_past_the_depth_cap_meters_max_instead_of_recursing() {
        use arrow_array::StructArray;

        let deep = |levels: usize| -> RecordBatch {
            use arrow_array::Array as _;
            let mut array: ArrayRef = Arc::new(Int64Array::from(vec![1i64]));
            let mut field = Field::new("leaf", DataType::Int64, true);
            for level in 0..levels {
                let wrapped = StructArray::new(vec![field.clone()].into(), vec![array], None);
                field = Field::new(format!("n{level}"), wrapped.data_type().clone(), true);
                array = Arc::new(wrapped);
            }
            RecordBatch::try_new(Arc::new(Schema::new(vec![field.clone()])), vec![array])
                .expect("nested batch")
        };

        assert_eq!(
            PushPayload::Arrow(deep(100)).byte_size(),
            usize::MAX,
            "past the cap the meter must stop and over-count, never recurse on"
        );
        let shallow = PushPayload::Arrow(deep(10)).byte_size();
        assert!(
            (1..usize::MAX).contains(&shallow),
            "an ordinarily nested batch still meters its real bytes: {shallow}"
        );
    }

    /// 047 round 2, the L2 siblings: the bit-window and fixed-width-cell
    /// arithmetic over IPC-skewed `offset`/`len` must fall into the same
    /// charge-everything fallback as the offsets window. Unchecked, these
    /// panicked debug builds inside `send` — and in RELEASE they WRAP,
    /// which can land a small bogus window inside the buffer and
    /// UNDER-count: the budget's unsafe direction, worse than any
    /// over-count.
    #[test]
    fn overflowing_bit_and_cell_windows_charge_everything_instead_of_wrapping() {
        // Bit windows (boolean values, validity bitmaps).
        for (first_bit, bits) in [
            (usize::MAX, 8),
            (usize::MAX - 3, usize::MAX),
            (8, usize::MAX),
        ] {
            assert_eq!(
                bit_window(first_bit, bits),
                (0, usize::MAX),
                "first_bit {first_bit}, bits {bits}: overflow must charge-everything"
            );
        }
        assert_eq!(bit_window(3, 10), (0, 2), "the ordinary window is exact");
        assert_eq!(bit_window(8, 8), (1, 1));

        // Cell windows (FixedSizeBinary and primitive-width values).
        for (offset, len, width) in [
            (usize::MAX / 2, 3, 8),
            (2, usize::MAX / 2, 8),
            (usize::MAX, usize::MAX, 2),
        ] {
            assert_eq!(
                cell_window(offset, len, width),
                (0, usize::MAX),
                "offset {offset}, len {len}, width {width}: overflow must charge-everything"
            );
        }
        assert_eq!(
            cell_window(2, 3, 8),
            (16, 24),
            "the ordinary window is exact"
        );
    }

    /// 047 L2: the window arithmetic over IPC-skewed `offset`/`len` values
    /// must SATURATE into the over-count fallback — the unchecked
    /// `(offset + len + 1) * width` panicked debug builds inside `send`
    /// (release wrapped into the safe `None` arm by accident, not design).
    #[test]
    fn an_overflowing_offsets_window_saturates_into_the_overcount_fallback() {
        let decode_i32 =
            |chunk: &[u8]| i32::from_ne_bytes(chunk.try_into().expect("a 4-byte chunk")) as usize;
        let backing = arrow_buffer::Buffer::from_vec(vec![0i32, 1, 3, 0]);
        for (offset, len) in [
            (usize::MAX, 2),
            (usize::MAX - 1, usize::MAX - 1),
            (0, usize::MAX),
        ] {
            assert_eq!(
                offsets_window(&backing, offset, len, 4, decode_i32),
                (0, usize::MAX, 0, usize::MAX),
                "offset {offset}, len {len}: overflow must charge-everything, never panic"
            );
        }
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
