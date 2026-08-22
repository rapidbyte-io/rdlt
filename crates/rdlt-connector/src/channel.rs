//! The byte-budgeted channel a source pushes records through.
//!
//! Backpressure is the contract here, and it is measured in BYTES, not
//! messages: a slow consumer parks the producer once the bytes sitting
//! unconsumed in the channel reach the budget, so peak memory stays capped
//! no matter how wide the rows are. Awaiting a push IS the flow control —
//! a source never polls, sleeps, or counts.
//!
//! Two layers live here. The generic core ([`bytes()`],
//! [`ByteSender`]/[`ByteReceiver`], [`ByteSized`], [`Permitted`]) is the one
//! implementation of the byte-budget rule for the whole tree — the SPI
//! states the backpressure contract, so the SPI owns the code; a second
//! copy elsewhere would let a fix to the rule apply to one path only. The
//! records layer ([`records()`], [`RecordsOut`]/[`RecordsIn`],
//! [`PushPayload`]) is that core specialized to what sources push.

use std::sync::Arc;

use arrow_array::RecordBatch;
use bytes::Bytes;
use tokio::sync::{Semaphore, mpsc};

use crate::core::cursor::Cursor;

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

/// The most JSON VALUES one raw push may carry — every object entry and
/// every nested array element counts one. Distinct
/// from the row cap, deliberately: rows materialize lineage and per-row
/// output, while each VALUE spends its arena node at parse. On the
/// pinned toolchain the measured layout is a ~24-byte `ArenaNode` (the
/// `Cow<str>` niche-packs), an object entry adds its ~32-byte `(key,
/// id)` tuple, and the per-object parse transients (the entries Vec,
/// the dedup map's escaped-key clones) roughly double the peak again —
/// so a 64 MiB frame of dense values would otherwise build a ~12-16×
/// arena before any traversal-time check could refuse. One of those
/// transients is KEPT rather than dropped for objects wide enough to
/// earn it (the engine's parse persists a wide object's key index so
/// the batch build stops re-scanning it per column per row); the
/// threshold is set so ordinary rows keep the transient-only profile
/// this arithmetic assumes, and a document wide enough to cross it
/// trades a bounded per-object table for a quadratic it would
/// otherwise pay per row. The value budget
/// caps that transient at the same order an honest maximal push already
/// pays: sixteen units per row covers a full-row-cap push of fifteen
/// fields or list elements per row, so no honest shape is refused,
/// while a frame denser than ~4 bytes per value refuses typed instead
/// of ballooning.
pub const MAX_JSON_VALUES_PER_PUSH: usize = 16 * MAX_RECORD_BATCH_ROWS;

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
    /// value.
    bytes: usize,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl<T> Permitted<T> {
    /// Take the value, releasing its budget.
    pub fn into_value(self) -> T {
        self.value
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
    /// Whether [`Self::close`] closes the budget semaphore too. Channels
    /// drawing from a SHARED budget ([`records_shared`]) must not:
    /// closing the shared semaphore would fail every OTHER channel's parked
    /// producer, turning one stream's teardown into a cascade.
    close_budget: bool,
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
    /// event the producer observes from either wait. A channel drawing
    /// from a SHARED budget closes only its own queue (see the field on
    /// the struct): the host's task teardown bounds how long a producer
    /// parked on the shared pool can linger.
    pub fn close(&mut self) {
        self.messages.close();
        if self.close_budget {
            self.budget.close();
        }
    }
}

/// A byte-budgeted channel: `byte_budget` caps unconsumed bytes in flight;
/// `message_capacity` is the secondary cap that keeps zero-byte markers
/// from queueing without limit.
pub fn bytes<T>(byte_budget: usize, message_capacity: usize) -> (ByteSender<T>, ByteReceiver<T>) {
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
            close_budget: true,
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
    /// could not enqueue would stall the commit it announces. The Arrow
    /// arm meters through [`arrow_batch_footprint`]; the charging rule
    /// and its rationale live on `data_footprint`.
    fn byte_size(&self) -> usize {
        match self {
            PushPayload::RawJson(bytes) => bytes.len(),
            PushPayload::Arrow(batch) => arrow_batch_footprint(batch),
            PushPayload::Checkpoint(cursor) => cursor.resident_footprint(),
        }
    }
}

/// The bytes an Arrow batch holds resident: the summed sizes of the
/// distinct underlying ALLOCATIONS reachable from its columns (values,
/// offsets, nulls, and nested child data, recursively), each counted
/// once. The charging rule and its rationale live on `data_footprint`.
///
/// Public because this is the ONE byte meter for Arrow batches wherever
/// a budget, commit policy, or report counts them — a host metering
/// batches by any other rule inflates or uncaps its accounting (arrow
/// 58 exposes no footprint API with these semantics, hence the walk).
pub fn arrow_batch_footprint(batch: &RecordBatch) -> usize {
    let mut seen = std::collections::HashSet::new();
    // Saturating: a column nested past [`MAX_ARROW_DEPTH`] charges
    // `usize::MAX`, and the sum must carry that over-count through
    // rather than overflow.
    batch.columns().iter().fold(0usize, |total, column| {
        total.saturating_add(data_footprint(&column.to_data(), &mut seen, 0))
    })
}

/// One node of the walk: this array's own buffers — each charged as
/// its whole underlying ALLOCATION, once per walk — then its children.
/// This comment is the ONE full telling of the charging rule that
/// `PushPayload::byte_size` and [`arrow_batch_footprint`] point at.
///
/// The budget's failure directions are asymmetric — over-counting
/// narrows a healthy window, under-counting uncaps memory, the one
/// unsafe direction — so every ambiguity breaks toward counting, and
/// both obvious meters fail: `RecordBatch::get_array_memory_size`
/// capacity-sums each buffer's parent allocation once PER BUFFER, and
/// an IPC-decoded batch's columns are all zero-copy slices of ONE
/// message-body allocation, so it charges that body once per buffer
/// (measured ≈10-17× the true footprint) and a wire source burns its
/// budget that many times too fast. Summing buffer LENGTHS fails the
/// other way: arrow-data validates buffer sizes only as a MINIMUM, the
/// IPC reader preserves wire-declared buffer lengths, and arrow's
/// typed wrappers trim a fixed-width values buffer to its node window
/// on construction while the trimmed slice keeps the entire allocation
/// alive — a crafted 64 MiB `Int64` values buffer with node length 1
/// metered 8 bytes, and with the meter neutralized a slow consumer
/// queues gigabytes against a 64 MiB budget. So the meter charges what
/// is actually RESIDENT: each distinct allocation (identity =
/// `data_ptr()`, the allocation's base, plus its size), once, at its
/// full `capacity()` — the dedup is what keeps the capacity-summing
/// pathology dead, and an `Arc`'d array reachable twice (two columns,
/// a shared dictionary) counts once for the same reason. The remaining
/// costs sit on the over-count side only: builder-built buffers'
/// capacity slack counts (bounded ≤2× by `Vec` growth), and a
/// `RecordBatch::slice` chunk charges its parent's allocations whole
/// (they remain resident through the chunk). No buffer content or
/// window arithmetic is read at all, so IPC-skewed `offset`/`len`
/// values cannot overflow, wrap, or panic the send path.
fn data_footprint(
    data: &arrow_data::ArrayData,
    seen: &mut std::collections::HashSet<(usize, usize)>,
    depth: usize,
) -> usize {
    // THE DEPTH CAP: nesting is connector-controlled and a stack
    // overflow is an abort. The meter must never error the send path, so
    // past the cap it stops and charges `usize::MAX` — the over-count
    // direction, the budget's safe side: `send` clamps the request to
    // the whole budget, so a hostile batch degrades to
    // drain-the-budget-and-go, never to a deeper frame.
    if depth > MAX_ARROW_DEPTH {
        return usize::MAX;
    }

    let mut count = |buffer: &arrow_buffer::Buffer| -> usize {
        // `capacity()` answers 0 for externally owned allocations (FFI
        // imports, which never arrive off this crate's wire) — the
        // slice length is the honest floor there.
        let size = buffer.capacity().max(buffer.len());
        if size > 0 && seen.insert((buffer.data_ptr().as_ptr() as usize, size)) {
            size
        } else {
            0
        }
    };

    let mut total = 0usize;
    for buffer in data.buffers() {
        total = total.saturating_add(count(buffer));
    }
    if let Some(nulls) = data.nulls() {
        total = total.saturating_add(count(nulls.buffer()));
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
pub fn records(byte_budget: usize) -> (RecordsOut, RecordsIn) {
    let (sender, receiver) = bytes(byte_budget, RECORDS_MESSAGE_CAPACITY);
    (
        RecordsOut { channel: sender },
        RecordsIn { channel: receiver },
    )
}

/// ONE byte budget shared by several records channels:
/// the engine runs one channel per stream, and per-channel budgets meant a
/// run's total in-flight allowance was `streams × budget` — unbounded on
/// the one axis discovery never capped. Channels drawn from one
/// `SharedBudget` spend from a single semaphore, so the run's peak memory
/// is the configured budget no matter how many streams the source declares.
///
/// Two semantics differ from a per-channel budget, both deliberate:
///
/// - Backpressure is shared: one stream's unconsumed bytes park EVERY
///   stream's producer once the pool is spent. That is the point — the
///   ceiling prices the run, not the stream. A rogue stream's expensive
///   payload can thus starve its siblings for the duration of one
///   push's processing — a degradation, never a hang (FIFO ordering,
///   zero-byte checkpoints exempt, and the parse/build paths are
///   budget-bounded so the duration is finite).
/// - Closing one channel's receiver does NOT close the shared semaphore
///   (that would cascade a failure into every sibling channel's parked
///   producer). A producer parked on the shared pool is woken by its own
///   task's teardown, which every host of this channel already performs
///   (the engine aborts its reader tasks after a bounded grace).
#[derive(Debug, Clone)]
pub struct SharedBudget {
    budget: Arc<Semaphore>,
    total: usize,
}

impl SharedBudget {
    /// A shared budget of `bytes` total in-flight bytes across every
    /// channel drawn from it.
    pub fn new(bytes: usize) -> Self {
        Self {
            budget: Arc::new(Semaphore::new(bytes)),
            total: bytes,
        }
    }
}

/// The shared-budget twin of [`records()`] — see [`SharedBudget`]
/// for the two semantic differences (shared backpressure; close does not
/// close the pool).
pub fn records_shared(budget: &SharedBudget) -> (RecordsOut, RecordsIn) {
    let (messages, receiver) = mpsc::channel(RECORDS_MESSAGE_CAPACITY);
    (
        RecordsOut {
            channel: ByteSender {
                messages,
                budget: Arc::clone(&budget.budget),
                budget_total: budget.total,
            },
        },
        RecordsIn {
            channel: ByteReceiver {
                messages: receiver,
                budget: Arc::clone(&budget.budget),
                close_budget: false,
            },
        },
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
        let (sender, mut receiver) = bytes::<Weighted>(100, ROOMY);
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
        let (sender, mut receiver) = bytes::<Weighted>(100, ROOMY);
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
        let (sender, mut receiver) = bytes::<Weighted>(16, ROOMY);
        sender.send(Weighted(1_000_000)).await.unwrap();
        assert_eq!(receiver.recv().await.unwrap().into_value().0, 1_000_000);
    }

    #[tokio::test]
    async fn receiving_without_dropping_keeps_the_budget_spent() {
        // The distinction `Permitted` exists for: receiving ≠ releasing.
        let (sender, mut receiver) = bytes::<Weighted>(100, ROOMY);
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
    //! THE FIXTURE HERE IS THIS CRATE'S OWN: the shared IPC fixture lives
    //! in rdlt-engine's tests and this crate sits below it, so these unit
    //! tests carry a private twin.
    //! This one is shaped for the channel pins alone — wider (more
    //! buffers) so capacity-summing lands further outside every bound —
    //! and the two need not agree.
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

    /// Every layout deliberately charges its full backing buffers, so a
    /// `slice()` chunk meters its parent's whole buffers (they stay
    /// resident through the chunk) and n chunks may meter up to n× the
    /// parent — the over-count direction, the budget's safe side. The
    /// load-bearing invariant is the floor: the chunks together must
    /// never under-count the parent's resident bytes.
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

    /// Arrow permits a values buffer to contain bytes no offset
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

    /// The unreferenced-values sibling for every other layout: arrow-data validates
    /// buffer sizes as a MINIMUM and the IPC reader preserves
    /// wire-declared buffer lengths, so a fixed-width column can carry
    /// a values buffer (or validity bitmap) far larger than its node
    /// length references — and arrow's typed wrappers then TRIM the
    /// values buffer to its 8-byte node window while the trimmed slice
    /// keeps the whole allocation alive. Those bytes stay resident;
    /// metering the node window (or the trimmed slice length)
    /// neutralizes the budget exactly the way the variable-width
    /// values gap did.
    #[test]
    fn an_oversized_fixed_width_buffer_and_bitmap_are_charged_in_full() {
        let retained = 8 * 1024 * 1024;
        // Bit 0 cleared so the one row is null: a bitmap with no nulls
        // may be normalized away, and this pin needs it to survive to
        // the meter.
        let mut bitmap = vec![0xffu8; retained];
        bitmap[0] = 0xfe;
        let data = arrow_data::ArrayData::try_new(
            DataType::Int64,
            1,
            Some(arrow_buffer::Buffer::from_vec(bitmap)),
            0,
            vec![arrow_buffer::Buffer::from_vec(vec![0u8; retained])],
            vec![],
        )
        .expect("arrow validates buffer sizes as a minimum, not an exact fit");
        let array = arrow_array::make_array(data);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)])),
            vec![array],
        )
        .expect("matching batch");

        let metered = arrow_batch_footprint(&batch);
        assert!(
            metered >= 2 * retained,
            "all resident bytes — the oversized values buffer AND the oversized \
             validity bitmap — must be charged: retained 2×{retained}, got {metered}"
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

    /// THE EMPTY-OFFSETS PIN, kept through whole-buffer
    /// charging: Arrow permits a zero-length variable-width array to
    /// carry an EMPTY offsets buffer — arrow-data 58's own validation
    /// allows 0 offsets ("An empty list-like array can have 0
    /// offsets") — and an earlier windowed meter panicked reading it.
    /// The walk is driven on the RAW `ArrayData` here, deliberately:
    /// arrow-array 58's typed wrappers happen to normalize the empty
    /// buffer to a single `0` on construction (measured: `make_array`
    /// then `to_data` yields a 4-byte offsets buffer), so a batch-level
    /// fixture cannot carry the shape today, but the meter walks
    /// `child_data` trees and validated foreign constructions where
    /// nothing promises that normalization.
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

    /// The meter walks connector-controlled nesting, and a stack
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
        let (mut out, mut input) = records(100);
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

    /// A checkpoint is charged for what its cursor holds, like every
    /// other queued value — but a charge is capped at the budget total,
    /// so on a zero budget a checkpoint still passes: one that could
    /// not enqueue would stall the commit it announces.
    #[tokio::test]
    async fn a_checkpoint_passes_even_on_a_zero_budget() {
        let (mut out, mut input) = records(0);
        tokio::time::timeout(
            BOUND,
            out.checkpoint(Cursor::new(serde_json::json!("watermark"))),
        )
        .await
        .expect("a zero-byte marker must not wait on the budget")
        .expect("channel open");
        assert!(input.recv().await.is_some(), "the marker arrives");
    }

    /// A dense checkpoint costs the budget what it holds: against a
    /// budget smaller than one cursor's footprint, a second checkpoint
    /// parks until the first is drained — the flood of zero-cost
    /// markers that rode past the budget no longer does.
    #[tokio::test]
    async fn a_dense_checkpoint_is_charged_to_the_budget() {
        let cursor = Cursor::new(serde_json::Value::Array(vec![
            serde_json::Value::Null;
            1024
        ]));
        let footprint = cursor.resident_footprint();
        assert!(
            footprint > 1024,
            "a thousand nodes cost more than a byte each"
        );
        let (mut out, mut input) = records(footprint);
        out.checkpoint(cursor.clone())
            .await
            .expect("the first fills the budget");
        let parked = tokio::time::timeout(Duration::from_millis(100), out.checkpoint(cursor)).await;
        assert!(
            parked.is_err(),
            "the second waits on the budget the first holds"
        );
        drop(input.recv().await.expect("drain the first"));
    }

    #[tokio::test]
    async fn close_wakes_a_push_parked_on_the_budget() {
        // A producer parked on the semaphore learns about the close too;
        // otherwise "stop" would only reach sources between pushes.
        let (mut out, mut input) = records(8);
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

    /// The shared budget's whole point: two channels drawn from one
    /// [`SharedBudget`] spend from ONE pool — a full first channel parks
    /// the second channel's producer even though the second channel's own
    /// queue is empty, and draining the first frees the second.
    #[tokio::test]
    async fn channels_sharing_a_budget_share_one_in_flight_ceiling() {
        let budget = SharedBudget::new(100);
        let (mut out_a, mut in_a) = records_shared(&budget);
        let (mut out_b, mut in_b) = records_shared(&budget);
        out_a
            .raw_json(Bytes::from(vec![b'x'; 100]))
            .await
            .expect("the first channel fills the pool");
        let parked = tokio::spawn(async move { out_b.raw_json(Bytes::from_static(b"y")).await });
        tokio::time::sleep(Duration::from_millis(50)).await; // let it park
        assert!(
            !parked.is_finished(),
            "the sibling channel parks on the shared pool"
        );
        // Draining the first channel frees the pool — the parked sibling
        // push completes.
        drop(in_a.recv().await.expect("a's queued push"));
        tokio::time::timeout(BOUND, parked)
            .await
            .expect("the parked push completes once the pool frees")
            .expect("task joins")
            .expect("push lands");
        assert!(in_b.recv().await.is_some(), "and it arrived on b");
    }

    /// Closing a shared-budget channel must NOT close the shared semaphore:
    /// doing so would fail every sibling channel's parked producer — one
    /// stream's teardown cascading into the rest (the engine closes a
    /// stream's channel on EVERY exit, normal ones included).
    #[tokio::test]
    async fn closing_one_shared_channel_never_closes_the_pool() {
        let budget = SharedBudget::new(8);
        let (_out_a, mut in_a) = records_shared(&budget);
        let (mut out_b, mut in_b) = records_shared(&budget);
        in_a.close();
        out_b
            .raw_json(Bytes::from_static(b"12345678"))
            .await
            .expect("the sibling's pool is intact after a's close");
        assert!(in_b.recv().await.is_some(), "the push arrives");
    }

    #[tokio::test]
    async fn close_refuses_further_pushes() {
        let (mut out, mut input) = records(1024);
        input.close();
        let refused = tokio::time::timeout(BOUND, out.raw_json(Bytes::from_static(b"{\"row\":1}")))
            .await
            .expect("a closed channel answers promptly");
        assert_eq!(refused, Err(ChannelClosed));
    }

    #[tokio::test]
    async fn rows_serialize_once_to_ndjson_and_an_empty_iterator_is_a_no_op() {
        let (mut out, mut input) = records(1024);
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
