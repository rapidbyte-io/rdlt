//! The byte-budgeted push channel between a host and one `Source::read` call.
//!
//! Awaiting a push IS the flow control: the budget is bounded in BYTES (not messages),
//! so a slow destination propagates backpressure to the source without letting peak
//! memory scale with row width.

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{Semaphore, mpsc};

use crate::RecordBatch;
use crate::core::Cursor;

/// Secondary message-count bound on the channel. The byte budget is the primary
/// backpressure; this hard cap keeps zero-byte messages (checkpoints) from queueing
/// without limit when the budget alone would never park them.
const CHANNEL_MSG_CAPACITY: usize = 64;

/// The stream was closed by the host (cancellation or failure downstream). Sources
/// should return promptly without escalating.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("records channel closed by host")]
pub struct ChannelClosed;

/// What a source pushed. Obtained by hosts from [`RecordsIn::recv`]; carries its
/// byte-budget permit, released when the message is dropped.
#[derive(Debug)]
pub struct SourcePush {
    /// What was pushed.
    pub payload: PushPayload,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

/// The shapes a source can push.
///
/// Three shapes rather than one because the cheapest path differs by source: a
/// REST body is already JSON bytes, a database read is already Arrow, and a
/// checkpoint carries no rows at all. Forcing everything through one
/// representation would mean parsing and re-serializing data that was already in
/// the right form.
#[derive(Debug)]
pub enum PushPayload {
    /// Raw JSON bytes: one document, an array of documents, or NDJSON. The engine's
    /// shredder parses these directly into Arrow builders. `rows()` also lands here
    /// (canonically serialized) so hosts have exactly one JSON ingest path.
    RawJson(Bytes),
    /// A source-native Arrow batch; bypasses the shredder (schema check only).
    Arrow(RecordBatch),
    /// "All rows pushed so far are complete up to this cursor."
    Checkpoint(Cursor),
}

/// Push handle held by sources. Awaiting a push is the flow control: the byte budget
/// (bounded in BYTES, not messages) is what a slow destination propagates back to the
/// source.
#[derive(Debug)]
pub struct RecordsOut {
    tx: ByteTx<PushPayload>,
}

impl RecordsOut {
    /// Perf path: raw JSON bytes (one document, an array, or NDJSON). Sources that
    /// already hold bytes (HTTP bodies, files) should always use this.
    pub async fn raw_json(&mut self, bytes: Bytes) -> Result<(), ChannelClosed> {
        self.tx.send(PushPayload::RawJson(bytes)).await
    }

    /// Convenience path for programmatically constructed rows; serialized here to
    /// NDJSON so hosts see a single JSON ingest path.
    pub async fn rows(
        &mut self,
        rows: impl IntoIterator<Item = serde_json::Value>,
    ) -> Result<(), ChannelClosed> {
        let mut buf = Vec::new();
        for row in rows {
            // A `serde_json::Value` written to a `Vec` cannot fail (the writer is
            // infallible and a `Value` is always valid JSON — `Number` rejects
            // non-finite floats at construction). Asserting that invariant is
            // truthful; mapping a would-be failure to `ChannelClosed` was a lie
            // that told the source to stop as if the host had hung up.
            serde_json::to_writer(&mut buf, &row)
                .expect("a serde_json::Value serializes to a Vec infallibly");
            buf.push(b'\n');
        }
        if buf.is_empty() {
            return Ok(());
        }
        self.tx.send(PushPayload::RawJson(buf.into())).await
    }

    /// Push a source-native Arrow batch, bypassing the shredder — only its
    /// schema is checked. The cheapest path for a source that already has
    /// columnar data.
    pub async fn arrow(&mut self, batch: RecordBatch) -> Result<(), ChannelClosed> {
        self.tx.send(PushPayload::Arrow(batch)).await
    }

    /// "All rows pushed so far are complete up to `cursor`."
    pub async fn checkpoint(&mut self, cursor: Cursor) -> Result<(), ChannelClosed> {
        self.tx.send(PushPayload::Checkpoint(cursor)).await
    }
}

/// Receiving half held by hosts (the engine, or a conformance harness).
#[derive(Debug)]
pub struct RecordsIn {
    rx: ByteRx<PushPayload>,
}

impl RecordsIn {
    /// `None` when the source finished (dropped its `RecordsOut`).
    ///
    /// The permit travels with the payload into `SourcePush`, so the budget
    /// stays spent until the host drops it — the byte bound describes queued
    /// bytes, not bytes ever sent.
    pub async fn recv(&mut self) -> Option<SourcePush> {
        self.rx.recv().await.map(|permitted| {
            let (payload, permit) = permitted.into_parts();
            SourcePush {
                payload,
                _permit: permit,
            }
        })
    }

    /// Closing tells the source to stop at its next push.
    pub fn close(&mut self) {
        self.rx.close();
    }
}

impl ByteSized for PushPayload {
    /// What the payload actually holds. A checkpoint carries no rows, so it
    /// costs nothing and must never be gated by the budget — a marker that
    /// could not enqueue would stall committing.
    fn byte_size(&self) -> usize {
        match self {
            PushPayload::RawJson(bytes) => bytes.len(),
            PushPayload::Arrow(batch) => batch.get_array_memory_size(),
            PushPayload::Checkpoint(_) => 0,
        }
    }
}

// ---- The generic byte-budget channel core ----
//
// One implementation serves every byte-bounded channel in the tree: the
// source-push path above, and the host's own internal stages. It lives in the
// SPI because the SPI is where the byte-budget CONTRACT is stated — a channel
// bounded by bytes rather than messages, so peak memory stays capped regardless
// of row width. Nothing about it is host-specific, and a second copy would mean
// a fix to the backpressure rule silently applying to one path only.

/// Items that know their own in-memory footprint.
///
/// The budget is spent in these units, so an implementation that under-reports
/// weakens backpressure and one that over-reports throttles early. Report what
/// the value actually holds.
pub trait ByteSized {
    /// Bytes this value occupies in memory.
    fn byte_size(&self) -> usize;
}

/// A received value together with the budget it still holds.
///
/// The permit rides WITH the value and is released when this is dropped, which
/// is what makes the budget describe queued-but-unconsumed bytes rather than
/// bytes-ever-sent. A consumer that holds one keeps the producer parked; that
/// is the intended coupling, not a leak.
#[derive(Debug)]
pub struct Permitted<T> {
    value: T,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl<T> Permitted<T> {
    /// The value, dropping the permit and releasing its budget.
    pub fn into_value(self) -> T {
        self.value
    }

    /// Borrow without releasing anything.
    pub fn value(&self) -> &T {
        &self.value
    }

    /// Split into value and permit, for a caller that must keep the budget held
    /// while re-wrapping the value in its own type.
    pub(crate) fn into_parts(self) -> (T, Option<tokio::sync::OwnedSemaphorePermit>) {
        (self.value, self._permit)
    }
}

/// Sending half of a byte-bounded channel.
#[derive(Debug)]
pub struct ByteTx<T> {
    tx: mpsc::Sender<Permitted<T>>,
    budget: Arc<Semaphore>,
    budget_max: usize,
}

// Manual impl: `T` need not be `Clone` for the SENDER to be.
impl<T> Clone for ByteTx<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            budget: Arc::clone(&self.budget),
            budget_max: self.budget_max,
        }
    }
}

/// Receiving half of a byte-bounded channel.
#[derive(Debug)]
pub struct ByteRx<T> {
    rx: mpsc::Receiver<Permitted<T>>,
    budget: Arc<Semaphore>,
}

impl<T: ByteSized> ByteTx<T> {
    /// Send, parking until the value's bytes fit the budget.
    ///
    /// An item larger than the WHOLE budget degrades to "drain the budget"
    /// rather than deadlocking: the `.min` caps the request, so the `u32`
    /// conversion can only overflow when the budget itself exceeds `u32::MAX`
    /// (semaphore permits are `u32`), where it saturates — still a valid
    /// drain-everything request.
    pub async fn send(&self, value: T) -> Result<(), ChannelClosed> {
        let permits = value
            .byte_size()
            .min(self.budget_max)
            .try_into()
            .unwrap_or(u32::MAX);
        // `> 0` versus `>= 0` is an equivalent mutant — acquiring zero permits
        // always succeeds. Kept as the cheaper path, and as a statement that
        // zero-byte markers are not budgeted.
        let permit = if permits > 0 {
            Some(
                Arc::clone(&self.budget)
                    .acquire_many_owned(permits)
                    .await
                    .map_err(|_| ChannelClosed)?,
            )
        } else {
            None
        };
        self.tx
            .send(Permitted {
                value,
                _permit: permit,
            })
            .await
            .map_err(|_| ChannelClosed)
    }
}

impl<T> ByteRx<T> {
    /// `None` once every sender is dropped and the channel is drained.
    pub async fn recv(&mut self) -> Option<Permitted<T>> {
        self.rx.recv().await
    }

    /// Stop the producer at its next send.
    ///
    /// Both halves matter: closing the receiver refuses further sends, and
    /// closing the budget WAKES a producer already parked on a permit that
    /// would otherwise never be released.
    pub fn close(&mut self) {
        self.rx.close();
        self.budget.close();
    }
}

/// A byte-bounded channel: `byte_budget` bounds in-flight bytes, `msg_capacity`
/// is the secondary message cap that stops zero-byte markers queueing without
/// limit when the budget alone would never park them.
pub fn byte_channel<T>(byte_budget: usize, msg_capacity: usize) -> (ByteTx<T>, ByteRx<T>) {
    let budget = Arc::new(Semaphore::new(byte_budget));
    let (tx, rx) = mpsc::channel(msg_capacity);
    (
        ByteTx {
            tx,
            budget: Arc::clone(&budget),
            budget_max: byte_budget,
        },
        ByteRx { rx, budget },
    )
}

/// Create the push channel between a host and one `Source::read` call.
/// `byte_budget` bounds in-flight bytes (backpressure); message count is secondary.
pub fn records_channel(byte_budget: usize) -> (RecordsOut, RecordsIn) {
    let (tx, rx) = byte_channel(byte_budget, CHANNEL_MSG_CAPACITY);
    (RecordsOut { tx }, RecordsIn { rx })
}

#[cfg(test)]
mod budget_tests {
    // Mutation-report closure: same boundary at the SPI layer.
    use super::*;

    #[tokio::test]
    async fn exact_budget_passes_and_next_push_waits() {
        let (mut out, mut input) = records_channel(100);
        out.raw_json(bytes::Bytes::from(vec![b'1'; 100]))
            .await
            .expect("exactly the budget");
        let pending = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            out.raw_json(bytes::Bytes::from_static(b"2")),
        )
        .await;
        assert!(
            pending.is_err(),
            "budget exhausted: the next push must wait"
        );
        let push = input.recv().await.expect("push");
        drop(push);
        out.raw_json(bytes::Bytes::from_static(b"2"))
            .await
            .expect("freed budget");
    }
}

#[cfg(test)]
mod close_tests {
    //! `RecordsIn::close` is how a host tells a source to STOP.
    //!
    //! Without it a source blocked on the byte budget never learns the host is
    //! gone: it waits on a permit that will never be released. That is a hang,
    //! not a wrong answer, which is why it reached the survivor list as a
    //! timeout rather than a miss — and why these assertions are bounded.
    use super::*;
    use std::time::Duration;

    const BOUND: Duration = Duration::from_secs(5);

    #[tokio::test]
    async fn close_wakes_a_source_parked_on_the_byte_budget() {
        // Budget of 8 bytes: the first push takes it, the second must park.
        let (mut out, mut input) = records_channel(8);
        out.raw_json(bytes::Bytes::from_static(b"12345678"))
            .await
            .expect("first push fits the budget");

        // Park a second push, then close from the host side.
        let parked =
            tokio::spawn(async move { out.raw_json(bytes::Bytes::from_static(b"12345678")).await });
        // Give it a moment to actually block on the semaphore.
        tokio::time::sleep(Duration::from_millis(50)).await;
        input.close();

        let result = tokio::time::timeout(BOUND, parked)
            .await
            .expect("close must WAKE the parked source, not leave it waiting")
            .expect("task joins");
        assert_eq!(
            result,
            Err(ChannelClosed),
            "a woken source learns the host is gone, and says so"
        );
    }

    #[tokio::test]
    async fn close_stops_further_pushes() {
        let (mut out, mut input) = records_channel(1024);
        input.close();
        let result =
            tokio::time::timeout(BOUND, out.raw_json(bytes::Bytes::from_static(b"{\"a\":1}")))
                .await
                .expect("a closed channel refuses promptly");
        assert_eq!(result, Err(ChannelClosed));
    }

    #[tokio::test]
    async fn zero_sized_pushes_do_not_consume_budget() {
        // A checkpoint carries no rows, so it must not be gated by the byte
        // budget — on a zero budget it still has to get through, or committing
        // stalls forever.
        let (mut out, mut input) = records_channel(0);
        tokio::time::timeout(
            BOUND,
            out.checkpoint(rdlt_core::Cursor::new(serde_json::json!(1))),
        )
        .await
        .expect("a zero-byte marker must not wait on the budget")
        .expect("channel open");
        assert!(input.recv().await.is_some(), "the marker arrives");
    }
}

#[cfg(test)]
mod core_tests {
    // The generic core, exercised directly rather than through the records
    // path: what the budget counts, what happens at its exact boundary, and
    // that an item too large for the whole budget still gets through.
    use super::*;

    /// A payload whose only property is the footprint it reports.
    #[derive(Debug)]
    struct Blob(usize);
    impl ByteSized for Blob {
        fn byte_size(&self) -> usize {
            self.0
        }
    }

    /// Any capacity large enough that the message cap never fires first — these
    /// tests are about the BYTE budget, and a message-count stall would be a
    /// different thing passing for the same one.
    const AMPLE_MSGS: usize = 256;

    #[tokio::test]
    async fn capacity_is_bytes_not_items() {
        let (tx, mut rx) = byte_channel::<Blob>(100, AMPLE_MSGS);
        // Many small items fit even though they exceed any small item count…
        for _ in 0..50 {
            tx.send(Blob(2)).await.unwrap();
        }
        for _ in 0..50 {
            rx.recv().await.unwrap();
        }

        // …but a second large item blocks until the first is received. The budget
        // covers *queued* bytes; once a consumer takes an item, accounting for the
        // derived data belongs to that consumer's own downstream channel.
        tx.send(Blob(80)).await.unwrap();
        let pending = tx.send(Blob(80));
        tokio::pin!(pending);
        tokio::select! {
            _ = &mut pending => panic!("byte budget not enforced"),
            _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {}
        }
        // `drop`, not a binding: receiving does not release the budget — the
        // permit rides with the value (see `a_held_permit_keeps_the_budget_spent`),
        // so holding the received item here would starve the pending send forever.
        drop(rx.recv().await.unwrap());
        pending.await.unwrap();
    }

    #[tokio::test]
    async fn oversized_item_still_passes() {
        // Degrades to "drain the budget" rather than deadlocking forever on a
        // request the semaphore can never satisfy.
        let (tx, mut rx) = byte_channel::<Blob>(16, AMPLE_MSGS);
        tx.send(Blob(1_000_000)).await.unwrap();
        assert_eq!(rx.recv().await.unwrap().value().0, 1_000_000);
    }

    #[tokio::test]
    async fn exact_budget_passes_and_next_send_waits() {
        let (tx, mut rx) = byte_channel::<Blob>(100, AMPLE_MSGS);
        tx.send(Blob(100)).await.expect("exactly the budget");
        let pending =
            tokio::time::timeout(std::time::Duration::from_millis(50), tx.send(Blob(1))).await;
        assert!(pending.is_err(), "budget exhausted: the next send must wait");
        // Dropping the received item releases its permit; the budget is what is
        // HELD, not what was ever sent.
        drop(rx.recv().await.expect("item"));
        tx.send(Blob(1)).await.expect("freed budget");
    }

    #[tokio::test]
    async fn a_held_permit_keeps_the_budget_spent() {
        // The distinction `Permitted` exists to make: receiving is not releasing.
        let (tx, mut rx) = byte_channel::<Blob>(100, AMPLE_MSGS);
        tx.send(Blob(100)).await.expect("exactly the budget");
        let held = rx.recv().await.expect("item");
        let blocked =
            tokio::time::timeout(std::time::Duration::from_millis(50), tx.send(Blob(1))).await;
        assert!(
            blocked.is_err(),
            "a received-but-unreleased item still occupies the budget"
        );
        assert_eq!(held.into_value().0, 100);
        tx.send(Blob(1)).await.expect("released by into_value");
    }
}
