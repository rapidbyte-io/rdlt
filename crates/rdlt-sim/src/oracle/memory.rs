//! What a seed's thread holds on the heap at each moment of an attempt beyond what is charged for
//! it then: what the engine's budgets hold reserved and the source holds of pushes not yet sent
//! on; over the network, what the reads hold between them; and an allowance.

#[cfg(test)]
mod tests;
mod trace;

use std::cell::{Cell, RefCell};
use std::marker::PhantomData;

use alloc_tracker::{Session, ThreadSpan};
use serde_json::{Map, Value, json};

use super::scenario::Executed;
use crate::seed::Seed;
use crate::world::Largest;
pub use trace::TRACE_VAR;
use trace::Trace;
pub(super) use trace::{Rise, Tracing};

/// Bytes: what the thread of a seed whose connectors run in its process may hold beyond what is
/// charged for it: the runtime's, the world's copies of what it stores and builders' spare room;
/// ADR 0039 says how it was found.
///
/// Fat values do not raise it: the data they add is smaller than this fixed overhead.
const IN_PROCESS: u64 = 13 << 20;

/// Bytes: as [`IN_PROCESS`], where the connectors are served apart from the engine, on the
/// simulated network or in played processes, and their hosts share the thread; what their reads
/// hold between the source and the engine is counted beside it, by [`QUEUED`], [`CHANNEL`] and
/// what each read's stream holds in transport.
const NETWORK: u64 = 11 << 20;

/// Events a read over the network may hold beyond the push its source is sending and its
/// channel's, each place holding at most one: the batch being cut, and the piece of it being
/// written; an event is a push or a checkpoint, which carries its cursor.
///
/// The host reserves each frame before it reads its body, so nothing it holds of a read is
/// uncharged.
const QUEUED: u64 = 2;

/// Events the channel of a read the connector serves holds, whose pushes keep alive no more than
/// the host's frame limit together, or one larger push alone; checkpoints pass it free.
const CHANNEL: u64 = 16;

/// Bytes: the allowance of `seed`'s attempts, where its work is `inline` on one thread and its
/// connectors are served apart from the engine on its thread, on the network or in played
/// processes, where `apart`; `None` where nothing is measured, among them a networked seed with a
/// `hostile` host, whose heap shares the thread with the engine's.
///
/// # Panics
///
/// Panics where the binary's allocator counts nothing.
pub(super) fn allowance(seed: Seed, inline: bool, apart: bool, hostile: bool) -> Option<u64> {
    if !inline || (apart && hostile) {
        return None;
    }
    assert!(
        installed(),
        "seed {seed}: the test binary counts no allocations: it needs alloc_tracker's Allocator \
         as its global allocator"
    );
    Some(if apart { NETWORK } else { IN_PROCESS })
}

/// Bytes: what `reads` reads over the network hold beyond the pushes their source is sending,
/// where the largest events are `largest`, the host's frame limit is `frame` and each read's
/// stream holds `transport` in transport.
///
/// A read's channel holds its events, or its pushes' window and every event a cursor, whichever
/// is less.
fn queued(reads: u64, largest: Largest, frame: u64, transport: u64) -> u64 {
    let event = largest.event();
    let window = frame
        .saturating_add(largest.push)
        .saturating_add(CHANNEL.saturating_mul(largest.cursor));
    let channel = CHANNEL.saturating_mul(event).min(window);
    let read = QUEUED
        .saturating_mul(event)
        .saturating_add(channel)
        .saturating_add(transport);
    reads.saturating_mul(read)
}

/// What a seed's thread held over its worst attempt beyond what was charged for it, and what
/// its reads hold over the network.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Held {
    /// Bytes: the most the thread held at any moment beyond what was charged for it then: what
    /// the engine's budgets held reserved and the source held of pushes not yet sent on, less
    /// what both held as the attempt began.
    pub held: u64,
    /// Bytes: over the network, what the reads hold between the source and the engine; none in
    /// process.
    pub queued: u64,
}

impl Held {
    /// Bytes: how far `held` passed what the reads hold, below zero where it stayed within it.
    fn over(self) -> i128 {
        i128::from(self.held) - i128::from(self.queued)
    }

    /// The worse of the worst attempt so far, if any, and `attempt`: whichever passed what its
    /// reads hold by more.
    fn worse(worst: Option<Self>, attempt: Self) -> Self {
        match worst {
            Some(worst) if worst.over() >= attempt.over() => worst,
            _ => attempt,
        }
    }

    /// Adds what the worst attempt held beyond its charge, and what its reads hold, to a timing
    /// line.
    pub(super) fn record(self, fields: &mut Map<String, Value>) {
        fields.insert("held_bytes".to_owned(), json!(self.held));
        fields.insert("queued_bytes".to_owned(), json!(self.queued));
    }

    /// Checks that over `phase` of `seed` the thread held no more beyond its charge than its
    /// reads hold and `allowance`, where a traced attempt's `rises` name when it held more.
    ///
    /// # Panics
    ///
    /// Panics, naming the seed and phase, and the change of the charge after which it held more
    /// where the attempt was traced, where it held more.
    fn within(self, seed: Seed, phase: usize, allowance: u64, rises: &[Rise]) {
        let (held, queued) = (self.held, self.queued);
        let limit = queued.saturating_add(allowance);
        if held <= limit {
            return;
        }
        // The trace reads the heap only at changes of the charge: where none read past the
        // limit, the change after which the most last rose names the moment.
        let named = rises.iter().find(|rise| rise.most > limit).or(rises.last());
        let moment = named.map_or_else(
            || format!("replay it with {TRACE_VAR}=1 to name the change of the charge after which it held them"),
            Rise::named,
        );
        panic!(
            "seed {seed}: phase {phase}: an attempt's thread held {held} bytes of heap beyond \
             what was charged for it at that moment, past the {queued} its reads hold over the \
             network and an allowance of {allowance}: {moment}"
        );
    }
}

impl super::Simulation {
    /// Checks that over an attempt of `phase` whose runs went as `executed` say, the thread held
    /// `held` bytes at most beyond what was charged for it, no more than the reads hold and the
    /// allowance; `lifted` is what the attempt's counterweight found.
    ///
    /// # Panics
    ///
    /// Panics, naming the seed and phase, where it held more, or where more was charged than the
    /// counterweight `lifted` had room for, so `held` is not known.
    pub(super) fn within_memory(
        &mut self,
        held: u64,
        lifted: &Lifted,
        executed: &[Executed],
        phase: usize,
    ) {
        let Some(allowance) = self.allowance else {
            return;
        };
        let seed = self.seed;
        assert!(
            !lifted.short,
            "seed {seed}: phase {phase}: more was charged than the counterweight had room for"
        );
        let started: u64 = executed.iter().map(|run| run.started).sum();
        let queued = self.placing.as_ref().map_or(0, |placing| {
            let reads = started.saturating_mul(self.partitions);
            let largest = self.world.pushes.largest();
            queued(reads, largest, placing.frame_bytes(), placing.transport())
        });
        let attempt = Held { held, queued };
        self.held = Some(Held::worse(self.held, attempt));
        attempt.within(seed, phase, allowance, &lifted.rises);
    }

    /// Bytes: room enough for what an attempt of `plans` may charge beyond what was charged as it
    /// began: every run's budget, as each pipeline starts a few runs, what the source holds, and
    /// the room the world's work and its measures take.
    pub(super) fn room(&self, plans: usize) -> u64 {
        let runs = u64::try_from(plans)
            .unwrap_or(u64::MAX)
            .saturating_mul(RUNS);
        self.budget
            .saturating_mul(runs)
            .saturating_add(PUSHES)
            .saturating_add(ASIDE)
            .saturating_add(u64::try_from(PAST).unwrap_or(u64::MAX))
    }
}

/// Bytes: room for what the world's own work allocates while it builds what it hands the engine.
const ASIDE: u64 = 256 << 20;

/// Runs `work`, the world's own, with room for what it allocates accounted while it runs, so
/// what it builds counts as held beyond a charge only once it is done.
pub(crate) fn aside<T>(work: impl FnOnce() -> T) -> T {
    let room = i64::try_from(ASIDE).unwrap_or(i64::MAX);
    account(room);
    let _given = Aside(room);
    work()
}

/// Bytes the simulated world keeps beside the engine, as its stores keep what they were handed,
/// counted with the charge while this lives: they are the world's, not what the engine holds.
#[derive(Debug)]
#[must_use = "the bytes count as kept only while this lives"]
pub(crate) struct Kept(i64);

impl Kept {
    /// Keeps `bytes` this thread allocated, which count with the charge from now on.
    pub(crate) fn held(bytes: usize) -> Self {
        let bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
        account(bytes);
        Self(bytes)
    }
}

impl Clone for Kept {
    /// A copy of what is kept is made on this thread, which counts it as it is made.
    fn clone(&self) -> Self {
        Self(0)
    }
}

impl Drop for Kept {
    fn drop(&mut self) {
        account(-self.0);
    }
}

/// What `run` makes, and the most the calling thread held beyond its charge while it ran.
#[cfg(test)]
pub(crate) fn beyond_charge<T>(run: impl FnOnce() -> T) -> (T, u64) {
    let room = 1 << 30;
    let window = Window::open();
    let weight = Counterweight::hang(charged(), room, None);
    let made = run();
    assert!(!weight.lift().short, "the counterweight had room");
    (made, window.close().saturating_sub(room))
}

/// Bytes: an allocation larger than what freeing any one kept value gives back.
const PAST: usize = 256 << 20;

/// Bytes: what dropping `value` gives back to the calling thread's count.
///
/// An allocation of [`PAST`] made and freed after the drop peaks at that, less what was given
/// back. It is accounted for first, so a window around it sees nothing.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "a peak is a whole, non-negative number of bytes far below 2^53"
)]
pub(crate) fn freed<T>(value: T) -> usize {
    let session = Session::new().no_stdout().no_file();
    let operation = session.operation("freed");
    let past = i64::try_from(PAST).unwrap_or(i64::MAX);
    account(past);
    let span = operation.measure_thread().iterations(1);
    drop(value);
    drop(std::hint::black_box(Vec::<u8>::with_capacity(PAST)));
    drop(span);
    account(-past);
    let peak = session
        .to_report()
        .operations()
        .find_map(|(_, operation)| operation.peak_outstanding_bytes())
        .map_or(0, |peak| peak as usize);
    assert!(
        peak > 0,
        "freeing a kept value gave back {PAST} bytes or more"
    );
    PAST - peak
}

/// Room accounted for the world's own work, given back when dropped.
struct Aside(i64);

impl Drop for Aside {
    fn drop(&mut self) {
        account(-self.0);
    }
}

/// Runs a pipeline's scenario may hold at once, a reset among them.
const RUNS: u64 = 4;

/// Bytes: room for what a source holds of pushes not yet sent on.
const PUSHES: u64 = 256 << 20;

thread_local! {
    /// Bytes: what the budgets that told this thread of their changes hold reserved, and what
    /// sources on it hold of pushes not yet sent on.
    static CHARGED: Cell<i64> = const { Cell::new(0) };

    /// The counterweight hung on this thread, if any.
    static WEIGHT: RefCell<Option<Weight>> = const { RefCell::new(None) };
}

/// An allocation counted on its thread whose size falls as what is charged on the thread rises,
/// so that what the thread counts, less it, is what the thread holds beyond what is charged.
struct Weight {
    /// Never written, so it holds address space rather than memory.
    bytes: Vec<u8>,
    /// Bytes: its size where nothing was charged.
    level: i64,
    /// Whether more was charged than it had room for.
    short: bool,
    /// Where the attempt is traced, its trace.
    trace: Option<Trace>,
}

impl Weight {
    /// Takes the size that keeps it and `charged` together at its level.
    fn follow(&mut self, charged: i64) {
        let Ok(size) = usize::try_from(self.level - charged) else {
            self.short = true;
            return;
        };
        if size < self.bytes.capacity() {
            self.bytes.shrink_to(size);
        } else {
            self.bytes.reserve_exact(size);
        }
    }
}

/// Bytes: what is charged on this thread now.
pub(crate) fn charged() -> i64 {
    CHARGED.get()
}

/// Accounts for `change` bytes more charged on this thread, or fewer where it is negative, and
/// moves the counterweight hung on it, if any.
pub(crate) fn account(change: i64) {
    let charged = CHARGED.get().saturating_add(change);
    CHARGED.set(charged);
    WEIGHT.with_borrow_mut(|weight| {
        if let Some(weight) = weight {
            weight.follow(charged);
            if let Some(trace) = &mut weight.trace {
                trace.read(Some(change));
            }
        }
    });
}

/// A counterweight hung on the calling thread: while it hangs, the heap the thread counts is what
/// it holds beyond what is charged on it, and a constant.
pub(super) struct Counterweight {
    /// Lifted on the thread it was hung on.
    _thread: PhantomData<*const ()>,
}

impl Counterweight {
    /// Hangs a counterweight with `room` for what is charged on the calling thread beyond
    /// `origin`, what was charged on it as its seed began, tracing the attempt where `tracing` is
    /// ready.
    ///
    /// What was charged before the attempt began counts against what the thread holds as surely
    /// as what is charged in it.
    ///
    /// # Panics
    ///
    /// Panics where one already hangs on the thread.
    pub(super) fn hang(origin: i64, room: u64, tracing: Option<Tracing>) -> Self {
        let charged = CHARGED.get();
        let mut weight = Weight {
            bytes: Vec::new(),
            level: origin.saturating_add(i64::try_from(room).unwrap_or(i64::MAX)),
            short: false,
            trace: None,
        };
        weight.follow(charged);
        weight.trace = tracing.map(|tracing| tracing.start(charged.saturating_sub(origin)));
        WEIGHT.with_borrow_mut(|hung| {
            assert!(
                hung.is_none(),
                "a counterweight already hangs on this thread"
            );
            *hung = Some(weight);
        });
        Self {
            _thread: PhantomData,
        }
    }

    /// Lifts the counterweight: what it found while it hung.
    pub(super) fn lift(self) -> Lifted {
        let weight = WEIGHT.with_borrow_mut(|weight| {
            weight
                .as_mut()
                .map(|weight| (weight.short, weight.trace.take()))
        });
        drop(self);
        let (short, trace) = weight.unwrap_or_default();
        Lifted {
            short,
            rises: trace.map(Trace::end).unwrap_or_default(),
        }
    }
}

/// What a counterweight found while it hung.
#[derive(Debug, Default)]
pub(super) struct Lifted {
    /// Whether more was charged than it had room for.
    short: bool,
    /// Where the attempt was traced, the changes of the charge after which the most the thread
    /// held beyond it rose past another [`RISE`], in order.
    rises: Vec<Rise>,
}

impl Drop for Counterweight {
    fn drop(&mut self) {
        drop(WEIGHT.with_borrow_mut(Option::take));
    }
}

/// A stretch of the calling thread's work whose heap peak is measured: the most the thread held
/// beyond what it held as the stretch began.
pub(crate) struct Window {
    session: Session,
    span: Option<ThreadSpan>,
}

impl Window {
    /// Starts measuring the calling thread's heap, from what it holds now.
    pub(crate) fn open() -> Self {
        let session = Session::new().no_stdout().no_file();
        let span = session.operation("window").measure_thread().iterations(1);
        Self {
            session,
            span: Some(span),
        }
    }

    /// Bytes: the most the thread held beyond what it held as the window opened.
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "a peak is a whole, non-negative number of bytes far below 2^53"
    )]
    pub(crate) fn close(mut self) -> u64 {
        drop(self.span.take());
        let report = self.session.to_report();
        report
            .operations()
            .find_map(|(_, operation)| operation.peak_outstanding_bytes())
            .map_or(0, |peak| peak as u64)
    }
}

/// Whether the binary's allocator counts this thread's allocations, without which every window
/// measures nothing.
pub(super) fn installed() -> bool {
    let window = Window::open();
    drop(std::hint::black_box(vec![0_u8; 64]));
    window.close() >= 64
}
