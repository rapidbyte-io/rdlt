//! What a seed's thread holds on the heap at each moment of an attempt beyond what is charged for
//! it then: what the engine's budgets hold reserved and the source holds of pushes not yet sent
//! on; over the network, what the reads hold between them; and an allowance.

#[cfg(test)]
mod tests;

use std::backtrace::Backtrace;
use std::cell::{Cell, RefCell};
use std::marker::PhantomData;

use alloc_tracker::{Operation, Session, ThreadSpan};
use serde_json::{Map, Value, json};

use super::scenario::Executed;
use crate::seed::Seed;

/// Bytes: what the thread of a seed whose connectors run in its process may hold beyond what is
/// charged for it, whether or not it keeps a write-ahead log: the runtime's, the world's copies of
/// what it stores and builders' spare room; ADR 0039 says how it was found.
///
/// Fat values do not raise it: the data they add is smaller than this fixed overhead.
const IN_PROCESS: u64 = 13 << 20;

/// Bytes: as [`IN_PROCESS`], where the connectors listen on the simulated network and their
/// hosts share the thread; what their reads hold between the source and the engine is counted
/// beside it, by [`QUEUED`] and the credit floor.
const NETWORK: u64 = 11 << 20;

/// Pushes a read over the network may hold beyond the push its source is sending, each place
/// holding at most one: the host's push waiting for the engine to admit it, the sixteen events
/// the connector may send ahead, the batch being cut and its piece waiting for credit, and the
/// frame sent past the credit left.
///
/// The frames credit lets go before the host takes them are counted apart: a window of two of
/// the largest frames, or the credit floor where it is larger.
const QUEUED: u64 = 1 + 16 + 2 + 1;

/// Bytes: the allowance of `seed`'s attempts, where its work is `inline` on one thread and its
/// connectors listen on the network where `network`; `None` where nothing is measured.
///
/// # Panics
///
/// Panics where the binary's allocator counts nothing.
pub(super) fn allowance(seed: Seed, inline: bool, network: bool) -> Option<u64> {
    if !inline {
        return None;
    }
    assert!(
        installed(),
        "seed {seed}: the test binary counts no allocations: it needs alloc_tracker's Allocator \
         as its global allocator"
    );
    Some(if network { NETWORK } else { IN_PROCESS })
}

/// Bytes: what `reads` reads over the network hold beyond the pushes their source is sending,
/// where the largest push is `push` and each read is granted credit of at least `floor`.
fn queued(reads: u64, push: u64, floor: u64) -> u64 {
    let window = push.saturating_mul(2).max(floor);
    reads.saturating_mul(QUEUED.saturating_mul(push).saturating_add(window))
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
            queued(reads, self.world.pushes.largest(), placing.read_floor())
        });
        let attempt = Held { held, queued };
        self.held = Some(Held::worse(self.held, attempt));
        attempt.within(seed, phase, allowance, &lifted.rises);
    }

    /// Bytes: room enough for what an attempt of `plans` may charge beyond what was charged as it
    /// began: every run's budget, as each pipeline starts a few runs, and what the source holds.
    pub(super) fn room(&self, plans: usize) -> u64 {
        let runs = u64::try_from(plans)
            .unwrap_or(u64::MAX)
            .saturating_mul(RUNS);
        self.budget
            .saturating_mul(runs)
            .saturating_add(PUSHES)
            .saturating_add(ASIDE)
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
pub(super) fn charged() -> i64 {
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

/// The variable that, set, traces each measured attempt, so an attempt that held too much names
/// the change of the charge after which it did.
///
/// At every change of what is charged, the trace reads the most the thread held beyond its charge
/// so far. Its spans and backtraces make it slow: it is for replaying a seed.
pub const TRACE_VAR: &str = "RDLT_SIM_TRACE";

/// Changes of the charge a traced attempt reads the heap at; it reads none after them.
const TRACED: usize = 1 << 14;

/// Bytes: the steps of the most held beyond the charge whose crossing the trace keeps.
const RISE: u64 = 1 << 20;

/// Sessions to trace an attempt with, each for one span, made before the attempt's window opens
/// so that making them is not measured.
pub(super) struct Tracing {
    sessions: Vec<(Session, Operation)>,
    /// Room for the sessions once read, kept until the attempt ends so that freeing them is
    /// not measured either.
    read: Vec<Session>,
}

impl Tracing {
    /// Sessions for a trace where [`TRACE_VAR`] is set; none otherwise.
    pub(super) fn ready() -> Option<Self> {
        std::env::var_os(TRACE_VAR).filter(|set| !set.is_empty())?;
        // What a first backtrace keeps for every later one is kept before the window opens.
        drop(Backtrace::force_capture().to_string());
        let sessions = (0..TRACED)
            .map(|_| {
                let session = Session::new().no_stdout().no_file();
                let operation = session.operation("change");
                (session, operation)
            })
            .collect();
        Some(Self {
            sessions,
            read: Vec::with_capacity(TRACED),
        })
    }

    /// Starts tracing the calling thread from what it holds now, `offset` bytes more charged
    /// than as its seed began.
    fn start(self, offset: i64) -> Trace {
        let spans = self
            .sessions
            .into_iter()
            .map(|(session, operation)| {
                let span = operation.measure_thread().iterations(1);
                (session, span)
            })
            .collect();
        Trace {
            spans,
            read: self.read,
            offset,
            changes: 0,
            most: 0,
            before: Backtrace::force_capture(),
            rises: Vec::new(),
        }
    }
}

/// An attempt traced: the spans it reads the heap with, and where the most held rose.
struct Trace {
    /// Spans opened as the attempt began, the innermost last: closing one gives the most the
    /// thread held beyond what it held then.
    spans: Vec<(Session, ThreadSpan)>,
    /// The sessions of the spans closed.
    read: Vec<Session>,
    /// Bytes: what was charged as the spans opened beyond what was charged as the seed began.
    offset: i64,
    /// Changes of the charge read so far.
    changes: u64,
    /// Bytes: the most held beyond the charge as of the last change read.
    most: u64,
    /// Where the change before the last was made.
    before: Backtrace,
    rises: Vec<Rise>,
}

impl Trace {
    /// Reads the most held beyond the charge at a change of `change` bytes, or at the attempt's
    /// end where `change` is none, keeping the change where the most rose past another [`RISE`].
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "a peak is a whole, non-negative number of bytes far below 2^53"
    )]
    fn read(&mut self, change: Option<i64>) {
        let Some((session, span)) = self.spans.pop() else {
            return;
        };
        drop(span);
        let peak = session
            .to_report()
            .operations()
            .find_map(|(_, operation)| operation.peak_outstanding_bytes())
            .map_or(0, |peak| peak as u64);
        self.read.push(session);
        let most = u64::try_from(i128::from(peak) - i128::from(self.offset)).unwrap_or(0);
        self.changes += 1;
        let at = Backtrace::force_capture();
        if most / RISE > self.most / RISE {
            let before = std::mem::replace(&mut self.before, Backtrace::force_capture());
            self.rises.push(Rise {
                change: self.changes,
                by: change,
                most,
                before,
                at,
            });
        } else {
            self.before = at;
        }
        self.most = self.most.max(most);
    }

    /// Reads the most held as the attempt ends, closes the spans left, innermost first, and
    /// gives the rises.
    fn end(mut self) -> Vec<Rise> {
        self.read(None);
        while let Some(span) = self.spans.pop() {
            drop(span);
        }
        self.rises
    }
}

/// A change of the charge after which the most a traced attempt's thread held beyond its charge
/// rose.
#[derive(Debug)]
pub(super) struct Rise {
    /// Which change of the attempt it was, from one.
    change: u64,
    /// Bytes: what it changed the charge by, fewer where negative; none for the attempt's end.
    by: Option<i64>,
    /// Bytes: the most held beyond the charge once it was made.
    most: u64,
    /// Where the change before it was made.
    before: Backtrace,
    /// Where it was made.
    at: Backtrace,
}

impl Rise {
    /// The rise as a failure names it: the change, and the frames of the workspace's code that
    /// made it and the change before it.
    fn named(&self) -> String {
        let Some(by) = self.by else {
            return format!(
                "it held {} bytes beyond its charge by the attempt's end, after the change of the \
                 charge made at\n{}",
                self.most,
                frames(&self.before)
            );
        };
        format!(
            "it held {} bytes beyond its charge once change {} of the charge, by {by} bytes, was \
             made at\n{}\nafter the change made at\n{}",
            self.most,
            self.change,
            frames(&self.at),
            frames(&self.before)
        )
    }
}

/// The frames of `backtrace` in the workspace's code, past the budget and this check, the
/// innermost first.
fn frames(backtrace: &Backtrace) -> String {
    let text = backtrace.to_string();
    let mut lines = text.lines().peekable();
    let mut frames = Vec::new();
    while let Some(line) = lines.next() {
        let Some((_, name)) = line.trim_start().split_once(": ") else {
            continue;
        };
        let place = lines
            .next_if(|next| next.trim_start().starts_with("at "))
            .map(|next| next.trim_start().trim_start_matches("at ").to_owned());
        let ours = ["rdlt_", "<rdlt_", "core::ptr::drop_glue::<rdlt_"]
            .iter()
            .any(|prefix| name.starts_with(prefix));
        let skipped = ["oracle::memory", "budget::", "SimEnv"]
            .iter()
            .any(|skip| name.contains(skip));
        if ours && !skipped {
            frames.push(match place {
                Some(place) => format!("  {name} at {place}"),
                None => format!("  {name}"),
            });
        }
    }
    frames.truncate(8);
    frames.join("\n")
}

/// A stretch of the calling thread's work whose heap peak is measured: the most the thread held
/// beyond what it held as the stretch began.
pub(super) struct Window {
    session: Session,
    span: Option<ThreadSpan>,
}

impl Window {
    /// Starts measuring the calling thread's heap, from what it holds now.
    pub(super) fn open() -> Self {
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
    pub(super) fn close(mut self) -> u64 {
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
