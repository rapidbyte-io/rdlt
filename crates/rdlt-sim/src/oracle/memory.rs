//! What a seed's thread holds on the heap over an attempt, against what its runs account for:
//! their reservations, the pushes held beyond them, and an allowance.

#[cfg(test)]
mod tests;

use alloc_tracker::{Session, ThreadSpan};
use serde_json::{Map, Value, json};

use super::scenario::Executed;
use crate::seed::Seed;

/// Bytes: what the thread of a seed whose connectors run in its process, and which keeps no
/// write-ahead log, may hold beyond what its runs account for: the runtime's, the world's copies
/// of what it stores and builders' spare room; ADR 0039 says how it was found.
///
/// Fat values do not raise it: the data they add is smaller than this fixed overhead.
const IN_PROCESS: u64 = 11 << 20;

/// Bytes: as [`IN_PROCESS`], for a seed that keeps a write-ahead log, whose world holds copies of
/// the log's frames as well.
const IN_PROCESS_LOGGED: u64 = 20 << 20;

/// Bytes: as [`IN_PROCESS`], where the connectors listen on the simulated network and their
/// hosts share the thread; the pushes they queue are in the bound, by [`QUEUED`].
const NETWORK: u64 = 16 << 20;

/// Bytes: as [`NETWORK`], for a seed that keeps a write-ahead log.
const NETWORK_LOGGED: u64 = 19 << 20;

/// Pushes a connector serving a read over the network may hold for each partition, beyond what
/// the engine admits, each place holding at most one.
///
/// They are the sixteen events its source may send ahead, a push the source waits to send, the
/// batch being cut and its piece waiting for credit, and the frames that credit lets go before
/// the host takes them: a window of two of the largest frames and one sent past it. Where the
/// pushes are smaller than half the credit floor, the window is the floor's, and the allowance
/// holds it.
const QUEUED: u64 = 16 + 1 + 2 + 3;

/// Bytes: the allowance of `seed`'s attempts, where its work is `inline` on one thread, its
/// connectors listen on the network where `network`, and it keeps a write-ahead log where
/// `logged`; `None` where nothing is measured.
///
/// # Panics
///
/// Panics where the binary's allocator counts nothing.
pub(super) fn allowance(seed: Seed, inline: bool, network: bool, logged: bool) -> Option<u64> {
    if !inline {
        return None;
    }
    assert!(
        installed(),
        "seed {seed}: the test binary counts no allocations: it needs alloc_tracker's Allocator \
         as its global allocator"
    );
    Some(match (network, logged) {
        (false, false) => IN_PROCESS,
        (false, true) => IN_PROCESS_LOGGED,
        (true, false) => NETWORK,
        (true, true) => NETWORK_LOGGED,
    })
}

/// The heap a seed's thread held over its worst attempt beyond where the attempt began, and
/// what that attempt's runs account for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Held {
    /// Bytes: the most the thread held beyond where the attempt began.
    pub held: u64,
    /// Bytes: the runs' reserved peaks, a budget for each run dropped, and the pushes each
    /// partition each run reads holds beyond them, one in process and more over the network.
    pub bound: u64,
}

impl Held {
    /// Bytes: how far `held` passed `bound`, below zero where it stayed within it.
    fn over(self) -> i128 {
        i128::from(self.held) - i128::from(self.bound)
    }

    /// The worse of the worst attempt so far, if any, and `attempt`: whichever
    /// passed its bound by more.
    fn worse(worst: Option<Self>, attempt: Self) -> Self {
        match worst {
            Some(worst) if worst.over() >= attempt.over() => worst,
            _ => attempt,
        }
    }

    /// Adds what the worst attempt held, and what its runs account for, to a timing line.
    pub(super) fn record(self, fields: &mut Map<String, Value>) {
        fields.insert("held_bytes".to_owned(), json!(self.held));
        fields.insert("bound_bytes".to_owned(), json!(self.bound));
    }

    /// Checks that over `phase` of `seed` the thread held no more than its runs account for and
    /// `allowance`.
    ///
    /// # Panics
    ///
    /// Panics, naming the seed and phase, where it held more.
    fn within(self, seed: Seed, phase: usize, allowance: u64) {
        let Self { held, bound } = self;
        assert!(
            held <= bound.saturating_add(allowance),
            "seed {seed}: phase {phase}: an attempt's thread held {held} bytes of heap beyond \
             where it began, past the {bound} its runs account for and an allowance of \
             {allowance}"
        );
    }
}

/// What the runs of an attempt reserve and hold beyond what they reserve.
#[derive(Clone, Copy, Debug)]
struct Account {
    /// Bytes: the peaks the runs' reports tell of.
    reserved: u64,
    /// How many runs were dropped as if their worker crashed, leaving no report.
    crashed: u64,
    /// How many runs were started.
    started: u64,
    /// Bytes: the memory budget of a run.
    budget: u64,
    /// Partitions each run reads at once.
    partitions: u64,
    /// Pushes each partition holds at once: one, and over the network those a connector queues.
    pushes: u64,
    /// Bytes: the largest push any source made.
    push: u64,
}

impl Account {
    /// Bytes: what the runs account for.
    fn bound(self) -> u64 {
        let held = self
            .started
            .saturating_mul(self.partitions)
            .saturating_mul(self.pushes)
            .saturating_mul(self.push);
        self.reserved
            .saturating_add(self.crashed.saturating_mul(self.budget))
            .saturating_add(held)
    }
}

impl super::Simulation {
    /// Checks that over an attempt of `phase` whose runs went as `executed` say, the thread held
    /// `held` bytes beyond where it began, no more than they account for and the allowance.
    pub(super) fn within_memory(&mut self, held: u64, executed: &[Executed], phase: usize) {
        let Some(allowance) = self.allowance else {
            return;
        };
        let reserved = executed
            .iter()
            .flat_map(|run| &run.reports)
            .map(|report| report.peak_memory)
            .sum();
        let crashed = executed.iter().filter(|run| run.crashed).count();
        let account = Account {
            reserved,
            crashed: u64::try_from(crashed).unwrap_or(u64::MAX),
            started: executed.iter().map(|run| run.started).sum(),
            budget: self.budget,
            partitions: self.partitions,
            pushes: if self.placing.is_some() {
                1 + QUEUED
            } else {
                1
            },
            push: self.world.largest_push(),
        };
        let attempt = Held {
            held,
            bound: account.bound(),
        };
        self.held = Some(Held::worse(self.held, attempt));
        attempt.within(self.seed, phase, allowance);
    }
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
