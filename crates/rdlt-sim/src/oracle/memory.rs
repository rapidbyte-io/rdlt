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
const IN_PROCESS: u64 = 13 << 20;

/// Bytes: as [`IN_PROCESS`], for a seed that keeps a write-ahead log, whose world holds copies of
/// the log's frames as well.
const IN_PROCESS_LOGGED: u64 = 22 << 20;

/// Bytes: as [`IN_PROCESS`], where the connectors listen on the simulated network and their
/// hosts share the thread; what their reads hold between the source and the engine is in the
/// bound, by [`QUEUED`] and the credit floor.
const NETWORK: u64 = 11 << 20;

/// Bytes: as [`NETWORK`], for a seed that keeps a write-ahead log.
const NETWORK_LOGGED: u64 = 17 << 20;

/// Pushes a read over the network may hold beyond the push its source is sending, each place
/// holding at most one: the host's push waiting for the engine to admit it, the sixteen events
/// the connector may send ahead, the batch being cut and its piece waiting for credit, and the
/// frame sent past the credit left.
///
/// The frames credit lets go before the host takes them are counted apart: a window of two of
/// the largest frames, or the credit floor where it is larger.
const QUEUED: u64 = 1 + 16 + 2 + 1;

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

/// Bytes: what `reads` reads over the network hold beyond the pushes their source is sending,
/// where the largest push is `push` and each read is granted credit of at least `floor`.
fn queued(reads: u64, push: u64, floor: u64) -> u64 {
    let window = push.saturating_mul(2).max(floor);
    reads.saturating_mul(QUEUED.saturating_mul(push).saturating_add(window))
}

/// The heap a seed's thread held over its worst attempt beyond where the attempt began, and
/// what that attempt's runs account for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Held {
    /// Bytes: the most the thread held beyond where the attempt began.
    pub held: u64,
    /// Bytes: the peaks the runs' reports tell of.
    pub reserved: u64,
    /// Bytes: a budget for each run dropped as if its worker crashed, leaving no report.
    pub dropped: u64,
    /// Bytes: the most the source held at once of pushes it made and had not yet sent on.
    pub pushes: u64,
    /// Bytes: over the network, what the reads hold between the source and the engine; none in
    /// process.
    pub queued: u64,
}

impl Held {
    /// Bytes: what the runs account for.
    pub fn bound(self) -> u64 {
        self.reserved
            .saturating_add(self.dropped)
            .saturating_add(self.pushes)
            .saturating_add(self.queued)
    }

    /// Bytes: how far `held` passed the bound, below zero where it stayed within it.
    fn over(self) -> i128 {
        i128::from(self.held) - i128::from(self.bound())
    }

    /// The worse of the worst attempt so far, if any, and `attempt`: whichever
    /// passed its bound by more.
    fn worse(worst: Option<Self>, attempt: Self) -> Self {
        match worst {
            Some(worst) if worst.over() >= attempt.over() => worst,
            _ => attempt,
        }
    }

    /// Adds what the worst attempt held, and what its runs account for term by term, to a timing
    /// line.
    pub(super) fn record(self, fields: &mut Map<String, Value>) {
        let terms = [
            ("held_bytes", self.held),
            ("bound_bytes", self.bound()),
            ("reserved_bytes", self.reserved),
            ("dropped_bytes", self.dropped),
            ("pushes_bytes", self.pushes),
            ("queued_bytes", self.queued),
        ];
        for (name, bytes) in terms {
            fields.insert(name.to_owned(), json!(bytes));
        }
    }

    /// Checks that over `phase` of `seed` the thread held no more than its runs account for and
    /// `allowance`.
    ///
    /// # Panics
    ///
    /// Panics, naming the seed and phase, where it held more.
    fn within(self, seed: Seed, phase: usize, allowance: u64) {
        let (held, bound) = (self.held, self.bound());
        assert!(
            held <= bound.saturating_add(allowance),
            "seed {seed}: phase {phase}: an attempt's thread held {held} bytes of heap beyond \
             where it began, past the {bound} its runs account for and an allowance of \
             {allowance}: {self:?}"
        );
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
        let started: u64 = executed.iter().map(|run| run.started).sum();
        let pushes = &self.world.pushes;
        let queued = self.placing.as_ref().map_or(0, |placing| {
            let reads = started.saturating_mul(self.partitions);
            queued(reads, pushes.largest(), placing.read_floor())
        });
        let attempt = Held {
            held,
            reserved,
            dropped: u64::try_from(crashed)
                .unwrap_or(u64::MAX)
                .saturating_mul(self.budget),
            pushes: pushes.most(),
            queued,
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
