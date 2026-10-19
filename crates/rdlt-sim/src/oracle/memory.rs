//! What a seed's thread holds on the heap over an attempt, against what its runs account for:
//! their reservations, a push a partition, and an allowance.

#[cfg(test)]
mod tests;

use alloc_tracker::{Session, ThreadSpan};
use serde_json::{Map, Value, json};

use super::scenario::Executed;
use crate::seed::Seed;

/// Bytes: what the thread of a seed whose connectors run in its process may hold beyond what
/// its runs account for: the runtime's, the world's copies of what it stores and builders'
/// spare room; ADR 0039 says how it was found.
const IN_PROCESS: u64 = 20 << 20;

/// Bytes: as [`IN_PROCESS`], where the connectors listen on the simulated network and their
/// hosts share the thread; the pushes they queue are in the bound, by [`QUEUED`].
const NETWORK: u64 = 19 << 20;

/// Pushes a connector serving a read over the network holds for each partition, beyond what
/// the engine admits: the events its source may send ahead of the frames waiting for credit, and
/// those frames.
const QUEUED: u64 = 32;

/// Bytes: the allowance of `seed`'s attempts, where its work is `inline` on one thread, and its
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

/// The heap a seed's thread held over its worst attempt beyond where the attempt began, and
/// what that attempt's runs account for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Held {
    /// Bytes: the most the thread held beyond where the attempt began.
    pub held: u64,
    /// Bytes: the runs' reserved peaks, a budget for each run dropped, and a push for each
    /// partition each run reads.
    pub bound: u64,
}

impl Held {
    /// Bytes: how far `held` passed `bound`, below zero where it stayed within it.
    fn over(self) -> i128 {
        i128::from(self.held) - i128::from(self.bound)
    }

    /// Adds what the worst attempt held, and what its runs account for, to a timing line.
    pub(super) fn record(self, fields: &mut Map<String, Value>) {
        fields.insert("held_bytes".to_owned(), json!(self.held));
        fields.insert("bound_bytes".to_owned(), json!(self.bound));
    }
}

impl super::Simulation {
    /// Checks that over an attempt of `phase` whose runs went as `executed` say, the thread held
    /// `held` bytes beyond where it began, no more than they account for and the allowance.
    pub(super) fn within_memory(&mut self, held: u64, executed: &[Executed], phase: usize) {
        let Some(allowance) = self.allowance else {
            return;
        };
        let reserved: u64 = executed
            .iter()
            .flat_map(|run| &run.reports)
            .map(|report| report.peak_memory)
            .sum();
        let crashed = executed.iter().filter(|run| run.crashed).count();
        let crashed = u64::try_from(crashed).unwrap_or(u64::MAX);
        let started: u64 = executed.iter().map(|run| run.started).sum();
        let held_ahead = if self.placing.is_some() {
            1 + QUEUED
        } else {
            1
        };
        let pushes = started
            .saturating_mul(self.partitions)
            .saturating_mul(held_ahead)
            .saturating_mul(self.world.largest_push());
        let bound = reserved
            .saturating_add(crashed.saturating_mul(self.budget))
            .saturating_add(pushes);
        let attempt = Held { held, bound };
        if self.held.is_none_or(|worst| attempt.over() > worst.over()) {
            self.held = Some(attempt);
        }
        let seed = self.seed;
        assert!(
            held <= bound.saturating_add(allowance),
            "seed {seed}: phase {phase}: an attempt's thread held {held} bytes of heap beyond \
             where it began, past the {bound} its runs account for and an allowance of \
             {allowance}"
        );
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
