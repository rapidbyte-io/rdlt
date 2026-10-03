//! How a simulation presses on the engine's memory budget, and what it checks of it: no run
//! reserves more than its budget, and none without faults waits out its deadline.

use rdlt_engine::Report;

use super::refusals::{Failure, Prediction, unexplained};
use crate::destination::Digest;
use crate::seed::Seed;

/// What a simulation left: a digest of what the destination holds at the end, which the same seed
/// always leaves alike, and how often its runs waited on the memory budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Checked {
    /// What the destination holds.
    pub digest: Digest,
    /// How many times a push or a piece being lowered waited for room in the budget.
    pub memory_waits: u64,
    /// How many times a cursor waited for a commit.
    pub cursor_waits: u64,
}

/// The code of a failure that waited out the memory budget's deadline: nothing but a fault
/// holds bytes that long.
const MEMORY_DEADLINE: &str = "memory_budget_wait_exceeded";

/// The code of a commit refused for leaving more state than an open may answer: the workload's
/// cursors are sized so its state stays well within it.
const STATE_BOUND: &str = "state_bytes_exceeded";

impl super::Simulation {
    /// Checks that none of the runs `reports` tell of reserved more than the budget, and counts
    /// their waits on it.
    pub(super) fn within_budget(&mut self, reports: &[Report], phase: usize) {
        let (seed, budget) = (self.seed, self.budget);
        for report in reports {
            assert!(
                report.peak_memory <= budget,
                "seed {seed}: phase {phase}: a run reserved {} bytes of a budget of {budget}",
                report.peak_memory
            );
            self.waits.0 += report.memory_waits;
            self.waits.1 += report.cursor_waits;
        }
    }
}

/// Checks that `failures`, of runs without faults, are all refusals `prediction` explains, and
/// that none ended on the memory budget's deadline.
pub(super) fn explained(failures: &[Failure], prediction: &Prediction, seed: Seed, phase: usize) {
    if let Some(waited) = failures
        .iter()
        .find(|failure| failure.text.contains(MEMORY_DEADLINE))
    {
        panic!(
            "seed {seed}: phase {phase}: a run without faults ended on the memory budget's \
             deadline: {}",
            waited.text
        );
    }
    if let Some(over) = failures
        .iter()
        .find(|failure| failure.text.contains(STATE_BOUND))
    {
        panic!(
            "seed {seed}: phase {phase}: the workload's cursors left more state than an open may \
             answer, which their sizes must not: {}",
            over.text
        );
    }
    if let Some(failure) = unexplained(failures, prediction) {
        panic!(
            "seed {seed}: phase {phase}: a run without faults failed with {}",
            failure.text
        );
    }
}
