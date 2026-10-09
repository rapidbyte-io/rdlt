//! How a simulation presses on the engine's memory budget, and what it checks of it: no run
//! reserves more than its budget, and none without faults waits out its deadline.

use std::time::Duration;

use rdlt_engine::{Report, Waited, Waits};
use serde_json::{Map, Value, json};

use super::refusals::{Failure, Prediction, unexplained};
use crate::destination::Digest;
use crate::seed::{Recorded, Seed};
use crate::swarm::Features;

/// What a simulation left: a digest of what the destination holds at the end, which the same seed
/// always leaves alike, and how often its runs waited on the memory budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Checked {
    /// What the destination holds.
    pub digest: Digest,
    /// How often and how long its runs waited for room in the budget, by share.
    pub waits: Waits,
    /// How long the simulation took on its own clock.
    pub simulated: Duration,
    /// The features its seed turned on.
    pub features: Features,
}

impl Recorded for Checked {
    fn recorded(&self) -> Map<String, Value> {
        let mut fields = self.digest.recorded();
        fields.insert(
            "simulated_ms".to_owned(),
            json!(self.simulated.as_secs_f64() * 1e3),
        );
        fields.insert("features".to_owned(), json!(self.features));
        fields.insert("waits".to_owned(), json!(self.waits));
        fields
    }
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
            added(&mut self.waits, &report.counters.waits);
        }
    }
}

/// `total` with the waits of `more` added, share by share.
fn added(total: &mut Waits, more: &Waits) {
    let shares = [
        (&mut total.intake, more.intake),
        (&mut total.work, more.work),
        (&mut total.cursors, more.cursors),
        (&mut total.log, more.log),
        (&mut total.tables, more.tables),
        (&mut total.control, more.control),
        (&mut total.reads, more.reads),
    ];
    for (total, more) in shares {
        *total = Waited {
            count: total.count + more.count,
            time: total.time + more.time,
        };
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
