//! How a simulation presses on the engine's memory budget, and what it checks of it: no run
//! reserves more than its budget, and none without faults waits out its deadline.

use std::time::Duration;

use rdlt_engine::{Report, Waited, Waits, codes};
use serde_json::{Map, Value, json};

use super::Held;
use super::refusals::{Failure, Prediction, unexplained};
use crate::destination::Digest;
use crate::reached::Reached;
use crate::seed::{Recorded, Seed};
use crate::swarm::Features;

/// What a simulation left: a digest of what the destination holds at the end, which the same seed
/// always leaves alike, and how often its runs waited on the memory budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Checked {
    /// What the destination holds.
    pub digest: Digest,
    /// Every report and failure of the phases' runs, in the order the runs ended, hashed: the same
    /// seed always comes to the same trace.
    pub trace: u64,
    /// How often and how long its runs waited for room in the budget, by share.
    pub waits: Waits,
    /// How long the simulation took on its own clock.
    pub simulated: Duration,
    /// The features its seed turned on.
    pub features: Features,
    /// What the thread held over the worst attempt beyond what was charged for it, and what its
    /// reads hold over the network; `None` where nothing was measured: the runs' work was on more
    /// than one thread, the seed's host was hostile, or the seed changed rows.
    pub held: Option<Held>,
    /// The paths its runs reached, which a seed kept for a defect names.
    pub reached: Reached,
}

impl Recorded for Checked {
    fn recorded(&self) -> Map<String, Value> {
        let mut fields = self.digest.recorded();
        fields.insert("trace".to_owned(), json!(self.trace));
        fields.insert(
            "simulated_ms".to_owned(),
            json!(self.simulated.as_secs_f64() * 1e3),
        );
        fields.insert("features".to_owned(), json!(self.features));
        fields.insert("waits".to_owned(), json!(self.waits));
        fields.insert("reached".to_owned(), json!(self.reached.names()));
        if let Some(held) = self.held {
            held.record(&mut fields);
        }
        fields
    }
}

impl super::Simulation {
    /// Checks that none of the runs `reports` tell of reserved more than the budget.
    pub(super) fn within_budget(&self, reports: &[Report], phase: usize) {
        let (seed, budget) = (self.seed, self.budget);
        for report in reports {
            assert!(
                report.peak_memory <= budget,
                "seed {seed}: phase {phase}: a run reserved {} bytes of a budget of {budget}",
                report.peak_memory
            );
        }
    }
}

/// `total` with the waits of `more` added, share by share.
pub(super) fn added(total: &mut Waits, more: &Waits) {
    let shares = [
        (&mut total.intake, more.intake),
        (&mut total.decode, more.decode),
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
/// that none ended on the memory budget's deadline, which nothing but a fault holds bytes for, or
/// left more state than an open may answer, which the workload's cursors are sized to stay within.
pub(super) fn explained(failures: &[Failure], prediction: &Prediction, seed: Seed, phase: usize) {
    if let Some(waited) = failures
        .iter()
        .find(|failure| failure.is(codes::MEMORY_BUDGET_WAIT_EXCEEDED))
    {
        panic!(
            "seed {seed}: phase {phase}: a run without faults ended on the memory budget's \
             deadline: {}",
            waited.text
        );
    }
    if let Some(over) = failures
        .iter()
        .find(|failure| failure.is(codes::STATE_BYTES_EXCEEDED))
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
