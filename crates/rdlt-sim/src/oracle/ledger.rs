//! What a simulation's runs left as they ended: a trace of them, their waits on the memory budget,
//! how long the last that succeeded took, and the paths their commits and reports to the source
//! reached.

use std::hash::{DefaultHasher, Hash as _, Hasher as _};
use std::time::Duration;

use rdlt_engine::{RunStatus, Waits};

use super::pressure::added;
use super::scenario::{Executed, Scenario};
use crate::reached::Reach;
use crate::world::World;

/// What a simulation's runs left, folded in as each scenario's runs end.
#[derive(Debug, Default)]
pub(super) struct Ledger {
    /// Every report, failure and event of the phases' runs, in the order the runs ended, hashed:
    /// alike on every run of a seed.
    trace: DefaultHasher,
    /// How often and how long the runs waited for room in the budget, by share.
    pub(super) waits: Waits,
    /// How long the last run that succeeded took; none before one has.
    pub(super) length: Option<Duration>,
}

impl Ledger {
    /// Folds in the runs of `scenario` in `phase`, those of each pipeline in `executed`, and
    /// notes in `world` where a commit was sent again in place, where their reports to the source
    /// failed, leaving cursors owed, and where an owed cursor was given up.
    pub(super) fn ended(
        &mut self,
        phase: usize,
        scenario: Scenario,
        executed: &[Executed],
        world: &World,
    ) {
        phase.hash(&mut self.trace);
        format!("{scenario:?}").hash(&mut self.trace);
        for executed in executed {
            for report in &executed.reports {
                let reported = serde_json::to_string(report).expect("a report serializes");
                reported.hash(&mut self.trace);
                added(&mut self.waits, &report.counters.waits);
                if report.counters.acks.failed > 0 {
                    world.reach(Reach::ReportOwed);
                }
                if report.counters.acks.dropped > 0 {
                    world.reach(Reach::OwedDropped);
                }
                if report.counters.commits.retried > 0 {
                    world.reach(Reach::InPlaceRetried);
                }
                if report.status == RunStatus::Succeeded {
                    self.length = Some(report.elapsed);
                }
            }
            for failure in &executed.failures {
                failure.text.hash(&mut self.trace);
            }
            for events in &executed.events {
                events.hash(&mut self.trace);
            }
        }
    }

    /// The trace of every run folded in so far.
    pub(super) fn trace(&self) -> u64 {
        self.trace.finish()
    }
}
