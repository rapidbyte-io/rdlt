//! A simulation's attempts: every pipeline's runs of one scenario at once, held to the memory
//! budget and the protocol, their failures checked against the refusals the model predicts.

use rdlt_connector::{ColumnPath, PipelineId, StreamName};
use rdlt_engine::{PipelinePlan, Report, StreamPlan, Until, codes};

use super::memory::{Counterweight, Tracing};
use super::pressure::explained;
use super::scenario::{Executed, Scenario, execute_all};
use super::{Simulation, Window, refusals};
use crate::placing::Placing;
use crate::reached::Reach;
use crate::rng::SplitMix64;
use crate::seed::Seed;
use crate::swarm::{FRAMING, HOSTILE};
use crate::workload::{Level, Relaxed, Workload};

impl Simulation {
    /// Runs `scenario` in `phase` for every pipeline at once, keeping the reports of runs that
    /// end, and checks their failures against the refusals the model predicts.
    ///
    /// A refusal an operator can relax is relaxed, and a `clean` run, with neither faults nor
    /// disruptions, fails with nothing else. With `network` faults, drawn from it, the network is
    /// disrupted while the runs last, and healed once they end.
    pub(super) async fn attempt(
        &mut self,
        phase: usize,
        scenario: Scenario,
        clean: bool,
        network: Option<SplitMix64>,
        reports: &mut Vec<Report>,
    ) -> Ran {
        self.attempt_until(phase, scenario, clean, network, reports, Until::Exhausted)
            .await
    }

    /// Runs `scenario` for `plans`, the thread's heap over it held to what is charged for it.
    async fn measured(
        &mut self,
        plans: &[PipelinePlan],
        scenario: Scenario,
        clean: bool,
        network: Option<SplitMix64>,
        phase: usize,
    ) -> Vec<Executed> {
        // The largest event is counted afresh for each attempt.
        self.world.pushes.restart();
        let room = self.room(plans.len());
        // The window opens before the counterweight hangs, so it measures from where the thread
        // would stand had nothing been charged since the seed began.
        let tracing = self.allowance.and_then(|_| Tracing::ready());
        let measuring = self.allowance.map(|_| {
            let window = Window::open();
            (window, Counterweight::hang(self.origin, room, tracing))
        });
        self.attacks += 1;
        let attacking = SplitMix64::new(self.seed.value() ^ HOSTILE ^ self.attacks);
        let placing = self.placing.as_ref();
        let executing = execute_all(&self.runs, plans, &self.name, placing, scenario);
        let hostile = placing
            .and_then(Placing::networked)
            .filter(|_| self.world.workload.features.hostile);
        let (world, name) = (&self.world, self.name.as_str());
        let breaking = SplitMix64::new(self.seed.value() ^ FRAMING ^ self.attacks);
        let framing = placing
            .and_then(Placing::networked)
            .filter(|_| self.world.workload.features.framing);
        // A broken connector is met beside the runs; where the run is clean, the network is healed
        // and its call is judged in full.
        let strict = clean;
        let executing = async {
            match framing {
                Some(networked) => {
                    Box::pin(networked.breaking(breaking, world, strict, executing)).await
                }
                None => executing.await,
            }
        };
        // A hostile host attacks the connectors beside the runs; where the run is clean, the
        // network is healed and its answers are judged.
        let attacked = async {
            match hostile {
                Some(networked) => {
                    Box::pin(networked.attacking(attacking, world, name, clean, executing)).await
                }
                None => executing.await,
            }
        };
        let executed = match (placing, network) {
            (Some(placing), Some(rng)) => Box::pin(placing.disrupting(rng, attacked)).await,
            _ => attacked.await,
        };
        if let Some((window, weight)) = measuring {
            let lifted = weight.lift();
            let held = window.close().saturating_sub(room);
            self.within_memory(held, &lifted, &executed, phase);
        }
        executed
    }

    /// Runs `scenario` as [`attempt`](Self::attempt) does, each run reading as `until` says.
    pub(super) async fn attempt_until(
        &mut self,
        phase: usize,
        scenario: Scenario,
        clean: bool,
        network: Option<SplitMix64>,
        reports: &mut Vec<Report>,
        until: Until,
    ) -> Ran {
        let seed = self.seed;
        // Each run meets a source started for it, as a spawned connector is: it remembers
        // nothing an earlier run was sent.
        self.world.reports.restart();
        // Each run is held to its own connections' plans: those made before it are forgotten.
        drop(self.world.connections());
        let mut prediction = refusals::predict(&self.world, &self.relaxed, phase);
        if until.follows() {
            // A run that follows the source may end before the rows a refusal needs arrive.
            if !prediction.must.is_empty() {
                self.world.reach(Reach::Spared);
            }
            prediction.must.clear();
        }
        let workload = &self.world.workload;
        let plans: Vec<PipelinePlan> = (0..workload.pipelines)
            .map(|pipeline| plan(workload, &self.relaxed, pipeline).with_until(until))
            .collect();
        let executed = self.measured(&plans, scenario, clean, network, phase).await;
        self.ledger.ended(phase, scenario, &executed, &self.world);
        self.held_to_plans(scenario, &executed);
        let mut failures = Vec::new();
        for (plan, executed) in plans.iter().zip(&executed) {
            let refused = plan
                .streams()
                .iter()
                .find(|stream| prediction.must.contains(&stream.name().to_string()));
            assert!(
                !(executed.succeeded && refused.is_some()),
                "seed {seed}: phase {phase}: a run succeeded, though every run of {:?} must meet \
                 one of {:?}",
                refused.map(StreamPlan::name),
                prediction.may
            );
        }
        let succeeded = executed.iter().map(|executed| executed.succeeded).collect();
        for executed in executed {
            failures.extend(executed.failures);
            self.within_budget(&executed.reports, phase);
            reports.extend(executed.reports);
        }
        kept_to_the_protocol(&failures, seed, phase);
        let refused = refusals::refused(&failures, &prediction)
            .unwrap_or_else(|finding| panic!("seed {seed}: phase {phase}: {finding}"));
        if clean {
            explained(&failures, &prediction, seed, phase);
        }
        let failure = failures.into_iter().last().map(|failure| failure.text);
        let mut stopped = false;
        if let Some((stream, code)) = refused {
            self.world.reach(Reach::Refused);
            if code == codes::MERGE_KEY_CHANGED {
                stopped = true;
            } else {
                refusals::relax(&self.world, &mut self.relaxed, &stream, code);
            }
        }
        Ran {
            succeeded,
            failure,
            stopped,
        }
    }
}

/// Checks that none of `failures` breaks the wire protocol, which no fault excuses.
pub(super) fn kept_to_the_protocol(failures: &[refusals::Failure], seed: Seed, phase: usize) {
    if let Some(broken) = refusals::violation(failures) {
        panic!(
            "seed {seed}: phase {phase}: a run broke the wire protocol: {}",
            broken.text
        );
    }
}

/// How one scenario's runs went.
pub(super) struct Ran {
    /// Whether one succeeded, for each pipeline.
    pub(super) succeeded: Vec<bool>,
    /// The last failure among them, printed.
    pub(super) failure: Option<String>,
    /// Whether one met a refusal no operator can relax.
    pub(super) stopped: bool,
}

/// The identifiers of the pipelines sharing the destination.
pub(super) const PIPELINES: [&str; 2] = ["sim", "sim-b"];

/// The plan of `workload`'s pipeline `pipeline`, of the streams it loads, each stream's settings
/// as `relaxed` leaves them.
pub(super) fn plan(workload: &Workload, relaxed: &[Relaxed], pipeline: usize) -> PipelinePlan {
    let id = PipelineId::parse(PIPELINES[pipeline]).expect("valid pipeline id");
    let streams = workload
        .streams
        .iter()
        .zip(relaxed)
        .enumerate()
        .filter(|(index, _)| index % workload.pipelines == pipeline)
        .map(|(_, (stream, relaxed))| {
            let name = StreamName::new(&stream.name).expect("valid stream name");
            let settings = stream.schema.relaxing(stream.pipeline, *relaxed);
            let mut plan = StreamPlan::new(name)
                .read(stream.read)
                .write(stream.write)
                .schema(settings.engine());
            for drift in &stream.drift {
                let column = ColumnPath::from(drift.name.as_str());
                if drift.settings != Level::default() {
                    plan = plan.column(column.clone(), drift.settings.relaxed(*relaxed).engine());
                }
                if let Some(hint) = &drift.hint {
                    plan = plan.hint(column, hint.clone());
                }
            }
            let excluded = stream.excluded.columns.iter();
            plan = plan.exclude(excluded.map(|excluded| excluded.name.as_str()));
            if stream.keys > 0 && stream.plan_key {
                plan.key(stream.key_columns())
            } else {
                plan
            }
        });
    PipelinePlan::new(id, streams)
        .expect("the simulated plan is valid")
        .schema(workload.pipeline.engine())
}
