//! An intruding pipeline: once a phase converges, a pipeline that created none of the
//! destination's tables loads into one, and must be refused as `table_owned`, changing no table.

use rdlt_connector::{PipelineId, TABLE_OWNED};
use rdlt_engine::{ErrorKind, Origin, PipelinePlan};

use super::scenario::{RUN_LIMIT, start};
use super::{Simulation, plan};
use crate::seed::Seed;

/// The pipeline that intrudes.
const INTRUDER: &str = "sim-x";

impl Simulation {
    /// Has a pipeline of its own load the first stream of the first pipeline, once that stream's
    /// table exists, where two pipelines share the destination and neither serves changes.
    pub(super) async fn intrude(&self, seed: Seed, phase: usize) {
        let (world, workload) = (&self.world, &self.world.workload);
        if !workload.features.shared || !world.changes.streams.is_empty() {
            return;
        }
        let owner = plan(workload, &self.relaxed, 0);
        let Some(stream) = owner.streams().first().cloned() else {
            return;
        };
        let table = stream.name().to_string();
        if !world.store.lock().has_table(&table) {
            return;
        }
        let id = PipelineId::parse(INTRUDER).expect("valid pipeline id");
        let intruder = PipelinePlan::new(id, vec![stream])
            .expect("the intruding plan is valid")
            .schema(workload.pipeline.engine());
        // A run that converged may have run with faults on; the intruder's must meet none.
        world.set_faulty(false);
        let before = world.store.lock().tables_digest();
        let run = start(&self.runs, &intruder, &self.name, self.placing.as_ref()).await;
        let (outcome, _, wrong) = run.ended(RUN_LIMIT).await;
        crate::canary::Canary::of(&self.name).kept(world, &outcome);
        for finding in wrong {
            world.violation(finding);
        }
        // The destination refuses the intruder, and the engine passes its refusal on.
        let refused = outcome.error.as_ref().is_some_and(|error| {
            (error.kind(), error.origin(), error.connector_code())
                == (ErrorKind::Config, Origin::Destination, Some(TABLE_OWNED))
        });
        assert!(
            refused,
            "seed {seed}: phase {phase}: a pipeline loading into {table}, which another pipeline \
             owns, ended with {:?}",
            outcome.error
        );
        assert_eq!(
            world.store.lock().tables_digest(),
            before,
            "seed {seed}: phase {phase}: a refused pipeline changed the destination's tables"
        );
    }
}
