//! The engine's reports of committed positions to a source that hears only what it sent or was
//! read from, checked once the workload has converged.

use rdlt_connector::{PartitionId, ReadMode, StreamName};
use rdlt_engine::{PipelinePlan, Until};

use super::scenario::{Executed, Scenario, execute_all};
use super::{Simulation, plan, settle};
use crate::destination::committed_next;
use crate::seed::Seed;

impl Simulation {
    /// Every pipeline's plan, read to its sources' ends.
    fn plans(&self) -> Vec<PipelinePlan> {
        let workload = &self.world.workload;
        (0..workload.pipelines)
            .map(|pipeline| plan(workload, &self.relaxed, pipeline).with_until(Until::Exhausted))
            .collect()
    }

    /// Runs every pipeline once more, without faults, against a source started again.
    async fn run_again(&self) -> Vec<Executed> {
        self.world.set_faulty(false);
        self.world.reports.restart();
        let placing = self.placing.as_ref();
        if let Some(placing) = placing {
            placing.restart_source().await;
        }
        execute_all(
            &self.engine,
            &self.plans(),
            &self.name,
            placing,
            Scenario::Plain,
        )
        .await
    }

    /// Checks, on a converged workload, that a run with nothing new takes one attempt and tells
    /// the source every committed position it read from, and that a run whose every report is
    /// refused fails.
    ///
    /// # Panics
    ///
    /// Panics, naming the seed, where a run does otherwise.
    pub(super) async fn check_reports(&self, seed: Seed) {
        for executed in self.run_again().await {
            let attempted: Vec<u64> = executed.reports.iter().map(|run| run.attempted).collect();
            assert!(
                executed.succeeded && attempted == [1],
                "seed {seed}: a run with nothing new took {attempted:?} attempts and failed with \
                 {:?}",
                executed.failures.last().map(|failure| &failure.text)
            );
        }
        settle(seed).await;
        for ((stream, partition), told) in self.world.reports.read() {
            let name = StreamName::new(&stream).expect("valid stream name");
            let id = PartitionId::parse(&partition).expect("valid partition id");
            // A partition that is done was told its last checkpoint, which state no longer holds.
            let committed =
                committed_next(&self.world, &name, &id).filter(|next| *next != u64::MAX);
            assert!(
                committed.is_none() || told == committed,
                "seed {seed}: stream {stream} partition {partition} is committed at \
                 {committed:?}, and its source was last told {told:?}"
            );
        }
        self.world.reports.refuse(true);
        let executed = self.run_again().await;
        let refused = self.world.reports.refuse(false);
        settle(seed).await;
        // An incremental read reports every partition state holds at a cursor in each attempt,
        // so its run fails. A completed full read and a done partition are not reported again.
        let incremental = |stream: &str| {
            let streams = &self.world.workload.streams;
            let found = streams.iter().find(|found| found.name == stream);
            found.is_some_and(|found| found.read == ReadMode::Incremental)
        };
        let reporting: Vec<String> = self
            .world
            .reports
            .read()
            .into_iter()
            .filter(|((stream, partition), _)| {
                incremental(stream) && self.stands(stream, partition).is_some()
            })
            .map(|((stream, _), _)| stream)
            .collect();
        for (plan, executed) in self.plans().iter().zip(executed) {
            let reports = plan
                .streams()
                .iter()
                .any(|stream| reporting.contains(&stream.name().to_string()));
            assert!(
                !(reports && executed.succeeded),
                "seed {seed}: a run succeeded, though its source refused {refused} reports, of \
                 streams {reporting:?} among them"
            );
            assert!(
                !reports || refused > 0,
                "seed {seed}: a run reported nothing of {reporting:?}"
            );
        }
    }

    /// Where state holds `partition` of `stream`, unless it is done or has no position.
    fn stands(&self, stream: &str, partition: &str) -> Option<u64> {
        let name = StreamName::new(stream).expect("valid stream name");
        let id = PartitionId::parse(partition).expect("valid partition id");
        committed_next(&self.world, &name, &id).filter(|next| *next != u64::MAX)
    }
}
