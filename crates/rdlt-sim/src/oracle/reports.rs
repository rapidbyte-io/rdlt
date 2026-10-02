//! The engine's reports of committed positions to a source that hears only what it sent or was
//! read from, checked once a workload has converged.

use rdlt_connector::{PartitionId, ReadMode, StreamName};
use rdlt_engine::{Engine, PipelinePlan, Until};

use super::scenario::{Executed, Scenario, execute_all};
use super::{Simulation, plan, settle};
use crate::destination::committed_next;
use crate::network::Placing;
use crate::seed::Seed;
use crate::world::World;

/// A converged workload whose reports are checked: its pipelines, and what its state holds.
pub(super) struct Reported<'a> {
    pub(super) engine: &'a Engine,
    pub(super) plans: Vec<PipelinePlan>,
    pub(super) world: &'a World,
    pub(super) name: &'a str,
    pub(super) placing: Option<&'a Placing>,
    /// Where state holds a partition of a stream at a cursor, by their names; none where it
    /// is done or has no position.
    pub(super) stands: fn(&World, &str, &str) -> Option<u64>,
    /// Whether every attempt reads the stream's partitions again from where state holds them,
    /// and so reports them: a full read that completed is not read again.
    pub(super) repeats: fn(&World, &str) -> bool,
    /// Whether a run whose reports are all refused is tried: not where a source sends rows
    /// again whenever it resumes, since every attempt then lands rows and none is the last.
    pub(super) refusable: bool,
}

impl Reported<'_> {
    /// Runs every pipeline once more, without faults, against a source started again.
    async fn run_again(&self) -> Vec<Executed> {
        self.world.set_faulty(false);
        self.world.reports.restart();
        if let Some(placing) = self.placing {
            placing.restart_source().await;
        }
        let (engine, placing) = (self.engine, self.placing);
        execute_all(engine, &self.plans, self.name, placing, Scenario::Plain).await
    }

    /// Checks that a run with nothing new takes one attempt and tells the source every
    /// committed position it read from, and that a run whose every report is refused fails.
    ///
    /// # Panics
    ///
    /// Panics, naming the seed, where a run does otherwise.
    pub(super) async fn check(&self, seed: Seed) {
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
            let committed = (self.stands)(self.world, &stream, &partition);
            assert!(
                committed.is_none() || told == committed,
                "seed {seed}: stream {stream} partition {partition} is committed at \
                 {committed:?}, and its source was last told {told:?}"
            );
        }
        if !self.refusable {
            return;
        }
        self.world.reports.refuse(true);
        let executed = self.run_again().await;
        let refused = self.world.reports.refuse(false);
        settle(seed).await;
        // A stream every attempt reads again reports each partition state holds at a cursor,
        // so its run fails.
        let reporting: Vec<String> = self
            .world
            .reports
            .read()
            .into_iter()
            .filter(|((stream, partition), _)| {
                (self.repeats)(self.world, stream)
                    && (self.stands)(self.world, stream, partition).is_some()
            })
            .map(|((stream, _), _)| stream)
            .collect();
        for (plan, executed) in self.plans.iter().zip(executed) {
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
}

/// Where state holds `partition` of `stream`, unless it is done or has no position.
fn stands(world: &World, stream: &str, partition: &str) -> Option<u64> {
    let name = StreamName::new(stream).expect("valid stream name");
    let id = PartitionId::parse(partition).expect("valid partition id");
    committed_next(world, &name, &id).filter(|next| *next != u64::MAX)
}

/// Whether `stream` is read incrementally.
fn incremental(world: &World, stream: &str) -> bool {
    let streams = &world.workload.streams;
    let found = streams.iter().find(|found| found.name == stream);
    found.is_some_and(|found| found.read == ReadMode::Incremental)
}

impl Simulation {
    /// Checks the reports of every pipeline, each read to its sources' ends.
    pub(super) async fn check_reports(&self, seed: Seed) {
        let workload = &self.world.workload;
        let plans = (0..workload.pipelines)
            .map(|pipeline| plan(workload, &self.relaxed, pipeline).with_until(Until::Exhausted))
            .collect();
        let reported = Reported {
            engine: &self.engine,
            plans,
            world: &self.world,
            name: &self.name,
            placing: self.placing.as_ref(),
            stands,
            repeats: incremental,
            refusable: true,
        };
        reported.check(seed).await;
    }
}
