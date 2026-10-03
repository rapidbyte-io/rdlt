//! What the coordinator's bookkeeping costs as a plan's partitions grow, and across replans.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rdlt_connector::{
    BoxFuture, Catalog, ConnectorError, Cursor, Partition, PartitionId, PartitionPlan,
    PartitionSink, PartitionState, ReadRequest, Result, Source, StreamName, StreamState,
};

use super::{Setup, cursor, name, position, stream};
use crate::coordinator::delta::Collected;
use crate::coordinator::{Coordinator, Phases, Template};
use crate::partition::{ChangeMode, Progress};
use crate::plan::{DeleteMode, OnTruncate, WriteMode};

/// Partitions: as many as an attempt may read at once, enough that work comparing each with
/// every other takes seconds.
const MANY: usize = rdlt_connector::limits::MAX_PLAN_PARTITIONS;

/// Work on `MANY` partitions done in time linear in them takes well under this much of a
/// thread's CPU time.
const LINEAR: Duration = Duration::from_secs(2);

/// The CPU time this thread has spent, which the work measured takes whatever else the machine
/// runs: other processes lengthen its wall-clock time, not this.
fn cpu() -> Duration {
    let spent = rustix::time::clock_gettime(rustix::time::ClockId::ThreadCPUTime);
    let seconds = u64::try_from(spent.tv_sec).expect("a thread's time is positive");
    let nanos = u32::try_from(spent.tv_nsec).expect("nanoseconds below a second");
    Duration::new(seconds, nanos)
}

/// A source whose next plan names the partitions `plans` holds next.
struct Planning {
    plans: Mutex<Vec<Vec<String>>>,
}

impl Source for Planning {
    fn check(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn discover(&self) -> BoxFuture<'_, Result<Catalog>> {
        Box::pin(async { Err(ConnectorError::internal("unused")) })
    }

    fn plan<'a>(
        &'a self,
        _stream: &'a StreamName,
        _state: &'a StreamState,
    ) -> BoxFuture<'a, Result<PartitionPlan>> {
        Box::pin(async move {
            let ids = self.plans.lock().remove(0);
            Ok(PartitionPlan::new(
                ids.iter().map(|id| partition(id)).collect(),
            ))
        })
    }

    fn read(&self, _request: ReadRequest, _sink: PartitionSink) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Err(ConnectorError::internal("unused")) })
    }

    fn committed<'a>(
        &'a self,
        _stream: &'a StreamName,
        _cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

fn partition(id: &str) -> Partition {
    Partition::new(PartitionId::parse(id).unwrap())
}

fn ids(count: usize) -> Vec<String> {
    (0..count).map(|index| format!("p{index:08}")).collect()
}

/// A coordinator of one change stream read in phases, its partitions started by nothing, whose
/// source plans as `plans` say, following its source where `follow`.
async fn phased(plans: Vec<Vec<String>>, follow: bool) -> Coordinator {
    let mut changes = stream(WriteMode::Append, None, 0);
    changes.phases = Some(Phases {
        phase: 0,
        reading: BTreeMap::new(),
        committed: BTreeMap::new(),
        begun: None,
        settled: false,
        template: Template {
            table: 0,
            on_demand: true,
            changes: Some(ChangeMode {
                merge: false,
                deletes: DeleteMode::Hard,
                truncates: OnTruncate::Apply,
                partial_updates: false,
            }),
            reset_retention: false,
        },
    });
    let (mut coordinator, _) = Setup::new(vec![changes], Vec::new()).coordinator().await;
    coordinator.parts.launcher = Box::new(|_| Ok(()));
    coordinator.parts.source = Arc::new(Planning {
        plans: Mutex::new(plans),
    });
    coordinator.parts.follow = follow;
    coordinator
}

/// `coordinator`, its stream begun in phase 1 with `MANY` partitions.
async fn begun_with_many() -> Coordinator {
    let mut coordinator = phased(Vec::new(), false).await;
    let plan = PartitionPlan::new(ids(MANY).iter().map(|id| partition(id)).collect()).phase(1);
    let started = cpu();
    coordinator.begin(0, 1, plan).unwrap();
    let elapsed = cpu().saturating_sub(started);
    assert!(elapsed < LINEAR, "beginning took {elapsed:?}");
    assert_eq!(coordinator.parts.partitions.len(), MANY);
    coordinator
}

#[tokio::test]
async fn beginning_a_phase_of_many_partitions_takes_linear_time() {
    let coordinator = begun_with_many().await;
    assert!(!coordinator.all_ended());
}

#[tokio::test]
async fn the_lag_of_many_partitions_is_kept_and_forgotten_in_linear_time() {
    let mut coordinator = begun_with_many().await;
    let started = cpu();
    for index in 0..MANY {
        coordinator.behind(index, 2);
    }
    assert_eq!(
        coordinator.parts.log.lock().behind.get(&name()),
        Some(&(2 * u64::try_from(MANY).unwrap()))
    );
    let forgotten: BTreeSet<PartitionId> = ids(MANY)
        .iter()
        .map(|id| PartitionId::parse(id).unwrap())
        .collect();
    coordinator.forget_lag(0, Some(&forgotten));
    let elapsed = cpu().saturating_sub(started);
    assert!(elapsed < LINEAR, "keeping lag took {elapsed:?}");
    assert_eq!(coordinator.parts.log.lock().behind.get(&name()), None);
}

#[tokio::test]
async fn positions_of_many_partitions_are_recorded_and_weighed_in_linear_time() {
    let mut coordinator = begun_with_many().await;
    let positions: BTreeMap<usize, PartitionState> = (0..MANY)
        .map(|index| (index, PartitionState::Cursor(cursor(1))))
        .collect();
    let started = cpu();
    coordinator.record_positions(&positions);
    coordinator.landed(&positions, false);
    // Recorded again where they stand, the positions move nothing.
    let delta: Vec<_> = ids(MANY)
        .iter()
        .map(|id| position(id, PartitionState::Cursor(cursor(1))))
        .collect();
    let collected = Collected {
        segments: rdlt_connector::SegmentSet::new(),
        positions,
        reported: BTreeMap::new(),
        streams: BTreeMap::new(),
        sealed: Vec::new(),
        held: Vec::new(),
    };
    assert!(!coordinator.progresses(&collected, &delta));
    let elapsed = cpu().saturating_sub(started);
    assert!(elapsed < LINEAR, "recording positions took {elapsed:?}");
    let phases = coordinator.parts.streams[0].phases.as_ref().unwrap();
    assert_eq!(phases.committed.len(), MANY);
}

#[tokio::test]
async fn ending_many_partitions_is_counted_in_linear_time() {
    let mut coordinator = begun_with_many().await;
    let started = cpu();
    for index in 0..MANY {
        assert!(!coordinator.all_ended());
        coordinator.observe(Progress::Ended {
            partition: index,
            stopped: false,
        });
    }
    assert!(coordinator.all_ended());
    let elapsed = cpu().saturating_sub(started);
    assert!(elapsed < LINEAR, "ending took {elapsed:?}");
}

#[tokio::test(start_paused = true)]
async fn replans_naming_new_partitions_take_the_places_of_those_they_dropped() {
    const REPLANS: usize = 200;
    let plans = (0..REPLANS)
        .map(|replan| vec![format!("r{replan}")])
        .collect();
    let mut coordinator = phased(plans, true).await;
    for _ in 0..REPLANS {
        coordinator.replan().await.unwrap();
        // The partition the plan named reads to its end before the next plan.
        let reading: Vec<usize> = coordinator.parts.streams[0]
            .phases
            .as_ref()
            .unwrap()
            .reading
            .values()
            .copied()
            .filter(|index| !coordinator.parts.partitions[*index].ended)
            .collect();
        for partition in reading {
            coordinator.observe(Progress::Ended {
                partition,
                stopped: false,
            });
        }
    }
    assert!(
        coordinator.parts.partitions.len() <= 2,
        "{} partitions tracked",
        coordinator.parts.partitions.len()
    );
    let phases = coordinator.parts.streams[0].phases.as_ref().unwrap();
    assert!(phases.reading.len() <= 2, "{} read", phases.reading.len());
}
