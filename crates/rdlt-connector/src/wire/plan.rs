//! A source's plan of a stream's partitions on the wire.

use std::collections::{BTreeMap, BTreeSet};

use super::{Invalid, narrow, v1};
use crate::cursor::Cursor;
use crate::id::PartitionId;
use crate::limits::MAX_PLAN_PARTITIONS;
use crate::source::{Partition, PartitionPlan};

impl From<&PartitionPlan> for v1::PlanResponse {
    fn from(plan: &PartitionPlan) -> Self {
        let ids = |partitions: &mut dyn Iterator<Item = &Partition>| {
            partitions
                .map(|partition| partition.id().as_str().to_owned())
                .collect()
        };
        Self {
            partitions: ids(&mut plan.partitions.iter()),
            phase: plan.phase.map(u32::from),
            unbounded: ids(&mut plan
                .partitions
                .iter()
                .filter(|partition| partition.is_unbounded())),
            starts: plan
                .starts
                .iter()
                .map(|(partition, cursor)| v1::PartitionState {
                    partition: partition.as_str().to_owned(),
                    state: Some(v1::partition_state::State::Cursor(v1::Cursor::from(cursor))),
                })
                .collect(),
        }
    }
}

impl TryFrom<v1::PlanResponse> for PartitionPlan {
    type Error = Invalid;

    /// The plan `planned` names, each list counted before any id of it is read, and checked as
    /// [`PartitionPlan::validate`] checks a plan.
    fn try_from(planned: v1::PlanResponse) -> Result<Self, Invalid> {
        let named = planned.partitions.len();
        if named > MAX_PLAN_PARTITIONS
            || planned.unbounded.len() > named
            || planned.starts.len() > named
        {
            return Err(Invalid::OutOfRange("plan partitions"));
        }
        let phase = planned
            .phase
            .map(|phase| narrow("phase", phase))
            .transpose()?;
        let mut unbounded: BTreeSet<String> = BTreeSet::new();
        for id in planned.unbounded {
            if !unbounded.insert(id) {
                return Err(Invalid::Duplicate("unbounded partition"));
            }
        }
        let mut partitions = Vec::with_capacity(named);
        for id in planned.partitions {
            let never_ends = unbounded.remove(&id);
            let id =
                PartitionId::parse(id).map_err(|error| Invalid::rejected("partition id", error))?;
            let partition = Partition::new(id);
            partitions.push(if never_ends {
                partition.unbounded()
            } else {
                partition
            });
        }
        // An unbounded partition must be one of the plan's.
        if !unbounded.is_empty() {
            return Err(Invalid::Unknown("unbounded partition"));
        }
        let mut starts = BTreeMap::new();
        for start in planned.starts {
            let partition = PartitionId::parse(start.partition)
                .map_err(|error| Invalid::rejected("partition id", error))?;
            let Some(v1::partition_state::State::Cursor(cursor)) = start.state else {
                return Err(Invalid::Missing("start cursor"));
            };
            if starts
                .insert(partition, Cursor::try_from(cursor)?)
                .is_some()
            {
                return Err(Invalid::Duplicate("start"));
            }
        }
        let plan = Self {
            phase,
            partitions,
            starts,
        };
        plan.validate()
            .map_err(|invalid| Invalid::rejected("plan", invalid))?;
        Ok(plan)
    }
}
