use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{Partition, PartitionId, StreamName};
use tokio_util::sync::CancellationToken;

use super::Slots;
use crate::budget::MemoryBudget;
use crate::env::InlineEnv;
use crate::error::{ErrorKind, codes};
use crate::partition::PartitionJob;

fn count(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

/// A job reading partition `index`, unbounded and followed where `followed` says.
fn job(index: usize, followed: bool) -> PartitionJob {
    let id = PartitionId::parse(format!("p{index}")).unwrap();
    PartitionJob {
        index,
        stream: StreamName::new("events").unwrap(),
        table: 0,
        partition: if followed {
            Partition::new(id).unbounded()
        } else {
            Partition::new(id)
        },
        cursor: None,
        on_demand: false,
        changes: None,
        stop: CancellationToken::new(),
        follow: followed,
        reset_retention: false,
    }
}

#[test]
fn followed_reads_beyond_the_slots_less_one_are_refused_by_whatever_sets_the_slots() {
    // Partitions, the source's calls, the followed reads that take a place, the refusal's code.
    for (partitions, calls, places, code) in [
        (16, None, 15, codes::PARTITIONS_TOO_FEW),
        (4, Some(199), 3, codes::PARTITIONS_TOO_FEW),
        (5, Some(6), 4, codes::PARTITIONS_TOO_FEW),
        (5, Some(5), 3, codes::SOURCE_CALLS_EXCEEDED),
        (16, Some(5), 3, codes::SOURCE_CALLS_EXCEEDED),
        (300, Some(199), 197, codes::SOURCE_CALLS_EXCEEDED),
        (2, Some(1), 0, codes::SOURCE_CALLS_EXCEEDED),
    ] {
        let slots = Slots::new(count(partitions), calls.map(count));
        let held: Vec<_> = (0..places)
            .map(|index| slots.endless(&job(index, true)).unwrap())
            .collect();
        assert!(held.iter().all(Option::is_some));
        let refused = slots.endless(&job(places, true)).unwrap_err();
        assert_eq!(
            (refused.kind(), refused.code()),
            (ErrorKind::Config, Some(code)),
            "{partitions} partitions, {calls:?} calls: {refused}"
        );
        assert!(slots.endless(&job(places, false)).unwrap().is_none());
    }
}

#[tokio::test(start_paused = true)]
#[expect(
    clippy::disallowed_methods,
    reason = "the test bounds its waits on tokio's paused clock directly"
)]
async fn reads_beyond_what_the_source_carries_wait_for_a_slot_until_one_frees() {
    let budget = MemoryBudget::new(64 << 20, Arc::new(InlineEnv), Duration::from_secs(60));
    let slots = Slots::new(count(16), Some(count(5)));
    let mut held = Vec::new();
    for _ in 0..4 {
        held.push(slots.read(&budget).await.unwrap());
    }
    let waiting = tokio::time::timeout(Duration::from_secs(3600), slots.read(&budget)).await;
    assert!(waiting.is_err(), "a fifth read found a slot");
    drop(held.pop());
    let freed = tokio::time::timeout(Duration::from_secs(1), slots.read(&budget)).await;
    assert!(freed.is_ok(), "a freed slot was not taken");
}
