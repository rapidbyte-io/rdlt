//! How far behind its source each stream last was, as its partitions said.

use std::time::Duration;

use rdlt_connector::{Headroom, PartitionId};

use super::{Setup, name, partition, stream};
use crate::coordinator::Coordinator;
use crate::plan::WriteMode;

/// A coordinator of one stream with partitions `p0` and `p1`, and the handles to its log.
async fn two_partitions() -> (Coordinator, super::Harness) {
    let partitions = vec![partition("p0", false), partition("p1", false)];
    Setup::new(vec![stream(WriteMode::Append, None, 2)], partitions)
        .coordinator()
        .await
}

fn id(id: &str) -> PartitionId {
    PartitionId::parse(id).unwrap()
}

#[tokio::test]
async fn a_stream_s_lag_is_the_total_its_partitions_last_said() {
    let (mut coordinator, harness) = two_partitions().await;
    assert!(!harness.log.lock().behind.contains_key(&name()));
    coordinator.behind(0, 10, Headroom::default());
    coordinator.behind(1, 5, Headroom::default());
    coordinator.behind(0, 3, Headroom::default());
    assert_eq!(
        harness
            .log
            .lock()
            .behind
            .get(&name())
            .map(|(records, _)| *records),
        Some(8)
    );
}

#[tokio::test]
async fn a_partition_asked_to_stop_no_longer_moves_its_stream_s_lag() {
    let (mut coordinator, harness) = two_partitions().await;
    coordinator.behind(0, 4, Headroom::default());
    coordinator.parts.partitions[1].stop.cancel();
    coordinator.behind(1, 50, Headroom::default());
    assert_eq!(
        harness
            .log
            .lock()
            .behind
            .get(&name())
            .map(|(records, _)| *records),
        Some(4)
    );
}

#[tokio::test]
async fn a_forgotten_partition_leaves_its_stream_s_lag_to_the_rest_and_none_leaves_it_unknown() {
    let (mut coordinator, harness) = two_partitions().await;
    coordinator.behind(0, 10, Headroom::default());
    coordinator.behind(1, 5, Headroom::default());
    coordinator.forget_lag(0, Some(&[id("p1")].into()));
    assert_eq!(
        harness
            .log
            .lock()
            .behind
            .get(&name())
            .map(|(records, _)| *records),
        Some(10)
    );
    coordinator.forget_lag(0, Some(&[id("p0")].into()));
    assert!(!harness.log.lock().behind.contains_key(&name()));
}

#[tokio::test]
async fn a_phase_s_end_forgets_every_partition_s_lag() {
    let (mut coordinator, harness) = two_partitions().await;
    coordinator.behind(0, 10, Headroom::default());
    coordinator.behind(1, 5, Headroom::default());
    coordinator.forget_lag(0, None);
    assert!(!harness.log.lock().behind.contains_key(&name()));
    // A stream no partition of which ever said stays unknown.
    coordinator.forget_lag(0, None);
    assert!(!harness.log.lock().behind.contains_key(&name()));
}

#[tokio::test]
async fn a_stream_s_headroom_is_the_least_its_partitions_last_said_of_each_measure() {
    let (mut coordinator, harness) = two_partitions().await;
    let headroom = |bytes: Option<u64>, seconds: Option<u64>| Headroom {
        bytes,
        time: seconds.map(Duration::from_secs),
    };
    let said = |harness: &super::Harness| {
        harness
            .log
            .lock()
            .behind
            .get(&name())
            .map(|(_, headroom)| *headroom)
    };
    coordinator.behind(0, 1, headroom(Some(100), None));
    coordinator.behind(1, 1, headroom(Some(300), Some(60)));
    assert_eq!(said(&harness), Some(headroom(Some(100), Some(60))));
    // A partition's newer word replaces its older, whichever measure it drops.
    coordinator.behind(0, 1, headroom(Some(500), Some(90)));
    assert_eq!(said(&harness), Some(headroom(Some(300), Some(60))));
    coordinator.forget_lag(0, Some(&[id("p1")].into()));
    assert_eq!(said(&harness), Some(headroom(Some(500), Some(90))));
}
