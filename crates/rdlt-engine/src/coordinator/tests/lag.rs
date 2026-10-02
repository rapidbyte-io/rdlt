//! How far behind its source each stream last was, as its partitions said.

use rdlt_connector::PartitionId;

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
    assert_eq!(harness.log.lock().behind.get(&name()), None);
    coordinator.behind(0, 10);
    coordinator.behind(1, 5);
    coordinator.behind(0, 3);
    assert_eq!(harness.log.lock().behind.get(&name()), Some(&8));
}

#[tokio::test]
async fn a_partition_asked_to_stop_no_longer_moves_its_stream_s_lag() {
    let (mut coordinator, harness) = two_partitions().await;
    coordinator.behind(0, 4);
    coordinator.parts.partitions[1].stop.cancel();
    coordinator.behind(1, 50);
    assert_eq!(harness.log.lock().behind.get(&name()), Some(&4));
}

#[tokio::test]
async fn a_forgotten_partition_leaves_its_stream_s_lag_to_the_rest_and_none_leaves_it_unknown() {
    let (mut coordinator, harness) = two_partitions().await;
    coordinator.behind(0, 10);
    coordinator.behind(1, 5);
    coordinator.forget_lag(0, Some(&[id("p1")].into()));
    assert_eq!(harness.log.lock().behind.get(&name()), Some(&10));
    coordinator.forget_lag(0, Some(&[id("p0")].into()));
    assert_eq!(harness.log.lock().behind.get(&name()), None);
}

#[tokio::test]
async fn a_phase_s_end_forgets_every_partition_s_lag() {
    let (mut coordinator, harness) = two_partitions().await;
    coordinator.behind(0, 10);
    coordinator.behind(1, 5);
    coordinator.forget_lag(0, None);
    assert_eq!(harness.log.lock().behind.get(&name()), None);
    // A stream no partition of which ever said stays unknown.
    coordinator.forget_lag(0, None);
    assert_eq!(harness.log.lock().behind.get(&name()), None);
}
