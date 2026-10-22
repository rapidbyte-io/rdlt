use std::collections::BTreeMap;

use super::{late, undrained};
use crate::source::Findings;

#[test]
fn a_read_is_late_only_past_its_channel_and_the_send_in_flight() {
    let findings = Findings {
        late: BTreeMap::from([((0, 0), 3), ((0, 1), 4), ((1, 0), 9)]),
        ..Findings::default()
    };
    assert_eq!(late(&findings, 2), vec![((0, 1), 4), ((1, 0), 9)]);
    assert_eq!(late(&findings, 7), vec![((1, 0), 9)]);
}

#[test]
fn a_dropped_partition_held_short_of_its_taken_checkpoint_or_nowhere_is_undrained() {
    let drained = [
        ((0, 0), 5),
        ((0, 1), 5),
        ((0, 2), 5),
        ((1, 0), 0),
        ((1, 1), 2),
    ];
    let held = |stream: usize, partition: usize| match (stream, partition) {
        (0, 0) => Some(5),
        (0, 1) => Some(4),
        (0, 2) => Some(u64::MAX),
        _ => None,
    };
    assert_eq!(
        undrained(&drained, held),
        vec![((0, 1), 5, Some(4)), ((1, 1), 2, None)]
    );
}
