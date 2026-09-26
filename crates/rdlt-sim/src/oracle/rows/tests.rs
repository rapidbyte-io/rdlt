use std::collections::BTreeSet;

use rdlt_engine::WriteMode;

use super::merged;
use crate::rng::SplitMix64;
use crate::swarm::Features;
use crate::workload::{SimStream, Workload};

/// A merge stream whose partitions share keys, with rows in more than one partition.
fn shared() -> SimStream {
    (0..400)
        .flat_map(|seed| Workload::generate(&mut SplitMix64::new(seed), Features::ALL).streams)
        .find(|stream| {
            stream.write == WriteMode::Merge
                && stream.shared_keys
                && stream.partitions.iter().filter(|rows| rows[1] > 0).count() > 1
        })
        .expect("a merge stream sharing keys across partitions")
}

#[test]
fn a_shared_key_may_hold_the_last_row_of_any_partition_delivering_it_last() {
    let stream = shared();
    for group in merged(&stream, 1, false) {
        assert_eq!(group.count, 1);
        let partitions: BTreeSet<i64> = group.rows.iter().map(|row| row.partition).collect();
        assert_eq!(partitions.len(), group.rows.len(), "one row per partition");
        let phases: BTreeSet<usize> = group.rows.iter().map(|row| row.delivered).collect();
        assert_eq!(
            phases.len(),
            1,
            "all delivered in the last phase with the key"
        );
        let keys: BTreeSet<_> = group.rows.iter().map(|row| (row.key, &row.tag)).collect();
        assert_eq!(keys.len(), 1, "all of one key");
    }
    assert!(
        merged(&stream, 1, false)
            .iter()
            .any(|group| group.rows.len() > 1),
        "some key races across partitions"
    );
}

#[test]
fn a_phase_stopped_short_may_hold_any_row_of_a_key() {
    let stream = shared();
    let every: usize = merged(&stream, 1, true)
        .iter()
        .map(|group| group.rows.len())
        .sum();
    let latest: usize = merged(&stream, 1, false)
        .iter()
        .map(|group| group.rows.len())
        .sum();
    assert!(every >= latest);
    assert!(
        merged(&stream, 1, true)
            .iter()
            .all(|group| group.count == 1)
    );
}
