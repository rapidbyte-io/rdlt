use std::collections::BTreeSet;

use rdlt_connector::ReadMode;
use rdlt_engine::{Nested, SchemaPolicy, WriteMode};

use super::{PHASES, Workload};
use crate::rng::SplitMix64;

#[test]
fn the_same_seed_generates_the_same_workload() {
    let a = Workload::generate(&mut SplitMix64::new(5));
    let b = Workload::generate(&mut SplitMix64::new(5));
    assert_eq!(a, b);
    assert_ne!(a, Workload::generate(&mut SplitMix64::new(6)));
}

#[test]
fn incremental_streams_only_grow_and_keep_their_rows() {
    for seed in 0..200 {
        let workload = Workload::generate(&mut SplitMix64::new(seed));
        for stream in workload
            .streams
            .iter()
            .filter(|s| s.read == ReadMode::Incremental)
        {
            for partition in 0..stream.partitions.len() {
                let first = stream.rows(workload.salt, partition, 0);
                let second = stream.rows(workload.salt, partition, 1);
                assert!(second.starts_with(&first), "seed {seed}");
            }
        }
    }
}

#[test]
fn full_reads_see_new_values_in_each_phase() {
    let workload = (0..200)
        .map(|seed| Workload::generate(&mut SplitMix64::new(seed)))
        .find(|workload| {
            workload
                .streams
                .iter()
                .any(|stream| stream.read == ReadMode::Full && stream.partitions[0] == [5, 5])
        });
    let Some(workload) = workload else {
        return;
    };
    let stream = workload
        .streams
        .iter()
        .find(|stream| stream.read == ReadMode::Full && stream.partitions[0] == [5, 5])
        .expect("found above");
    let first = stream.rows(workload.salt, 0, 0);
    let second = stream.rows(workload.salt, 0, 1);
    assert_eq!(first.len(), 5);
    assert!(
        first
            .iter()
            .zip(&second)
            .all(|(a, b)| a.id == b.id && a.value != b.value)
    );
    assert_eq!(PHASES, 2);
}

#[test]
fn row_ids_are_unique_within_a_stream() {
    for seed in 0..200 {
        let workload = Workload::generate(&mut SplitMix64::new(seed));
        for stream in &workload.streams {
            let rows = stream.all_rows(workload.salt, 1);
            let ids: BTreeSet<i64> = rows.iter().map(|row| row.id).collect();
            assert_eq!(ids.len(), rows.len(), "seed {seed}");
        }
    }
}

#[test]
fn workloads_cover_merges_drift_and_every_policy() {
    let streams: Vec<_> = (0..300)
        .flat_map(|seed| Workload::generate(&mut SplitMix64::new(seed)).streams)
        .collect();
    let writes: BTreeSet<_> = streams
        .iter()
        .map(|stream| format!("{:?}", stream.write))
        .collect();
    assert_eq!(writes.len(), 3, "append, replace and merge: {writes:?}");
    assert!(
        streams
            .iter()
            .any(|stream| stream.write == WriteMode::Merge && stream.plan_key)
    );
    assert!(
        streams
            .iter()
            .any(|stream| stream.write == WriteMode::Merge && !stream.plan_key)
    );
    for policy in [
        SchemaPolicy::Evolve,
        SchemaPolicy::DiscardRow,
        SchemaPolicy::DiscardValue,
    ] {
        assert!(
            streams.iter().any(|stream| stream.policy == policy),
            "{policy:?}"
        );
    }
    assert!(streams.iter().any(|stream| stream.nested == Nested::Json));
    let changing = streams
        .iter()
        .flat_map(|stream| &stream.drift)
        .any(|drift| {
            let shapes: BTreeSet<_> = drift.shapes.iter().flatten().flatten().collect();
            shapes.len() > 1
        });
    assert!(changing, "some drift column changes type");
}

#[test]
fn merge_rows_share_keys_within_their_partition() {
    let workload = (0..200)
        .map(|seed| Workload::generate(&mut SplitMix64::new(seed)))
        .find(|workload| {
            workload
                .streams
                .iter()
                .any(|stream| stream.keys > 0 && stream.partitions[0][0] > stream.keys)
        })
        .expect("some merge stream holds more rows than keys");
    let stream = workload
        .streams
        .iter()
        .find(|stream| stream.keys > 0 && stream.partitions[0][0] > stream.keys)
        .expect("found above");
    let rows = stream.rows(workload.salt, 0, 0);
    let keys: BTreeSet<_> = rows.iter().map(|row| row.key).collect();
    assert_eq!(keys.len() as u64, stream.keys);
    assert!(
        rows.iter()
            .all(|row| row.key.is_some_and(|key| key < 1_000_000))
    );
}
