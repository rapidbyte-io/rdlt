use std::collections::BTreeSet;

use rdlt_connector::{LogicalType, ReadMode, TypeKind};
use rdlt_engine::{Nested, SchemaPolicy, WriteMode};

use super::{PHASES, SimStream, Workload};
use crate::rng::SplitMix64;
use crate::swarm::Features;

#[test]
fn the_same_seed_generates_the_same_workload() {
    let a = Workload::generate(&mut SplitMix64::new(5), Features::ALL);
    let b = Workload::generate(&mut SplitMix64::new(5), Features::ALL);
    assert_eq!(format!("{a:?}"), format!("{b:?}"));
    assert_ne!(
        a,
        Workload::generate(&mut SplitMix64::new(6), Features::ALL)
    );
}

#[test]
fn incremental_streams_only_grow_and_keep_their_rows() {
    for seed in 0..200 {
        let workload = Workload::generate(&mut SplitMix64::new(seed), Features::ALL);
        for stream in workload
            .streams
            .iter()
            .filter(|s| s.read == ReadMode::Incremental)
        {
            for partition in 0..stream.partitions.len() {
                // Drawn floats may be NaN, which equals nothing, so rows compare as text.
                let text = |phase| -> Vec<String> {
                    let rows = stream.rows(partition, phase);
                    rows.iter().map(|row| format!("{row:?}")).collect()
                };
                assert!(text(1).starts_with(&text(0)), "seed {seed}");
            }
        }
    }
}

#[test]
fn full_reads_see_new_values_in_each_phase() {
    let workload = (0..200)
        .map(|seed| Workload::generate(&mut SplitMix64::new(seed), Features::ALL))
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
    let first = stream.rows(0, 0).to_vec();
    let second = stream.rows(0, 1).to_vec();
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
        let workload = Workload::generate(&mut SplitMix64::new(seed), Features::ALL);
        for stream in &workload.streams {
            let rows = stream.all_rows(1);
            let ids: BTreeSet<i64> = rows.iter().map(|row| row.id).collect();
            assert_eq!(ids.len(), rows.len(), "seed {seed}");
        }
    }
}

#[test]
fn workloads_cover_merges_drift_json_and_every_policy() {
    let streams: Vec<_> = (0..300)
        .flat_map(|seed| Workload::generate(&mut SplitMix64::new(seed), Features::ALL).streams)
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
    assert!(
        streams.iter().any(|stream| stream.json),
        "some stream pushes JSON"
    );
    assert!(
        streams
            .iter()
            .any(|stream| stream.json && stream.drift.len() > 1),
        "some JSON stream drifts"
    );
    let changing = streams
        .iter()
        .flat_map(|stream| &stream.drift)
        .any(|drift| {
            let shapes: Vec<_> = drift.shapes.iter().flatten().flatten().collect();
            shapes.iter().any(|shape| *shape != shapes[0])
        });
    assert!(changing, "some drift column changes type");
}

#[test]
fn merge_rows_share_keys_within_their_partition() {
    let workload = (0..200)
        .map(|seed| Workload::generate(&mut SplitMix64::new(seed), Features::ALL))
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
    let rows = stream.rows(0, 0).to_vec();
    let keys: BTreeSet<_> = rows.iter().map(|row| row.key).collect();
    assert_eq!(keys.len() as u64, stream.keys);
    assert!(
        rows.iter()
            .all(|row| row.key.is_some_and(|key| key < 1_000_000))
    );
}

#[test]
fn some_streams_normalize_arrays_under_every_write_mode_and_policy() {
    let streams: Vec<_> = (0..300)
        .flat_map(|seed| Workload::generate(&mut SplitMix64::new(seed), Features::ALL).streams)
        .collect();
    let has_arrays = |stream: &SimStream| {
        stream.normalized()
            && stream.drift.iter().any(|drift| {
                drift
                    .shapes
                    .iter()
                    .flatten()
                    .flatten()
                    .any(|shape| matches!(shape.logical, LogicalType::List(_)))
            })
    };
    for json in [false, true] {
        assert!(
            streams
                .iter()
                .any(|stream| has_arrays(stream) && stream.json == json),
            "some normalized stream pushing JSON={json} has arrays"
        );
    }
    for write in [WriteMode::Append, WriteMode::Replace, WriteMode::Merge] {
        assert!(
            streams
                .iter()
                .any(|stream| has_arrays(stream) && stream.write == write),
            "some normalized stream with arrays writes as {write:?}"
        );
    }
    for policy in [SchemaPolicy::DiscardRow, SchemaPolicy::DiscardValue] {
        assert!(
            streams
                .iter()
                .any(|stream| has_arrays(stream) && stream.policy == policy),
            "some normalized stream with arrays has policy {policy:?}"
        );
    }
}

/// Every type and encoding the shape of a column or anything inside it takes.
fn seen(shape: &rdlt_testkit::drawn::Shape, into: &mut BTreeSet<(TypeKind, String)>) {
    let encoding = match shape.encoding {
        rdlt_testkit::drawn::Encoding::FixedSize(_) => "FixedSize".to_owned(),
        other => format!("{other:?}"),
    };
    into.insert((shape.logical.kind(), encoding));
    for child in &shape.children {
        seen(child, into);
    }
}

#[test]
fn drift_columns_take_every_type_in_every_encoding() {
    let mut drawn = BTreeSet::new();
    for seed in 0..400 {
        for stream in Workload::generate(&mut SplitMix64::new(seed), Features::ALL).streams {
            for shape in stream
                .drift
                .iter()
                .flat_map(|drift| drift.shapes.iter().flatten().flatten())
            {
                seen(shape, &mut drawn);
            }
        }
    }
    let kinds: BTreeSet<TypeKind> = drawn.iter().map(|(kind, _)| *kind).collect();
    let every: BTreeSet<TypeKind> = rdlt_testkit::drawn::KINDS.into_iter().collect();
    assert_eq!(kinds, every);
    for encoding in [
        "Plain",
        "Unsigned",
        "Half",
        "Decimal32",
        "Decimal64",
        "Decimal256",
        "Large",
        "View",
        "LargeView",
        "Dictionary",
        "RunEnd",
        "FixedSize",
        "Date64",
        "Map",
    ] {
        assert!(
            drawn.iter().any(|(_, drawn)| drawn == encoding),
            "no drift column is ever encoded as {encoding}"
        );
    }
}

#[test]
fn drift_nests_objects_in_arrays_and_arrays_in_arrays() {
    let nested = |shape: &rdlt_testkit::drawn::Shape, outer: TypeKind, inner: TypeKind| {
        shape.logical.kind() == outer
            && shape
                .children
                .iter()
                .any(|child| child.logical.kind() == inner)
    };
    let shapes: Vec<rdlt_testkit::drawn::Shape> = (0..400)
        .flat_map(|seed| Workload::generate(&mut SplitMix64::new(seed), Features::ALL).streams)
        .flat_map(|stream| stream.drift)
        .flat_map(|drift| drift.shapes.into_iter().flatten().flatten())
        .collect();
    for (outer, inner) in [
        (TypeKind::List, TypeKind::Struct),
        (TypeKind::List, TypeKind::List),
        (TypeKind::Struct, TypeKind::Struct),
        (TypeKind::Struct, TypeKind::List),
    ] {
        assert!(
            shapes.iter().any(|shape| nested(shape, outer, inner)),
            "no drift column is a {outer:?} of {inner:?}"
        );
    }
}

#[test]
fn features_off_leave_their_parts_of_the_workload_out() {
    let none = Features {
        drift: false,
        depth: 0,
        encodings: false,
        json: false,
        normalize: false,
        sliced: false,
        faults: false,
        disruptions: false,
        narrow: false,
    };
    for seed in 0..100 {
        for stream in Workload::generate(&mut SplitMix64::new(seed), none).streams {
            assert!(
                stream.drift.is_empty() && !stream.json && !stream.sliced && !stream.normalized()
            );
        }
    }
    let scalars = Features {
        drift: true,
        ..none
    };
    for seed in 0..100 {
        for stream in Workload::generate(&mut SplitMix64::new(seed), scalars).streams {
            for shape in stream
                .drift
                .iter()
                .flat_map(|drift| drift.shapes.iter().flatten().flatten())
            {
                assert!(shape.children.is_empty(), "depth 0 draws scalars only");
                assert_eq!(shape.encoding, rdlt_testkit::drawn::Encoding::Plain);
            }
        }
    }
}
