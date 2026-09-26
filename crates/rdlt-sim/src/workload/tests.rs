use std::collections::BTreeSet;

use rdlt_connector::{LogicalType, ReadMode, TypeKind};
use rdlt_engine::{Nested, OnUnsupported, SchemaPolicy, WriteMode};

use super::{Drift, Level, PHASES, Relaxed, SimStream, Workload};
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

/// The policies `stream`'s columns resolve to: its drift columns' and its others'.
fn policies(stream: &SimStream) -> Vec<SchemaPolicy> {
    (0..stream.drift.len())
        .map(Some)
        .chain([None])
        .map(|column| stream.resolved(column, Relaxed::default()).policy)
        .collect()
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
            streams
                .iter()
                .any(|stream| policies(stream).contains(&policy)),
            "{policy:?}"
        );
    }
    assert!(
        streams
            .iter()
            .any(|stream| stream.resolved(None, Relaxed::default()).nested == Nested::Json)
    );
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
    assert!(rows.iter().all(|row| row.key.is_some_and(|key| key < 16)));
}

/// Every stream of 300 seeds' workloads with every feature on.
fn streams() -> Vec<SimStream> {
    (0..300)
        .flat_map(|seed| Workload::generate(&mut SplitMix64::new(seed), Features::ALL).streams)
        .collect()
}

#[test]
fn settings_are_drawn_at_every_level() {
    let workloads: Vec<Workload> = (0..300)
        .map(|seed| Workload::generate(&mut SplitMix64::new(seed), Features::ALL))
        .collect();
    let streams: Vec<&SimStream> = workloads.iter().flat_map(|w| &w.streams).collect();
    let drifts: Vec<&Drift> = streams.iter().flat_map(|stream| &stream.drift).collect();
    let pipelines: Vec<Level> = workloads.iter().map(|w| w.pipeline).collect();
    let levels: [Vec<Level>; 3] = [
        pipelines,
        streams.iter().map(|s| s.schema).collect(),
        drifts.iter().map(|d| d.settings).collect(),
    ];
    for level in &levels {
        for policy in [
            SchemaPolicy::Evolve,
            SchemaPolicy::Freeze,
            SchemaPolicy::DiscardRow,
            SchemaPolicy::DiscardValue,
        ] {
            assert!(level.iter().any(|l| l.policy == Some(policy)), "{policy:?}");
        }
        let refuse = Some(OnUnsupported::Refuse);
        assert!(level.iter().any(|l| l.on_unsupported == refuse));
        assert!(level.iter().any(|l| l.nested == Some(Nested::Json)));
    }
    assert!(
        levels[0]
            .iter()
            .any(|l| matches!(l.nested, Some(Nested::Normalize { .. })))
    );
}

#[test]
fn drift_columns_are_hinted_declared_both_or_neither() {
    let streams = streams();
    let drifts: Vec<&Drift> = streams.iter().flat_map(|stream| &stream.drift).collect();
    assert!(
        drifts
            .iter()
            .any(|d| d.hint.is_some() && d.declared.is_some())
    );
    assert!(
        drifts
            .iter()
            .any(|d| d.hint.is_some() && d.declared.is_none())
    );
    assert!(
        drifts
            .iter()
            .any(|d| d.hint.is_none() && d.declared.is_some())
    );
    assert!(
        drifts
            .iter()
            .any(|d| d.hint.is_none() && d.declared.is_none())
    );
    assert!(
        streams
            .iter()
            .any(|s| s.normalized() && s.drift.iter().any(|d| d.hint.is_some())),
        "some normalized stream hints a column"
    );
}

#[test]
fn json_streams_hint_and_declare_only_types_json_values_are_inferred_as() {
    let inferred = [
        LogicalType::Bool,
        LogicalType::Int64,
        LogicalType::Float64,
        LogicalType::Utf8,
        LogicalType::Json,
    ];
    let typed: Vec<LogicalType> = streams()
        .iter()
        .filter(|stream| stream.json)
        .flat_map(|stream| &stream.drift)
        .flat_map(|drift| drift.hint.iter().chain(&drift.declared))
        .cloned()
        .collect();
    assert!(!typed.is_empty());
    assert!(
        typed.iter().all(|logical| inferred.contains(logical)),
        "{typed:?}"
    );
}

#[test]
fn a_normalized_stream_declares_no_column_its_policy_discards() {
    for stream in streams().iter().filter(|stream| stream.normalized()) {
        for (column, drift) in stream.drift.iter().enumerate() {
            let policy = stream.resolved(Some(column), Relaxed::default()).policy;
            let discards = matches!(
                policy,
                SchemaPolicy::DiscardRow | SchemaPolicy::DiscardValue
            );
            assert!(!(discards && drift.declared.is_some()), "{}", drift.name);
        }
    }
}

#[test]
fn merge_keys_change_type_collide_across_partitions_and_span_two_columns() {
    let merging: Vec<SimStream> = streams()
        .into_iter()
        .filter(|stream| stream.keys > 0)
        .collect();
    let types: BTreeSet<String> = merging
        .iter()
        .flat_map(|stream| stream.key_types.iter().flatten())
        .map(ToString::to_string)
        .collect();
    for logical in [
        "int8",
        "int16",
        "int32",
        "int64",
        "decimal(20, 0)",
        "decimal(38, 0)",
        "utf8",
        "float64",
    ] {
        assert!(types.contains(logical), "{logical}: {types:?}");
    }
    let shared = merging
        .iter()
        .find(|stream| stream.shared_keys && stream.partitions.len() > 1)
        .expect("some stream shares keys across partitions");
    let keys = |partition| -> BTreeSet<Option<i64>> {
        shared
            .rows(partition, 0)
            .iter()
            .map(|row| row.key)
            .collect()
    };
    assert!(
        keys(0).is_empty() || keys(1).is_empty() || !keys(0).is_disjoint(&keys(1)),
        "partitions share keys"
    );
    let composite = merging
        .iter()
        .find(|stream| stream.composite)
        .expect("some key spans two columns");
    assert_eq!(composite.key_columns(), ["key", "tag"]);
    assert!(composite.all_rows(0).iter().all(|row| row.tag.is_some()));
    assert!(
        merging
            .iter()
            .filter(|stream| stream.json)
            .all(|stream| stream
                .key_types
                .iter()
                .flatten()
                .all(|l| *l == LogicalType::Int64)),
        "JSON keys are integers"
    );
}

#[test]
fn a_read_resumes_where_the_last_phases_ended_in_batches_of_the_streams_size() {
    let stream = streams()
        .into_iter()
        .find(|stream| {
            stream.read == ReadMode::Incremental
                && stream.batch_rows > 1
                && stream.partitions[0][0] > 0
                && stream.partitions[0][1] > stream.partitions[0][0]
        })
        .expect("an incremental stream that grows");
    let first = usize::try_from(stream.partitions[0][0]).unwrap();
    let batches = stream.batches(0, 1);
    assert_eq!(batches[0].start, first);
    assert_eq!(batches.last().unwrap().end, stream.rows(0, 1).len());
    assert!(
        batches
            .iter()
            .all(|batch| batch.len() <= usize::try_from(stream.batch_rows).unwrap())
    );
    assert!(stream.read(0, 1).iter().all(|row| row.delivered == 1));
    assert_eq!(stream.read(0, 0).len(), first);
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
                .any(|stream| has_arrays(stream) && policies(stream).contains(&policy)),
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
        settings: false,
        keys: false,
        identifiers: false,
        shared: false,
    };
    for seed in 0..100 {
        for stream in Workload::generate(&mut SplitMix64::new(seed), none).streams {
            assert!(
                stream.drift.is_empty() && !stream.json && !stream.sliced && !stream.normalized()
            );
            assert!(!stream.shared_keys && !stream.composite);
            assert!(
                stream
                    .key_types
                    .iter()
                    .flatten()
                    .all(|l| *l == LogicalType::Int64)
            );
        }
    }
    let unset = Features {
        drift: true,
        ..none
    };
    for seed in 0..100 {
        let workload = Workload::generate(&mut SplitMix64::new(seed), unset);
        assert_eq!(workload.pipeline, Level::default());
        for drift in workload.streams.iter().flat_map(|stream| &stream.drift) {
            assert_eq!(drift.settings, Level::default());
            assert!(drift.hint.is_none() && drift.declared.is_none());
            assert!(drift.name.is_ascii(), "{}", drift.name);
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

#[test]
fn a_span_holds_the_batches_between_the_checkpoints_around_a_row() {
    let stream = streams()
        .into_iter()
        .find(|stream| {
            stream.checkpointing == rdlt_connector::Checkpointing::Natural
                && stream.checkpoint_every > 1
                && stream.batches(0, 0).len() > usize::try_from(stream.checkpoint_every).unwrap()
        })
        .expect("a stream checkpointing every few batches");
    let every = usize::try_from(stream.checkpoint_every).unwrap();
    let batches = stream.batches(0, 0);
    let span = stream.span(0, 0, batches[every].start);
    assert_eq!(
        span.start, batches[every].start,
        "the span starts at a checkpoint"
    );
    let last = batches.len().min(2 * every) - 1;
    assert_eq!(span.end, batches[last].end, "and ends at the next");
    let on_demand = streams()
        .into_iter()
        .find(|stream| {
            stream.checkpointing == rdlt_connector::Checkpointing::OnDemand
                && !stream.batches(0, 0).is_empty()
        })
        .expect("a stream checkpointing on demand");
    let batches = on_demand.batches(0, 0);
    let span = on_demand.span(0, 0, batches[0].start);
    assert_eq!(
        span,
        batches[0].start..batches.last().unwrap().end,
        "anywhere in the read"
    );
}
