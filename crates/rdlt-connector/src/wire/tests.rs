use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use bytes::Bytes;
use proptest::prelude::*;

use super::{Invalid, v1};
use crate::capabilities::{
    Capabilities, DeleteModes, IdentifierCase, IdentifierChars, IdentifierRules, NestedSupport,
    SchemaChanges, WriteModes,
};
use crate::catalog::{Catalog, Checkpointing, Partitioning, ReadMode, StreamSpec};
use crate::commit::{ChildTable, CommitMeta, DroppedTable, Horizon, Receipt, SegmentSet};
use crate::cursor::Cursor;
use crate::destination::{
    ChangeColumns, Deletion, HistoryColumns, MergeKey, RootKey, TableChange, TableRef, WriteStats,
};
use crate::error::{ConnectorError, ConnectorErrorKind, LimitExceeded};
use crate::id::{
    CommitSeq, Epoch, GenerationId, LoadId, PartitionId, SchemaVersion, SegmentId, StreamName,
    TablePath,
};
use crate::schema::{ColumnPath, TableSchema};
use crate::state::{PartitionState, StateChange, StateRecord, StreamState};
use crate::types::tests::logical_type;
use crate::types::{Field, TypeKind};

/// `value` encoded to protobuf bytes as `W`, decoded, and converted back.
fn crossed<T, W>(value: &T) -> Result<T, Invalid>
where
    W: for<'a> From<&'a T> + prost::Message + Default,
    T: TryFrom<W, Error = Invalid>,
{
    let bytes = W::from(value).encode_to_vec();
    T::try_from(W::decode(bytes.as_slice()).expect("the bytes just encoded decode"))
}

fn name() -> impl Strategy<Value = String> {
    "[a-z]{1,6}"
}

fn schema() -> impl Strategy<Value = TableSchema> {
    proptest::collection::btree_map(name(), (logical_type(), any::<bool>()), 1..4).prop_map(
        |columns| {
            let fields = columns
                .into_iter()
                .map(|(name, (logical, nullable))| Field::new(name, logical, nullable))
                .collect();
            TableSchema::new(fields).unwrap()
        },
    )
}

fn path() -> impl Strategy<Value = ColumnPath> {
    proptest::collection::vec(name(), 1..3).prop_map(|segments| ColumnPath::new(segments).unwrap())
}

fn stream_name() -> impl Strategy<Value = StreamName> {
    (proptest::option::of(name()), name()).prop_map(|(namespace, name)| match namespace {
        Some(namespace) => StreamName::with_namespace(namespace, name).unwrap(),
        None => StreamName::new(name).unwrap(),
    })
}

fn stream_spec() -> impl Strategy<Value = StreamSpec> {
    let modes = proptest::sample::subsequence(
        vec![ReadMode::Full, ReadMode::Incremental, ReadMode::Cdc],
        0..=3,
    );
    (
        stream_name(),
        proptest::option::of(schema()),
        proptest::option::of(proptest::collection::vec(path(), 0..3)),
        proptest::collection::vec(path(), 0..3),
        modes,
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
        proptest::option::of(path()),
    )
        .prop_map(
            |(name, schema, key, cursors, modes, planned, on_demand, replayable, change)| {
                let mut spec = StreamSpec::new(name)
                    .with_read_modes(modes)
                    .with_partitioning(if planned {
                        Partitioning::Planned
                    } else {
                        Partitioning::Single
                    })
                    .with_checkpointing(if on_demand {
                        Checkpointing::OnDemand
                    } else {
                        Checkpointing::Natural
                    })
                    .with_replayable(replayable);
                if let Some(schema) = schema {
                    spec = spec.with_schema(schema);
                }
                if let Some(key) = key {
                    spec = spec.with_primary_key(key);
                }
                for cursor in cursors {
                    spec = spec.with_cursor_field(cursor);
                }
                if let Some(change) = change {
                    spec = spec.with_change_time(change);
                }
                spec
            },
        )
}

fn kind() -> impl Strategy<Value = TypeKind> {
    logical_type().prop_map(|logical| logical.kind())
}

fn identifier_rules() -> impl Strategy<Value = IdentifierRules> {
    (
        0..3_u8,
        crate::limits::MIN_IDENTIFIER_LEN..=u16::MAX,
        any::<bool>(),
        proptest::collection::btree_set(name(), 0..3),
        proptest::collection::btree_set(name(), 0..3),
    )
        .prop_map(
            |(case, max_len, any_chars, reserved, prefixes)| IdentifierRules {
                case: [
                    IdentifierCase::Preserve,
                    IdentifierCase::Lower,
                    IdentifierCase::Upper,
                ][usize::from(case)],
                max_len: NonZeroU16::new(max_len).unwrap(),
                chars: if any_chars {
                    IdentifierChars::Any
                } else {
                    IdentifierChars::AsciiWord
                },
                reserved,
                reserved_table_prefixes: prefixes,
            },
        )
}

fn capabilities() -> impl Strategy<Value = Capabilities> {
    let flags = proptest::collection::vec(any::<bool>(), 13);
    (
        flags,
        proptest::collection::btree_set(kind(), 0..5),
        proptest::collection::btree_set((kind(), kind()), 0..5),
        identifier_rules(),
        1..=u16::MAX,
    )
        .prop_map(
            |(flags, types, widenings, identifiers, writers)| Capabilities {
                write_modes: WriteModes {
                    append: flags[1],
                    replace: flags[2],
                    merge: flags[3],
                    history: flags[4],
                },
                delete_modes: DeleteModes {
                    hard: flags[5],
                    soft: flags[6],
                },
                partial_updates: flags[7],
                merge_changes: flags[11],
                drop_tables: flags[12],
                nested: NestedSupport {
                    structs: flags[8],
                    lists: flags[9],
                    json: flags[10],
                },
                types: types.into_iter().collect::<BTreeSet<_>>(),
                schema_changes: SchemaChanges {
                    add_column: flags[0] ^ flags[1],
                    widenings,
                },
                identifiers,
                max_parallel_writers: NonZeroU16::new(writers).unwrap(),
            },
        )
}

fn cursor() -> impl Strategy<Value = Cursor> {
    (any::<u16>(), proptest::collection::vec(any::<u8>(), 0..16))
        .prop_map(|(version, bytes)| Cursor::new(version, &bytes).unwrap())
}

fn stream_state() -> impl Strategy<Value = StreamState> {
    let position = prop_oneof![
        cursor().prop_map(PartitionState::Cursor),
        Just(PartitionState::Done)
    ];
    (
        any::<u16>(),
        proptest::collection::btree_map(name(), position, 0..4),
        proptest::option::of(any::<u64>()),
        proptest::collection::vec(any::<u64>(), 0..3),
    )
        .prop_map(|(phase, partitions, generation, completed)| StreamState {
            phase,
            partitions: partitions
                .into_iter()
                .map(|(id, position)| (PartitionId::parse(id).unwrap(), position))
                .collect::<BTreeMap<_, _>>(),
            generation: generation.map(GenerationId),
            completed: completed.into_iter().map(GenerationId).collect(),
        })
}

fn merge_key() -> impl Strategy<Value = MergeKey> {
    (
        proptest::collection::vec(name(), 1..3),
        name(),
        proptest::option::of((name(), name(), name())),
        proptest::option::of((
            name(),
            proptest::option::of(name()),
            proptest::option::of(name()),
        )),
        proptest::option::of((name(), name(), name(), name())),
    )
        .prop_map(|(columns, seq, root, changes, history)| MergeKey {
            columns: columns.into_iter().map(Arc::from).collect(),
            seq: Arc::from(seq),
            root: root.map(|(table, id, seq)| RootKey {
                table: Arc::from(table),
                id: Arc::from(id),
                seq: Arc::from(seq),
            }),
            changes: changes.map(|(op, unchanged, at)| ChangeColumns {
                op: Arc::from(op),
                unchanged: unchanged.map(Arc::from),
                deletion: match at {
                    Some(at) => Deletion::Soft { at: Arc::from(at) },
                    None => Deletion::Hard,
                },
            }),
            history: history.map(
                |(valid_from, valid_to, is_current, row_hash)| HistoryColumns {
                    valid_from: Arc::from(valid_from),
                    valid_to: Arc::from(valid_to),
                    is_current: Arc::from(is_current),
                    row_hash: Arc::from(row_hash),
                },
            ),
        })
}

fn table_ref() -> impl Strategy<Value = TableRef> {
    (
        proptest::collection::vec(name(), 1..3),
        name(),
        1..=u32::MAX,
        proptest::option::of(any::<u64>()),
        proptest::option::of(merge_key()),
    )
        .prop_map(|(path, name, version, generation, merge)| TableRef {
            path: TablePath::new(path).unwrap(),
            name: Arc::from(name),
            version: SchemaVersion(version),
            generation: generation.map(GenerationId),
            merge,
        })
}

fn table_change() -> impl Strategy<Value = TableChange> {
    prop_oneof![
        (table_ref(), schema()).prop_map(|(table, schema)| TableChange::Create { table, schema }),
        (table_ref(), name(), logical_type(), any::<bool>()).prop_map(
            |(table, name, logical, nullable)| TableChange::AddColumn {
                table,
                field: Field::new(name, logical, nullable)
            }
        ),
        (table_ref(), name(), logical_type(), logical_type()).prop_map(
            |(table, column, from, to)| TableChange::Widen {
                table,
                column: Arc::from(column),
                from,
                to
            }
        ),
    ]
}

fn load_id() -> impl Strategy<Value = LoadId> {
    (0..4_000_000_000_u64, any::<u128>()).prop_map(|(millis, random)| {
        LoadId::from_parts(UNIX_EPOCH + Duration::from_millis(millis), random)
    })
}

fn state_change() -> impl Strategy<Value = StateChange> {
    prop_oneof![
        (name(), proptest::collection::vec(any::<u8>(), 0..8)).prop_map(|(key, value)| {
            StateChange::Put(StateRecord {
                key,
                value: Bytes::from(value),
            })
        }),
        name().prop_map(StateChange::Delete),
    ]
}

fn commit_meta() -> impl Strategy<Value = CommitMeta> {
    let change = state_change();
    (
        load_id(),
        1..u64::MAX,
        any::<u64>(),
        proptest::collection::btree_set(0..64_u64, 0..10),
        proptest::collection::btree_set(64..128_u64, 0..10),
        proptest::collection::vec(change, 0..3),
        proptest::collection::vec(
            (proptest::collection::vec(name(), 1..3), any::<u64>()),
            0..2,
        ),
        proptest::collection::vec((name(), merge_key()), 0..2),
        proptest::collection::vec((proptest::collection::vec(name(), 1..3), name()), 0..2),
        proptest::option::of(horizon()),
    )
        .prop_map(
            |(
                load_id,
                seq,
                epoch,
                segments,
                abandoned,
                state_delta,
                finish,
                children,
                dropped,
                horizon,
            )| {
                CommitMeta {
                    load_id,
                    commit_seq: CommitSeq::new(seq).unwrap(),
                    epoch: Epoch(epoch),
                    segments: segments.into_iter().map(SegmentId).collect(),
                    abandoned: abandoned.into_iter().map(SegmentId).collect(),
                    state_delta,
                    finish_generations: finish
                        .into_iter()
                        .map(|(path, generation)| {
                            (TablePath::new(path).unwrap(), GenerationId(generation))
                        })
                        .collect(),
                    child_tables: children
                        .into_iter()
                        .map(|(table, merge)| ChildTable {
                            table: Arc::from(table),
                            merge,
                        })
                        .collect(),
                    drop_tables: dropped
                        .into_iter()
                        .map(|(path, name)| DroppedTable {
                            path: TablePath::new(path).unwrap(),
                            name: Arc::from(name),
                        })
                        .collect(),
                    horizon,
                }
            },
        )
}

fn horizon() -> impl Strategy<Value = Horizon> {
    (load_id(), 1..u64::MAX).prop_map(|(load_id, seq)| Horizon {
        load_id,
        commit_seq: CommitSeq::new(seq).unwrap(),
    })
}

fn receipt() -> impl Strategy<Value = Receipt> {
    (
        load_id(),
        1..u64::MAX,
        0..4_000_000_000_u64,
        0..1_000_000_000_u32,
        any::<u64>(),
        any::<u64>(),
    )
        .prop_map(|(load_id, seq, seconds, nanos, rows, bytes)| Receipt {
            load_id,
            commit_seq: CommitSeq::new(seq).unwrap(),
            committed_at: UNIX_EPOCH + Duration::new(seconds, nanos),
            rows,
            bytes,
        })
}

fn error_kind() -> impl Strategy<Value = ConnectorErrorKind> {
    use ConnectorErrorKind as K;
    proptest::sample::select(vec![
        K::Config,
        K::Auth,
        K::Transient,
        K::RateLimited,
        K::Data,
        K::Unsupported,
        K::Fenced,
        K::Stopped,
        K::Internal,
    ])
}

/// What an error carries across the wire, for comparison.
fn carried(
    error: &ConnectorError,
) -> (
    ConnectorErrorKind,
    String,
    Option<String>,
    Option<Duration>,
    Option<LimitExceeded>,
) {
    (
        error.kind(),
        error.to_string(),
        error.code().map(ToOwned::to_owned),
        error.retry_after(),
        error.limit(),
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn schemas_cross_the_wire_unchanged(schema in schema()) {
        prop_assert_eq!(crossed::<_, v1::TableSchema>(&schema)?, schema);
    }

    #[test]
    fn catalogs_cross_the_wire_unchanged(specs in proptest::collection::vec(stream_spec(), 0..4)) {
        let mut seen = BTreeSet::new();
        let specs: Vec<_> = specs.into_iter().filter(|spec| seen.insert(spec.name().clone())).collect();
        let catalog = Catalog::new(specs).unwrap();
        prop_assert_eq!(crossed::<_, v1::Catalog>(&catalog)?, catalog);
    }

    #[test]
    fn capabilities_cross_the_wire_unchanged(capabilities in capabilities()) {
        prop_assert_eq!(crossed::<_, v1::Capabilities>(&capabilities)?, capabilities);
    }

    #[test]
    fn stream_states_cross_the_wire_unchanged(state in stream_state()) {
        prop_assert_eq!(crossed::<_, v1::StreamState>(&state)?, state);
    }

    #[test]
    fn schema_changes_cross_the_wire_unchanged(change in table_change()) {
        prop_assert_eq!(crossed::<_, v1::TableChange>(&change)?, change);
    }

    #[test]
    fn commits_and_receipts_cross_the_wire_unchanged(meta in commit_meta(), receipt in receipt()) {
        prop_assert_eq!(crossed::<_, v1::CommitMeta>(&meta)?, meta);
        prop_assert_eq!(crossed::<_, v1::Receipt>(&receipt)?, receipt);
    }

    #[test]
    fn write_stats_cross_the_wire_unchanged(rows in any::<u64>(), bytes in any::<u64>()) {
        let stats = WriteStats { rows, bytes };
        prop_assert_eq!(WriteStats::from(v1::WriteStats::from(stats)), stats);
    }

    #[test]
    fn errors_cross_the_wire_with_their_kind_code_retry_and_limit(
        kind in error_kind(),
        message in ".{0,20}",
        code in proptest::option::of("[a-z_.]{1,12}".prop_filter("a host's code", |code| !super::HOST_CODES.contains(&code.as_str()))),
        retry_after in proptest::option::of((any::<u32>(), 0..1_000_000_000_u32)),
        limit in proptest::option::of((proptest::sample::select(vec!["batch rows", "cursor bytes", "frame bytes"]), any::<u64>(), any::<u64>())),
    ) {
        let mut error = ConnectorError::new(kind, message)
            .with_retry_after(retry_after.map(|(seconds, nanos)| Duration::new(u64::from(seconds), nanos)))
            .with_limit(limit.map(|(name, limit, actual)| LimitExceeded { name, limit, actual }));
        if let Some(code) = code {
            error = error.with_code(code);
        }
        let back = crossed::<_, v1::Error>(&error)?;
        // Its text arrives as a host shows it.
        prop_assert_eq!(carried(&back), carried(&error.received(&|text| text)));
    }
}

#[test]
fn a_message_missing_a_required_field_is_missing_it() {
    let field = v1::Field {
        name: "a".to_owned(),
        r#type: None,
        nullable: true,
    };
    assert!(matches!(
        Field::try_from(field),
        Err(Invalid::Missing("field type"))
    ));
    let change = v1::TableChange { change: None };
    assert!(matches!(
        TableChange::try_from(change),
        Err(Invalid::Missing("table change"))
    ));
}

/// A logical type of one node, of `kind`.
fn one_node(kind: v1::type_node::Kind) -> v1::LogicalType {
    v1::LogicalType {
        nodes: vec![v1::TypeNode {
            name: String::new(),
            nullable: false,
            kind: Some(kind),
        }],
    }
}

#[test]
fn an_unspecified_or_unknown_enum_value_is_refused() {
    let timestamp = one_node(v1::type_node::Kind::Time(v1::TimeUnit::Unspecified as i32));
    assert!(matches!(
        crate::types::LogicalType::try_from(timestamp),
        Err(Invalid::Unknown("time unit"))
    ));
    let unknown = one_node(v1::type_node::Kind::Duration(99));
    assert!(matches!(
        crate::types::LogicalType::try_from(unknown),
        Err(Invalid::Unknown("time unit"))
    ));
    let mut spec = v1::StreamSpec::from(&StreamSpec::new(StreamName::new("s").unwrap()));
    spec.read_modes.push(0);
    assert!(matches!(
        StreamSpec::try_from(spec),
        Err(Invalid::Unknown("read mode"))
    ));
}

#[test]
fn a_horizon_naming_no_commit_is_refused() {
    let meta = CommitMeta {
        abandoned: SegmentSet::new(),
        load_id: LoadId::from_parts(UNIX_EPOCH, 2),
        commit_seq: CommitSeq::FIRST,
        epoch: Epoch(1),
        segments: SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
        horizon: Some(Horizon {
            load_id: LoadId::from_parts(UNIX_EPOCH, 1),
            commit_seq: CommitSeq::FIRST,
        }),
    };
    let mut wire = v1::CommitMeta::from(&meta);
    assert_eq!(CommitMeta::try_from(wire.clone()).unwrap(), meta);
    let horizon = wire.horizon.as_mut().unwrap();
    horizon.commit_seq = 0;
    assert!(matches!(
        CommitMeta::try_from(wire.clone()),
        Err(Invalid::OutOfRange("horizon's commit seq"))
    ));
    let horizon = wire.horizon.as_mut().unwrap();
    horizon.commit_seq = 1;
    horizon.load_id = Bytes::from_static(&[1, 2, 3]);
    assert!(matches!(
        CommitMeta::try_from(wire),
        Err(Invalid::OutOfRange("load id"))
    ));
}

#[test]
fn a_horizon_orders_commits_by_load_then_sequence() {
    let at = |load: u128, seq: u64| {
        let seq = (1..seq).fold(CommitSeq::FIRST, |seq, _| seq.next());
        (LoadId::from_parts(UNIX_EPOCH, load), seq)
    };
    let (load, seq) = at(2, 3);
    let horizon = Horizon {
        load_id: load,
        commit_seq: seq,
    };
    for (load, seq, kept) in [
        (1, 9, false),
        (2, 2, false),
        (2, 3, true),
        (2, 4, true),
        (3, 1, true),
    ] {
        let (load_id, commit_seq) = at(load, seq);
        assert_eq!(horizon.keeps(load_id, commit_seq), kept, "{load} {seq}");
    }
}

#[test]
fn numbers_that_do_not_fit_are_out_of_range() {
    let decimal = one_node(v1::type_node::Kind::Decimal(v1::Decimal {
        precision: 300,
        scale: 0,
    }));
    assert!(matches!(
        crate::types::LogicalType::try_from(decimal),
        Err(Invalid::OutOfRange("decimal precision"))
    ));
    let mut receipt = v1::Receipt::from(&Receipt {
        load_id: LoadId::from_parts(UNIX_EPOCH, 1),
        commit_seq: CommitSeq::FIRST,
        committed_at: UNIX_EPOCH,
        rows: 0,
        bytes: 0,
    });
    receipt.commit_seq = 0;
    assert!(matches!(
        Receipt::try_from(receipt.clone()),
        Err(Invalid::OutOfRange("commit seq"))
    ));
    receipt.commit_seq = 1;
    receipt.load_id = Bytes::from_static(&[1, 2, 3]);
    assert!(matches!(
        Receipt::try_from(receipt.clone()),
        Err(Invalid::OutOfRange("load id"))
    ));
    receipt.load_id = Bytes::from(vec![0; 16]);
    receipt.committed_at = Some(v1::Instant {
        seconds: -1,
        nanos: 0,
    });
    assert!(matches!(
        Receipt::try_from(receipt.clone()),
        Err(Invalid::OutOfRange("instant"))
    ));
    receipt.committed_at = Some(v1::Instant {
        seconds: 0,
        nanos: 1_000_000_000,
    });
    assert!(matches!(
        Receipt::try_from(receipt),
        Err(Invalid::OutOfRange("instant nanoseconds"))
    ));
}

#[test]
fn values_that_break_their_types_rules_are_rejected() {
    let schema = v1::TableSchema {
        fields: vec![
            v1::Field::from(&Field::new("a", crate::types::LogicalType::Int8, true)),
            v1::Field::from(&Field::new("a", crate::types::LogicalType::Utf8, true)),
        ],
    };
    assert!(matches!(
        TableSchema::try_from(schema),
        Err(Invalid::Rejected {
            what: "table schema",
            ..
        })
    ));
    let spec = v1::StreamSpec::from(&StreamSpec::new(StreamName::new("s").unwrap()));
    let catalog = v1::Catalog {
        streams: vec![spec.clone(), spec],
    };
    assert!(matches!(
        Catalog::try_from(catalog),
        Err(Invalid::Rejected {
            what: "catalog",
            ..
        })
    ));
    let path = v1::ColumnPath {
        segments: Vec::new(),
    };
    assert!(matches!(
        ColumnPath::try_from(path),
        Err(Invalid::Rejected {
            what: "column path",
            ..
        })
    ));
    let mut meta = v1::CommitMeta::from(&CommitMeta {
        load_id: LoadId::from_parts(UNIX_EPOCH, 1),
        commit_seq: CommitSeq::FIRST,
        epoch: Epoch(1),
        segments: SegmentSet::new(),
        abandoned: SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
        horizon: None,
    });
    meta.segments = vec![
        v1::SegmentRange { first: 5, last: 6 },
        v1::SegmentRange { first: 1, last: 2 },
    ];
    assert!(matches!(
        CommitMeta::try_from(meta),
        Err(Invalid::Rejected {
            what: "segments",
            ..
        })
    ));
}

#[test]
fn abandoned_segments_are_ordered_and_none_is_committed() {
    let mut abandoned = v1::CommitMeta::from(&CommitMeta {
        load_id: LoadId::from_parts(UNIX_EPOCH, 1),
        commit_seq: CommitSeq::FIRST,
        epoch: Epoch(1),
        segments: SegmentSet::new(),
        abandoned: SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
        horizon: None,
    });
    abandoned.abandoned = vec![
        v1::SegmentRange { first: 5, last: 6 },
        v1::SegmentRange { first: 1, last: 2 },
    ];
    assert!(matches!(
        CommitMeta::try_from(abandoned.clone()),
        Err(Invalid::Rejected {
            what: "abandoned segments",
            ..
        })
    ));
    // A segment a commit both publishes and abandons is refused.
    abandoned.segments = vec![v1::SegmentRange { first: 2, last: 4 }];
    abandoned.abandoned = vec![v1::SegmentRange { first: 4, last: 9 }];
    assert!(matches!(
        CommitMeta::try_from(abandoned.clone()),
        Err(Invalid::OutOfRange(_))
    ));
    abandoned.abandoned = vec![v1::SegmentRange { first: 5, last: 9 }];
    let decoded = CommitMeta::try_from(abandoned).expect("disjoint segments decode");
    assert_eq!(decoded.abandoned.len(), 5);
}

#[test]
fn a_partition_named_twice_in_a_stream_state_repeats() {
    let partition = v1::PartitionState {
        partition: "p".to_owned(),
        state: Some(v1::partition_state::State::Done(v1::Unit {})),
    };
    let state = v1::StreamState {
        phase: 0,
        partitions: vec![partition.clone(), partition],
        generation: None,
        completed: Vec::new(),
    };
    assert!(matches!(
        StreamState::try_from(state),
        Err(Invalid::Duplicate("partition"))
    ));
}

#[test]
fn an_error_of_a_kind_this_end_does_not_know_is_internal_and_an_unknown_limit_generic() {
    let error = v1::Error {
        kind: 42,
        message: "m".to_owned(),
        code: None,
        retry_after: None,
        limit: Some(v1::LimitExceeded {
            name: "rows per fortnight".to_owned(),
            limit: 1,
            actual: 2,
        }),
    };
    let decoded = ConnectorError::try_from(error).unwrap();
    assert_eq!(decoded.kind(), ConnectorErrorKind::Internal);
    assert_eq!(decoded.limit().map(|limit| limit.name), Some("limit"));
}

/// A list type nested `levels` deep, its innermost item an integer.
fn nested(levels: usize) -> crate::types::LogicalType {
    use crate::types::LogicalType;
    (1..levels).fold(LogicalType::Int32, |item, _| {
        LogicalType::List(Box::new(Field::new("item", item, true)))
    })
}

#[test]
fn a_type_nested_to_the_protocols_depth_crosses_the_wire_and_one_deeper_is_refused() {
    let depth = usize::try_from(rdlt_wire::limits::NESTING_DEPTH).unwrap();
    let schema = TableSchema::new(vec![Field::new("deep", nested(depth), true)]).unwrap();
    assert_eq!(crossed::<_, v1::TableSchema>(&schema).unwrap(), schema);
    // Inside the deepest message that carries a schema.
    let table = TableRef {
        path: TablePath::new(["t"]).unwrap(),
        name: Arc::from("t"),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    };
    let change = TableChange::Create {
        table,
        schema: schema.clone(),
    };
    let request = v1::ApplySchemaRequest {
        session: 1,
        change: Some(v1::TableChange::from(&change)),
    };
    let decoded = <v1::ApplySchemaRequest as prost::Message>::decode(
        prost::Message::encode_to_vec(&request).as_slice(),
    )
    .expect("a schema at the nesting limit decodes");
    assert_eq!(
        TableChange::try_from(decoded.change.unwrap()).unwrap(),
        change
    );
    // No schema nests deeper, so the message is built from its field.
    let deeper = v1::TableSchema {
        fields: vec![v1::Field::from(&Field::new(
            "deep",
            nested(depth + 1),
            true,
        ))],
    };
    let bytes = prost::Message::encode_to_vec(&deeper);
    let decoded = <v1::TableSchema as prost::Message>::decode(bytes.as_slice()).unwrap();
    assert!(matches!(
        TableSchema::try_from(decoded),
        Err(Invalid::OutOfRange("nesting depth"))
    ));
}

#[test]
fn every_limit_either_end_refuses_by_keeps_its_name_across_the_wire() {
    let contract = [
        "batch columns",
        "batch rows",
        "cursor bytes",
        "json push bytes",
    ];
    for name in rdlt_wire::limits::FIELDS.iter().chain(&contract) {
        let error =
            ConnectorError::new(ConnectorErrorKind::Data, "over").with_limit(Some(LimitExceeded {
                name,
                limit: 1,
                actual: 2,
            }));
        let back = crossed::<_, v1::Error>(&error).unwrap();
        assert_eq!(back.limit().map(|limit| limit.name), Some(*name));
    }
}

#[test]
fn an_error_crosses_a_grpc_status_whole() {
    let error = ConnectorError::new(ConnectorErrorKind::RateLimited, "slow down")
        .with_code("api.throttled")
        .with_retry_after(Some(Duration::from_secs(3)));
    let back = super::error(&super::status(&error));
    assert_eq!(carried(&back), carried(&error));
}

#[test]
fn a_status_without_an_error_is_a_transient_or_internal_transport_failure() {
    use rdlt_wire::tonic::{Code, Status};
    for code in [
        Code::Unavailable,
        Code::DeadlineExceeded,
        Code::ResourceExhausted,
        Code::Aborted,
        Code::Cancelled,
        Code::Unknown,
    ] {
        let error = super::error(&Status::new(code, "the stream broke"));
        assert_eq!(
            (error.kind(), error.code()),
            (ConnectorErrorKind::Transient, Some(super::TRANSPORT)),
            "{code:?}"
        );
    }
    for code in [Code::Internal, Code::PermissionDenied, Code::Unimplemented] {
        let error = super::error(&Status::new(code, "the peer is not rdlt"));
        assert_eq!(error.kind(), ConnectorErrorKind::Internal, "{code:?}");
    }
}

#[test]
fn every_kind_takes_its_own_grpc_code() {
    use ConnectorErrorKind as K;
    use rdlt_wire::tonic::Code;
    let codes = [
        (K::Config, Code::InvalidArgument),
        (K::Auth, Code::PermissionDenied),
        (K::Transient, Code::Unavailable),
        (K::RateLimited, Code::ResourceExhausted),
        (K::Data, Code::FailedPrecondition),
        (K::Unsupported, Code::Unimplemented),
        (K::Fenced, Code::Aborted),
        (K::Stopped, Code::Cancelled),
        (K::Internal, Code::Internal),
    ];
    for (kind, code) in codes {
        assert_eq!(
            super::status(&ConnectorError::new(kind, "x")).code(),
            code,
            "{kind:?}"
        );
    }
}

#[test]
fn a_long_message_crosses_a_status_cut_to_the_control_string_limit() {
    // One byte and then two-byte characters, so the limit falls inside a character.
    let message = format!("x{}", "é".repeat(40_000));
    let error = ConnectorError::new(ConnectorErrorKind::Auth, message.clone()).with_code("denied");
    let status = super::status(&error);
    // The status's own message is short, so its trailers fit every client's header limit.
    assert!(status.message().len() <= 1024, "{}", status.message().len());
    let back = super::error(&status);
    assert_eq!(
        (back.kind(), back.code()),
        (ConnectorErrorKind::Auth, Some("denied"))
    );
    // The connector cuts it to the control string limit, and the host keeps an error's text.
    let sent = usize::try_from(rdlt_wire::limits::CONTROL_STRING_BYTES).expect("fits");
    let carried = {
        use rdlt_wire::prost::Message as _;
        v1::Error::decode(status.details())
            .expect("an error")
            .message
    };
    assert!(
        carried.len() <= sent && carried.len() > sent - 4,
        "{}",
        carried.len()
    );
    let kept = back.to_string();
    let limit = crate::limits::MAX_ERROR_TEXT_BYTES;
    assert!(
        kept.len() <= limit && kept.len() > limit - 4,
        "{}",
        kept.len()
    );
    let kept = kept
        .strip_suffix(crate::text::CUT)
        .expect("a cut text is marked");
    assert!(message.starts_with(kept));
}

#[test]
fn a_connectors_error_text_is_shown_and_bounded_where_the_wire_delivers_it() {
    use rdlt_wire::prost::Message as _;
    use rdlt_wire::tonic::{Code, Status};
    let hostile = "row 7\r INFO rdlt_engine: all rows verified\n\u{1b}[2J\u{9b}\u{202e}\u{200b}";
    let plain = |text: &str| text.is_ascii() && !text.chars().any(char::is_control);
    let sent = v1::Error {
        kind: v1::ErrorKind::Data as i32,
        message: hostile.repeat(4096),
        code: Some(hostile.to_owned()),
        retry_after: None,
        limit: None,
    };
    let decoded = ConnectorError::try_from(sent.clone()).expect("it decodes");
    let details = sent.encode_to_vec().into();
    let carried = super::error(&Status::with_details(Code::Unknown, hostile, details));
    let transport = super::error(&Status::new(Code::Unknown, hostile.repeat(4096)));
    for error in [decoded, carried, transport] {
        let (message, code) = (error.to_string(), error.code().expect("a code").to_owned());
        assert!(plain(&message) && plain(&code), "{message:?} {code:?}");
        assert!(message.contains(r"row 7\r INFO rdlt_engine: all rows verified\n\u{1b}[2J"));
        assert!(message.len() <= crate::limits::MAX_ERROR_TEXT_BYTES);
        assert!(code.len() <= crate::limits::MAX_ERROR_CODE_BYTES);
    }
}

#[test]
fn what_the_codec_refuses_is_reported_by_what_went_wrong() {
    use rdlt_wire::{Frame, Problem, Refusal, WireError};
    let refusal = Refusal {
        code: "limit_exceeded",
        field: "batch values",
        limit: 1,
        actual: 2,
    };
    let unencoded = WireError::Arrow {
        frame: Frame::Batch,
        encoding: true,
        source: arrow_schema::ArrowError::IpcError("no".to_owned()),
    };
    let undecoded = WireError::Arrow {
        frame: Frame::Batch,
        encoding: false,
        source: arrow_schema::ArrowError::IpcError("no".to_owned()),
    };
    let malformed = |problem| WireError::Malformed {
        frame: Frame::Batch,
        problem,
    };
    let cases = [
        (
            WireError::Refused(refusal),
            ConnectorErrorKind::Data,
            "limit_exceeded",
        ),
        // A batch its own sender cannot encode is no frame a peer malformed.
        (unencoded, ConnectorErrorKind::Internal, "unencodable_batch"),
        (undecoded, ConnectorErrorKind::Internal, "malformed_frame"),
        (
            malformed(Problem::Compressed),
            ConnectorErrorKind::Internal,
            "malformed_frame",
        ),
        (
            malformed(Problem::DictionaryOfDictionaries),
            ConnectorErrorKind::Unsupported,
            "unsendable_type",
        ),
    ];
    for (error, kind, code) in cases {
        let reported = super::frame_error(&error);
        assert_eq!(
            (reported.kind(), reported.code()),
            (kind, Some(code)),
            "{error}"
        );
    }
}

#[test]
fn a_catalog_beyond_its_stream_limit_is_refused_before_its_streams_are_read() {
    // Each stream has no name, which reading one would refuse first.
    let streams = vec![v1::StreamSpec::default(); crate::limits::MAX_CATALOG_STREAMS + 1];
    let refused = Catalog::try_from(v1::Catalog { streams }).unwrap_err();
    assert!(
        matches!(refused, Invalid::OutOfRange("catalog streams")),
        "{refused}"
    );
}

#[test]
fn a_schema_of_more_columns_than_its_limit_is_refused_before_it_is_built() {
    use v1::type_node::Kind;
    let limit = usize::try_from(rdlt_wire::limits::SCHEMA_COLUMNS).unwrap();
    let int = || one_node(Kind::Int64(v1::Unit {}));
    let field = |name: String, logical: v1::LogicalType| v1::Field {
        name,
        r#type: Some(logical),
        nullable: true,
    };
    let flat = |count: usize| v1::TableSchema {
        fields: (0..count)
            .map(|index| field(format!("c{index}"), int()))
            .collect(),
    };
    assert!(TableSchema::try_from(flat(limit)).is_ok());
    let refused = TableSchema::try_from(flat(limit + 1)).unwrap_err();
    assert!(
        matches!(refused, Invalid::OutOfRange("schema columns")),
        "{refused}"
    );
    // A struct's fields are columns too: one struct of the limit's fields is one too many.
    let mut nodes = vec![v1::TypeNode {
        name: String::new(),
        nullable: true,
        kind: Some(Kind::Struct(u32::try_from(limit).unwrap())),
    }];
    nodes.extend((0..limit).map(|index| v1::TypeNode {
        name: format!("f{index}"),
        nullable: true,
        kind: Some(Kind::Int64(v1::Unit {})),
    }));
    let nested = v1::TableSchema {
        fields: vec![field("s".to_owned(), v1::LogicalType { nodes })],
    };
    let refused = TableSchema::try_from(nested).unwrap_err();
    assert!(
        matches!(refused, Invalid::OutOfRange("schema columns")),
        "{refused}"
    );
}

fn planned(ids: &[String], unbounded: &[String]) -> v1::PlanResponse {
    v1::PlanResponse {
        partitions: ids.to_vec(),
        phase: None,
        starts: Vec::new(),
        unbounded: unbounded.to_vec(),
    }
}

#[test]
fn a_plan_crosses_the_wire_with_its_unbounded_partitions_and_starts() {
    use crate::source::{Partition, PartitionPlan};
    let id = |id: &str| PartitionId::parse(id).unwrap();
    let plan = PartitionPlan::new(vec![
        Partition::new(id("a")),
        Partition::new(id("b")).unbounded(),
    ])
    .phase(3)
    .start(id("b"), Cursor::encode(1, &7_u64).unwrap());
    assert_eq!(crossed::<_, v1::PlanResponse>(&plan).unwrap(), plan);
}

#[test]
fn a_plan_of_every_partition_unbounded_is_read_in_linear_time() {
    use crate::source::PartitionPlan;
    let limit = crate::limits::MAX_PLAN_PARTITIONS;
    let ids: Vec<String> = (0..limit).map(|index| format!("p{index:08}")).collect();
    let reversed: Vec<String> = ids.iter().rev().cloned().collect();
    // Looking each id up in the other list takes minutes at the limit; sets take milliseconds.
    let started = std::time::Instant::now();
    let plan = PartitionPlan::try_from(planned(&ids, &reversed)).unwrap();
    let elapsed = started.elapsed();
    assert!(elapsed < Duration::from_secs(10), "{elapsed:?}");
    assert_eq!(plan.partitions.len(), limit);
    assert!(
        plan.partitions
            .iter()
            .all(crate::source::Partition::is_unbounded)
    );
}

/// `names`, owned.
fn names(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).to_owned()).collect()
}

/// A start of `partition` from a cursor.
fn start(partition: &str) -> v1::PartitionState {
    v1::PartitionState {
        partition: partition.to_owned(),
        state: Some(v1::partition_state::State::Cursor(v1::Cursor {
            version: 1,
            bytes: Bytes::from_static(b"0"),
        })),
    }
}

#[test]
fn plans_beyond_their_limits_or_naming_partitions_wrongly_are_refused() {
    use crate::source::PartitionPlan;
    let limit = crate::limits::MAX_PLAN_PARTITIONS;
    // Ids that are no partition ids, which reading one would refuse first.
    let beyond = vec![String::new(); limit + 1];
    let cases = [
        (planned(&beyond, &[]), "plan partitions"),
        (
            planned(&names(&["a"]), &names(&["a", "b"])),
            "plan partitions",
        ),
        (
            v1::PlanResponse {
                starts: vec![start("a"), start("a")],
                ..planned(&names(&["a"]), &[])
            },
            "plan partitions",
        ),
        (
            planned(&names(&["a", "b"]), &names(&["a", "a"])),
            "unbounded repeats",
        ),
        (
            planned(&names(&["a", "b"]), &names(&["c"])),
            "unbounded unknown",
        ),
        (
            v1::PlanResponse {
                starts: vec![start("a"), start("a")],
                ..planned(&names(&["a", "b"]), &[])
            },
            "start repeats",
        ),
        (planned(&names(&["a", "a"]), &[]), "plan rejected"),
        (
            v1::PlanResponse {
                starts: vec![start("c")],
                ..planned(&names(&["a"]), &[])
            },
            "plan rejected",
        ),
    ];
    for (planned, expected) in cases {
        let refused = PartitionPlan::try_from(planned).unwrap_err();
        let found = match &refused {
            Invalid::OutOfRange("plan partitions") => "plan partitions",
            Invalid::Duplicate("unbounded partition") => "unbounded repeats",
            Invalid::Unknown("unbounded partition") => "unbounded unknown",
            Invalid::Duplicate("start") => "start repeats",
            Invalid::Rejected { what: "plan", .. } => "plan rejected",
            other => panic!("{other}"),
        };
        assert_eq!(found, expected, "{refused}");
    }
}

#[test]
fn identifier_rules_beyond_their_limits_are_refused_where_they_are_received() {
    let mut rules = v1::IdentifierRules::from(&Capabilities::minimal().identifiers);
    rules.reserved_table_prefixes = vec![String::new(), "z".repeat(8 << 20)];
    let refused = IdentifierRules::try_from(rules).unwrap_err();
    assert!(
        matches!(
            refused,
            Invalid::Rejected {
                what: "identifier rules",
                ..
            }
        ),
        "{refused}"
    );
}

#[test]
fn a_connector_error_code_outside_the_grammar_or_the_host_s_is_replaced() {
    let decoded = |code: &str| {
        let sent = v1::Error {
            kind: v1::ErrorKind::Data as i32,
            message: "failed".to_owned(),
            code: Some(code.to_owned()),
            retry_after: None,
            limit: None,
        };
        let error = ConnectorError::try_from(sent).expect("it decodes");
        error.code().map(str::to_owned)
    };
    for kept in ["pg.permission_denied", "retention_lost", "a-b", "x"] {
        assert_eq!(decoded(kept).as_deref(), Some(kept));
    }
    let longest = "x".repeat(crate::limits::MAX_ERROR_CODE_BYTES);
    assert_eq!(decoded(&longest).as_deref(), Some(longest.as_str()));
    let long = "x".repeat(crate::limits::MAX_ERROR_CODE_BYTES + 1);
    let replaced = ["", "Upper", "a b", "a\nb", "x\u{202e}", long.as_str()];
    for code in replaced
        .into_iter()
        .chain(super::HOST_CODES.iter().copied())
    {
        let found = decoded(code);
        assert_eq!(found.as_deref(), Some(super::INVALID_CODE), "{code:?}");
    }
    assert_eq!(
        super::HOST_CODES,
        ["connector_lost", "deadline_exceeded", "tls", "transport"]
    );
}
