use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use bytes::Bytes;
use proptest::prelude::*;

use super::{Invalid, v1};
use crate::capabilities::{
    Capabilities, CommitKind, DeleteModes, IdentifierCase, IdentifierChars, IdentifierRules,
    NestedSupport, SchemaChanges, WriteModes,
};
use crate::catalog::{Catalog, Checkpointing, Partitioning, ReadMode, StreamSpec};
use crate::commit::{ChildTable, CommitMeta, Receipt, SegmentSet};
use crate::cursor::Cursor;
use crate::destination::{MergeKey, RootKey, TableChange, TableRef, WriteStats};
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
        1..=u16::MAX,
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
    let flags = proptest::collection::vec(any::<bool>(), 11);
    (
        flags,
        proptest::collection::btree_set(kind(), 0..5),
        proptest::collection::btree_set((kind(), kind()), 0..5),
        identifier_rules(),
        1..=u16::MAX,
        proptest::option::of(any::<u64>()),
    )
        .prop_map(
            |(flags, types, widenings, identifiers, writers, preferred)| Capabilities {
                commit: if flags[0] {
                    CommitKind::Manifest
                } else {
                    CommitKind::Transactional
                },
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
                preferred_batch_bytes: preferred,
            },
        )
}

fn cursor() -> impl Strategy<Value = Cursor> {
    (any::<u16>(), proptest::collection::vec(any::<u8>(), 0..16))
        .prop_map(|(version, bytes)| Cursor::new(version, Bytes::from(bytes)).unwrap())
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
    )
        .prop_map(|(columns, seq, root)| MergeKey {
            columns: columns.into_iter().map(Arc::from).collect(),
            seq: Arc::from(seq),
            root: root.map(|(table, id, seq)| RootKey {
                table: Arc::from(table),
                id: Arc::from(id),
                seq: Arc::from(seq),
            }),
        })
}

fn table_ref() -> impl Strategy<Value = TableRef> {
    (
        proptest::collection::vec(name(), 1..3),
        name(),
        any::<u32>(),
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

fn commit_meta() -> impl Strategy<Value = CommitMeta> {
    let change = prop_oneof![
        (name(), proptest::collection::vec(any::<u8>(), 0..8)).prop_map(|(key, value)| {
            StateChange::Put(StateRecord {
                key,
                value: Bytes::from(value),
            })
        }),
        name().prop_map(StateChange::Delete),
    ];
    (
        load_id(),
        1..u64::MAX,
        any::<u64>(),
        proptest::collection::btree_set(0..64_u64, 0..10),
        proptest::collection::vec(change, 0..3),
        proptest::collection::vec(
            (proptest::collection::vec(name(), 1..3), any::<u64>()),
            0..2,
        ),
        proptest::collection::vec((name(), merge_key()), 0..2),
    )
        .prop_map(
            |(load_id, seq, epoch, segments, state_delta, finish, children)| {
                let mut set = SegmentSet::new();
                for segment in segments {
                    set.insert(SegmentId(segment));
                }
                CommitMeta {
                    load_id,
                    commit_seq: CommitSeq::new(seq).unwrap(),
                    epoch: Epoch(epoch),
                    segments: set,
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
                }
            },
        )
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
        code in proptest::option::of("[a-z_.]{1,12}"),
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
        prop_assert_eq!(carried(&back), carried(&error));
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
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
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
    let depth = usize::try_from(crate::limits::MAX_NESTING_DEPTH).unwrap();
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
    let deeper = TableSchema::new(vec![Field::new("deep", nested(depth + 1), true)]).unwrap();
    assert!(matches!(
        crossed::<_, v1::TableSchema>(&deeper),
        Err(Invalid::OutOfRange("nesting depth"))
    ));
}
