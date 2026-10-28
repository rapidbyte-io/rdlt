use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use bytes::Bytes;
use proptest::prelude::*;

use super::{
    Identity, NameConflict, NameMap, PartitionState, PipelineState, Sequences, StateChange,
    StateEntry, StateError, StateKey, StateRecord, StreamState, TableState, Writing,
};
use crate::commit::Receipt;
use crate::cursor::Cursor;
use crate::id::{
    CommitSeq, Epoch, GenerationId, LoadId, PartitionId, SchemaVersion, StreamName, TablePath,
};
use crate::schema::{ColumnKey, ColumnPath, TableSchema};
use crate::types::{Field, Fields, LogicalType, TypeKind};

fn stream(name: &str) -> StreamName {
    StreamName::new(name).unwrap()
}

fn partition(id: &str) -> PartitionId {
    PartitionId::parse(id).unwrap()
}

fn cursor(offset: u64) -> PartitionState {
    PartitionState::Cursor(Cursor::encode(1, &offset).unwrap())
}

/// The load of number `n`, loads of greater numbers later.
fn load(n: u64) -> LoadId {
    LoadId::from_parts(UNIX_EPOCH + Duration::from_secs(n), u128::from(n))
}

fn receipt() -> Receipt {
    Receipt {
        load_id: LoadId::from_parts(UNIX_EPOCH + Duration::from_secs(10), 7),
        commit_seq: CommitSeq::FIRST,
        committed_at: UNIX_EPOCH + Duration::from_secs(11),
        rows: 3,
        bytes: 40,
    }
}

fn sample_state() -> PipelineState {
    let mut state = PipelineState {
        epoch: Epoch(4),
        last_receipt: Some(receipt()),
        ..PipelineState::default()
    };
    let orders = state.streams.entry(stream("orders")).or_default();
    orders.phase = 1;
    orders.partitions.insert(partition("p0"), cursor(10));
    orders
        .partitions
        .insert(partition("p1"), PartitionState::Done);
    orders.generation = Some(GenerationId(2));
    for (id, number) in [("p0", 5), ("p1", 3)] {
        let position = (stream("orders"), partition(id));
        state.recorded_by.insert(position, load(number));
    }
    orders.completed = vec![GenerationId(0), GenerationId(1)];
    state.resets.insert(stream("orders"), Epoch(3));
    let mut names = NameMap::default();
    names.insert(ColumnPath::from("id"), "id").unwrap();
    let variant = ColumnKey::Variant {
        column: ColumnPath::from("id"),
        kind: TypeKind::Json,
    };
    names.insert(variant, "id__json").unwrap();
    let schema = TableSchema::new(vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("id__json", LogicalType::Json, true),
    ])
    .unwrap();
    state.tables.insert(
        TablePath::new(["orders"]).unwrap(),
        TableState {
            schema: Some((SchemaVersion(2), schema)),
            physical: Some("orders".into()),
            names,
            sequences: Some(Sequences::Source),
            history: true,
            key: vec![ColumnPath::from("id")],
            change_time: Some(ColumnPath::from("at")),
            identity: Identity::Position,
            exact: ["id".into()].into(),
        },
    );
    state
}

/// The records that store `state`, one a key.
fn records_of(state: &PipelineState) -> Vec<StateRecord> {
    let mut entries = vec![StateEntry::Epoch(state.epoch)];
    for (name, stream) in &state.streams {
        entries.push(StateEntry::Phase {
            stream: name.clone(),
            phase: stream.phase,
        });
        entries.extend(partition_entries(state, name, stream));
        if let Some(generation) = stream.generation {
            entries.push(StateEntry::Generation {
                stream: name.clone(),
                generation,
            });
        }
        if !stream.completed.is_empty() {
            entries.push(StateEntry::Completed {
                stream: name.clone(),
                generations: stream.completed.clone(),
            });
        }
    }
    for (stream, epoch) in &state.resets {
        entries.push(StateEntry::Reset {
            stream: stream.clone(),
            epoch: *epoch,
        });
    }
    for (path, table) in &state.tables {
        entries.extend(table_entries(path, table));
    }
    if let Some(receipt) = &state.last_receipt {
        entries.push(StateEntry::Receipt(receipt.clone()));
    }
    entries.iter().map(StateEntry::to_record).collect()
}

/// The entries of `stream`'s partitions, named `name`; a position no load is known to have
/// recorded, as in a state built by hand, records as the earliest load's.
fn partition_entries(
    state: &PipelineState,
    name: &StreamName,
    stream: &StreamState,
) -> Vec<StateEntry> {
    stream
        .partitions
        .iter()
        .map(|(partition, position)| {
            let load = state
                .recorded_by
                .get(&(name.clone(), partition.clone()))
                .copied()
                .unwrap_or_else(|| LoadId::from_parts(UNIX_EPOCH, 0));
            StateEntry::Partition {
                stream: name.clone(),
                partition: partition.clone(),
                state: position.clone(),
                load,
            }
        })
        .collect()
}

/// The entries that record `table`, at `path`.
fn table_entries(path: &TablePath, table: &TableState) -> Vec<StateEntry> {
    let mut entries = Vec::new();
    if let Some((version, schema)) = &table.schema {
        entries.push(StateEntry::Schema {
            table: path.clone(),
            version: *version,
            schema: schema.clone(),
            exact: table.exact.clone(),
        });
    }
    if let Some(physical) = &table.physical {
        entries.push(StateEntry::Names {
            table: path.clone(),
            physical: Arc::clone(physical),
            names: table.names.clone(),
        });
    }
    if let Some(sequences) = table.sequences {
        entries.push(StateEntry::Sequences {
            table: path.clone(),
            sequences,
            history: table.history,
            key: table.key.clone(),
            change_time: table.change_time.clone(),
            identity: table.identity,
        });
    }
    entries
}

#[test]
fn state_round_trips_through_records() {
    let state = sample_state();
    assert_eq!(
        PipelineState::from_records(&records_of(&state)).unwrap(),
        state
    );
    assert_eq!(
        PipelineState::from_records(&[]).unwrap(),
        PipelineState::default()
    );
}

#[test]
fn a_schema_record_without_its_exact_columns_is_refused_and_deleting_it_forgets_them() {
    let mut state = sample_state();
    let table = TablePath::new(["orders"]).unwrap();
    let key = StateKey::Schema(table.clone()).encode();
    let record = records_of(&state)
        .into_iter()
        .find(|record| record.key == key)
        .expect("the schema is recorded");
    let mut json: serde_json::Value = serde_json::from_slice(&record.value).unwrap();
    json["entry"]["schema"]
        .as_object_mut()
        .expect("a schema entry")
        .remove("exact")
        .expect("the exact columns are recorded");
    let lacking = StateRecord {
        key: record.key.clone(),
        value: versioned(super::STATE_VERSION, &json["entry"]),
    };
    assert!(matches!(
        sample_state().apply(&StateChange::Put(lacking)),
        Err(StateError::MalformedValue { .. })
    ));
    state.apply(&StateChange::Delete(key)).unwrap();
    assert!(state.tables[&table].exact.is_empty());
}

#[test]
fn deleting_a_completed_marker_clears_it() {
    let mut state = sample_state();
    let key = StateKey::Completed(stream("orders"));
    state.apply(&StateChange::Delete(key.encode())).unwrap();
    assert!(state.streams[&stream("orders")].completed.is_empty());
    assert_eq!(
        state.streams[&stream("orders")].generation,
        Some(GenerationId(2))
    );
}

#[test]
fn a_stream_s_reset_marker_is_kept_apart_from_its_position_and_deleting_it_forgets_it() {
    let mut state = sample_state();
    let key = StateKey::Reset(stream("orders"));
    assert_eq!(state.resets[&stream("orders")], Epoch(3));
    state.apply(&StateChange::Delete(key.encode())).unwrap();
    assert!(state.resets.is_empty());
    assert_eq!(state.streams[&stream("orders")].phase, 1);
    let marker = StateEntry::Reset {
        stream: stream("events"),
        epoch: Epoch(9),
    };
    state.apply(&StateChange::Put(marker.to_record())).unwrap();
    assert_eq!(state.resets[&stream("events")], Epoch(9));
    assert!(!state.streams.contains_key(&stream("events")));
}

#[test]
fn deleting_a_table_s_sequences_forgets_whose_they_are() {
    let mut state = sample_state();
    let table = TablePath::new(["orders"]).unwrap();
    let key = StateKey::Sequences(table.clone());
    state.apply(&StateChange::Delete(key.encode())).unwrap();
    assert_eq!(state.tables[&table].sequences, None);
    assert!(!state.tables[&table].history);
    assert!(state.tables[&table].key.is_empty());
    assert_eq!(state.tables[&table].change_time, None);
    assert_eq!(state.tables[&table].identity, Identity::Key);
    assert!(state.tables[&table].schema.is_some());
    let engine = StateEntry::Sequences {
        table: table.clone(),
        sequences: Sequences::Engine,
        history: false,
        key: Vec::new(),
        change_time: None,
        identity: Identity::Position,
    };
    state.apply(&StateChange::Put(engine.to_record())).unwrap();
    assert_eq!(state.tables[&table].sequences, Some(Sequences::Engine));
    assert_eq!(state.tables[&table].identity, Identity::Position);
}

#[test]
fn a_table_s_sequences_say_whether_it_keeps_history_and_a_record_without_it_is_refused() {
    let table = TablePath::new(["orders"]).unwrap();
    for history in [true, false] {
        let entry = StateEntry::Sequences {
            table: table.clone(),
            sequences: Sequences::Source,
            history,
            key: vec![ColumnPath::from("id")],
            change_time: None,
            identity: Identity::Key,
        };
        let record = entry.to_record();
        assert_eq!(StateEntry::from_record(&record).unwrap(), entry);
        let text = String::from_utf8(record.value.to_vec()).unwrap();
        let text = text.replace(&format!(r#","history":{history}"#), "");
        assert!(!text.contains("history"), "{text}");
        let lacking = StateRecord {
            key: record.key.clone(),
            value: text.into_bytes().into(),
        };
        assert!(matches!(
            StateEntry::from_record(&lacking),
            Err(StateError::MalformedValue { .. })
        ));
    }
}

#[test]
fn record_keys_are_stable_text() {
    assert_eq!(StateKey::Epoch.encode(), "\"epoch\"");
    let key = StateKey::Partition(stream("orders"), partition("p0"));
    assert_eq!(
        key.encode(),
        r#"{"partition":[{"namespace":null,"name":"orders"},"p0"]}"#
    );
    assert_eq!(StateKey::parse(&key.encode()).unwrap(), key);
}

#[test]
fn unreadable_records_are_typed_errors() {
    let epoch = StateEntry::Epoch(Epoch(1)).to_record();
    let cases = [
        (
            StateRecord {
                key: "nonsense".to_owned(),
                value: epoch.value.clone(),
            },
            StateError::MalformedKey {
                key: "nonsense".to_owned(),
            },
        ),
        (
            StateRecord {
                key: epoch.key.clone(),
                value: Bytes::from_static(b"{\"v\":3,\"entry\":{\"epoch\":1}}"),
            },
            StateError::UnsupportedVersion {
                key: epoch.key.clone(),
                version: 3,
            },
        ),
        (
            StateRecord {
                key: epoch.key.clone(),
                value: Bytes::from_static(b"{\"v\":5,\"entry\":{\"no\":[\"such\"]}}"),
            },
            StateError::UnsupportedVersion {
                key: epoch.key.clone(),
                version: 5,
            },
        ),
        (
            StateRecord {
                key: StateKey::Receipt.encode(),
                value: epoch.value.clone(),
            },
            StateError::KeyMismatch {
                key: StateKey::Receipt.encode(),
            },
        ),
    ];
    for (record, expected) in cases {
        assert_eq!(StateEntry::from_record(&record).unwrap_err(), expected);
    }
    let garbage = StateRecord {
        key: epoch.key.clone(),
        value: Bytes::from_static(b"\xff"),
    };
    assert!(matches!(
        StateEntry::from_record(&garbage),
        Err(StateError::MalformedValue { .. })
    ));
}

#[test]
fn state_holding_a_key_twice_is_refused() {
    let epoch = StateEntry::Epoch(Epoch(1)).to_record();
    let again = StateEntry::Epoch(Epoch(2)).to_record();
    let refused = PipelineState::from_records(&[epoch.clone(), again]).unwrap_err();
    assert_eq!(refused, StateError::Repeated { key: epoch.key });
}

#[test]
fn keys_in_a_non_canonical_form_are_malformed() {
    let entry = StateEntry::Partition {
        stream: stream("s"),
        partition: partition("p"),
        state: cursor(5),
        load: load(1),
    };
    let canonical = entry.to_record();
    // The stream's members in another order.
    let rewritten = r#"{"partition":[{"name":"s","namespace":null},"p"]}"#;
    assert_eq!(
        serde_json::from_str::<StateKey>(rewritten).unwrap(),
        entry.key()
    );
    let record = StateRecord {
        key: rewritten.to_owned(),
        value: canonical.value,
    };
    assert_eq!(
        StateEntry::from_record(&record).unwrap_err(),
        StateError::MalformedKey {
            key: rewritten.to_owned()
        }
    );
}

#[test]
fn a_key_a_state_error_names_is_shown_bounded() {
    let hostile = "\u{1b}[2J\u{202e}".repeat(100_000);
    let record = StateRecord {
        key: hostile,
        value: Bytes::from_static(b"{}"),
    };
    let refused = StateEntry::from_record(&record).unwrap_err().to_string();
    assert!(refused.len() <= 1024, "{} bytes", refused.len());
    assert!(!refused.chars().any(char::is_control), "{refused:?}");
    assert!(refused.contains(r"\u{1b}[2J\u{202e}"), "{refused:?}");
}

#[test]
fn applying_changes_puts_and_deletes_entries() {
    let mut state = PipelineState::default();
    let put = StateEntry::Partition {
        stream: stream("orders"),
        partition: partition("p0"),
        state: cursor(5),
        load: load(2),
    };
    state.apply(&StateChange::Put(put.to_record())).unwrap();
    assert_eq!(
        state.streams[&stream("orders")].partitions[&partition("p0")],
        cursor(5)
    );
    state
        .apply(&StateChange::Delete(put.key().encode()))
        .unwrap();
    assert_eq!(state.streams[&stream("orders")], StreamState::default());
    let mut full = sample_state();
    for record in records_of(&full) {
        full.apply(&StateChange::Delete(record.key)).unwrap();
    }
    assert_eq!(full.epoch, Epoch::default());
    assert!(full.last_receipt.is_none());
    assert!(
        full.tables
            .values()
            .all(|table| *table == TableState::default())
    );
}

#[test]
fn name_maps_are_append_only() {
    let mut names = NameMap::default();
    assert!(names.is_empty());
    names.insert(ColumnPath::from("a-b"), "a_b").unwrap();
    names.insert(ColumnPath::from("a-b"), "a_b").unwrap();
    names.insert(ColumnPath::from("c"), "c").unwrap();
    assert_eq!(
        names.insert(ColumnPath::from("a-b"), "a_b_2"),
        Err(NameConflict::Remapped {
            key: ColumnKey::Source(ColumnPath::from("a-b")),
            existing: "a_b".to_owned()
        })
    );
    assert_eq!(names.get(&ColumnPath::from("a-b").into()), Some("a_b"));
    assert_eq!(names.len(), 2);
    assert!(!names.is_empty());
    let pairs: Vec<_> = names.iter().collect();
    assert_eq!(
        pairs,
        [
            (&ColumnKey::Source(ColumnPath::from("a-b")), "a_b"),
            (&ColumnKey::Source(ColumnPath::from("c")), "c")
        ]
    );
}

#[test]
fn name_maps_never_give_two_columns_one_identifier() {
    let mut names = NameMap::default();
    names.insert(ColumnPath::from("a"), "x").unwrap();
    let variant = ColumnKey::Variant {
        column: ColumnPath::from("b"),
        kind: TypeKind::Json,
    };
    assert_eq!(
        names.insert(variant.clone(), "x"),
        Err(NameConflict::Taken {
            name: "x".to_owned(),
            owner: ColumnKey::Source(ColumnPath::from("a"))
        })
    );
    assert_eq!(
        names.owner("x"),
        Some(&ColumnKey::Source(ColumnPath::from("a")))
    );
    assert_eq!(names.owner("y"), None);
    assert!(names.get(&variant).is_none());
}

#[test]
fn column_keys_name_their_source_column() {
    let variant = ColumnKey::Variant {
        column: ColumnPath::new(["a", "b"]).unwrap(),
        kind: TypeKind::Json,
    };
    assert_eq!(variant.column(), &ColumnPath::new(["a", "b"]).unwrap());
    assert_eq!(variant.to_string(), "a.b (Json variant)");
    assert_eq!(ColumnKey::from(ColumnPath::from("a")).to_string(), "a");
}

fn partition_states() -> impl Strategy<Value = PartitionState> {
    prop_oneof![Just(PartitionState::Done), any::<u64>().prop_map(cursor)]
}

/// Records each of `state`'s positions as recorded by a load of its own, from load `first` on.
fn recorded(state: &mut PipelineState, first: u64) {
    let positions: Vec<(StreamName, PartitionId)> = state
        .streams
        .iter()
        .flat_map(|(name, stream)| {
            stream
                .partitions
                .keys()
                .map(|id| (name.clone(), id.clone()))
        })
        .collect();
    for (offset, position) in (0_u64..).zip(positions) {
        let number = (first % (1 << 40)).wrapping_add(offset);
        state.recorded_by.insert(position, load(number));
    }
}

fn states() -> impl Strategy<Value = PipelineState> {
    let streams = proptest::collection::btree_map(
        prop_oneof![Just("a"), Just("b"), Just("c.d")],
        (
            any::<u16>(),
            proptest::collection::btree_map("[a-z0-9]{1,4}", partition_states(), 0..3),
            any::<Option<u64>>(),
            proptest::collection::vec(any::<u64>(), 0..3),
        ),
        0..3,
    );
    let resets = proptest::collection::btree_map(
        prop_oneof![Just("a"), Just("b"), Just("e")],
        any::<u64>(),
        0..3,
    );
    (any::<u64>(), streams, any::<bool>(), resets)
        .prop_map(|(epoch, streams, with_receipt, resets)| PipelineState {
            epoch: Epoch(epoch),
            streams: streams
                .into_iter()
                .map(|(name, (phase, partitions, generation, completed))| {
                    let partitions = partitions
                        .into_iter()
                        .map(|(id, state)| (partition(&id), state))
                        .collect();
                    (
                        stream(name),
                        StreamState {
                            phase,
                            partitions,
                            generation: generation.map(GenerationId),
                            completed: completed.into_iter().map(GenerationId).collect(),
                        },
                    )
                })
                .collect(),
            tables: std::collections::BTreeMap::default(),
            resets: resets
                .into_iter()
                .map(|(name, epoch)| (stream(name), Epoch(epoch)))
                .collect(),
            last_receipt: with_receipt.then(receipt),
            recorded_by: std::collections::BTreeMap::new(),
        })
        .prop_flat_map(|state| (Just(state), tables(), any::<u64>()))
        .prop_map(|(mut state, tables, first)| {
            state.tables = tables;
            recorded(&mut state, first);
            state
        })
}

fn tables() -> impl Strategy<Value = std::collections::BTreeMap<TablePath, TableState>> {
    let columns = proptest::collection::btree_set("[a-z]{1,3}", 1..4);
    proptest::collection::btree_map(
        prop_oneof![Just("a"), Just("b.c")],
        (1..9_u32, columns, any::<bool>(), 0..5_u8),
        0..3,
    )
    .prop_map(|tables| {
        tables
            .into_iter()
            .map(|(path, (version, columns, variant, sequences))| {
                let mut names = NameMap::default();
                let mut fields = Vec::new();
                for column in &columns {
                    names
                        .insert(ColumnPath::from(column.as_str()), column.as_str())
                        .unwrap();
                    fields.push(Field::new(column.as_str(), LogicalType::Int64, true));
                }
                if variant {
                    let key = ColumnKey::Variant {
                        column: ColumnPath::from(columns.first().unwrap().as_str()),
                        kind: TypeKind::Json,
                    };
                    names.insert(key, "v__json").unwrap();
                    fields.push(Field::new("v__json", LogicalType::Json, true));
                }
                // Every other column is exact.
                let exact = columns
                    .iter()
                    .step_by(2)
                    .map(|column| Arc::from(column.as_str()))
                    .collect();
                let state = TableState {
                    schema: Some((SchemaVersion(version), TableSchema::new(fields).unwrap())),
                    physical: Some(path.replace('.', "_").into()),
                    names,
                    sequences: [
                        None,
                        Some(Sequences::Engine),
                        Some(Sequences::Source),
                        Some(Sequences::Engine),
                        Some(Sequences::Source),
                    ][usize::from(sequences)],
                    // Only a table whose sequences are recorded records its history, key and
                    // change time.
                    history: sequences >= 3,
                    key: match sequences {
                        0 => Vec::new(),
                        _ => columns
                            .iter()
                            .take(usize::from(sequences % 2) + 1)
                            .map(|column| ColumnPath::from(column.as_str()))
                            .collect(),
                    },
                    change_time: (sequences >= 3)
                        .then(|| ColumnPath::from(columns.first().unwrap().as_str())),
                    identity: [Identity::Key, Identity::Position][usize::from(sequences % 2)],
                    exact,
                };
                (TablePath::new([path]).unwrap(), state)
            })
            .collect()
    })
}

proptest! {
    #[test]
    fn generated_states_round_trip_through_records(state in states()) {
        prop_assert_eq!(PipelineState::from_records(&records_of(&state)).unwrap(), state);
    }

    #[test]
    fn decoding_arbitrary_records_never_panics(key in ".{0,40}", value in proptest::collection::vec(any::<u8>(), 0..64)) {
        let record = StateRecord { key, value: Bytes::from(value) };
        drop(StateEntry::from_record(&record));
    }
}

#[test]
fn deleting_entries_that_do_not_exist_changes_nothing() {
    let mut state = PipelineState::default();
    let keys = [
        StateKey::Phase(stream("gone")),
        StateKey::Partition(stream("gone"), partition("p0")),
        StateKey::Generation(stream("gone")),
        StateKey::Schema(TablePath::new(["gone"]).unwrap()),
        StateKey::Names(TablePath::new(["gone"]).unwrap()),
    ];
    for key in keys {
        state.apply(&StateChange::Delete(key.encode())).unwrap();
    }
    assert_eq!(state, PipelineState::default());
    assert!(
        state
            .apply(&StateChange::Delete("not a key".to_owned()))
            .is_err()
    );
}

#[test]
fn a_recorded_name_map_naming_two_columns_alike_is_refused() {
    let two_on_one = serde_json::json!([[{"source": ["a"]}, "c"], [{"source": ["b"]}, "c"]]);
    assert!(serde_json::from_value::<NameMap>(two_on_one).is_err());
    let one_twice = serde_json::json!([[{"source": ["a"]}, "c"], [{"source": ["a"]}, "d"]]);
    assert!(serde_json::from_value::<NameMap>(one_twice).is_err());
    let distinct = serde_json::json!([[{"source": ["a"]}, "c"], [{"source": ["b"]}, "d"]]);
    let names = serde_json::from_value::<NameMap>(distinct).unwrap();
    assert_eq!(
        names.owner("d"),
        Some(&ColumnKey::Source(ColumnPath::from("b")))
    );
    assert_eq!(
        names.get(&ColumnKey::Source(ColumnPath::from("a"))),
        Some("c")
    );
}

/// How each level below a nested column is made.
#[derive(Clone, Copy, Debug)]
enum Nesting {
    Structs,
    Lists,
    StructsThenLists,
    ListsThenStructs,
}

/// A schema of one column nested `depth` levels deep, counting the column as the first: each
/// level below it a struct of one field or a list, as `nesting` says.
fn nested(depth: usize, nesting: Nesting) -> Vec<Field> {
    let mut logical = LogicalType::Int64;
    for level in (1..depth).rev() {
        let list = match nesting {
            Nesting::Structs => false,
            Nesting::Lists => true,
            Nesting::StructsThenLists => level.is_multiple_of(2),
            Nesting::ListsThenStructs => !level.is_multiple_of(2),
        };
        logical = if list {
            LogicalType::List(Box::new(Field::new("item", logical, true)))
        } else {
            let fields = Fields::new(vec![Field::new("a", logical, true)]).unwrap();
            LogicalType::Struct(fields)
        };
    }
    vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("c", logical, true),
    ]
}

const NESTINGS: [Nesting; 4] = [
    Nesting::Structs,
    Nesting::Lists,
    Nesting::StructsThenLists,
    Nesting::ListsThenStructs,
];

#[test]
fn a_schema_nested_to_the_limit_is_stored_and_read_back() {
    let limit = usize::try_from(rdlt_wire::limits::NESTING_DEPTH).unwrap();
    for nesting in NESTINGS {
        let schema = TableSchema::new(nested(limit, nesting)).unwrap();
        let entry = StateEntry::Schema {
            table: TablePath::new(["deep"]).unwrap(),
            version: SchemaVersion(1),
            schema,
            exact: ["id".into()].into(),
        };
        let record = entry.to_record();
        assert_eq!(
            StateEntry::from_record(&record),
            Ok(entry),
            "{nesting:?} nested to the limit"
        );
    }
}

#[test]
fn a_schema_nested_past_the_limit_is_refused() {
    let limit = usize::try_from(rdlt_wire::limits::NESTING_DEPTH).unwrap();
    for nesting in NESTINGS {
        assert!(
            matches!(
                TableSchema::new(nested(limit + 1, nesting)),
                Err(crate::types::TypeError::TooDeep { depth, limit: 64 }) if depth == 65
            ),
            "{nesting:?} nested past the limit"
        );
    }
}

/// The JSON pointers of every object in `value`, `at` and below.
fn objects(value: &serde_json::Value, at: String, found: &mut Vec<String>) {
    match value {
        serde_json::Value::Object(fields) => {
            for (name, field) in fields {
                objects(field, format!("{at}/{name}"), found);
            }
            found.push(at);
        }
        serde_json::Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                objects(item, format!("{at}/{index}"), found);
            }
        }
        _ => {}
    }
}

#[test]
fn a_state_record_with_a_field_this_build_does_not_know_is_refused() {
    for record in records_of(&sample_state()) {
        let value: serde_json::Value = serde_json::from_slice(&record.value).unwrap();
        let entry = &value["entry"];
        let mut found = Vec::new();
        objects(entry, String::new(), &mut found);
        let mut cases: Vec<(String, Bytes)> = found
            .into_iter()
            .map(|pointer| {
                let mut grown = entry.clone();
                grown
                    .pointer_mut(&pointer)
                    .and_then(serde_json::Value::as_object_mut)
                    .unwrap()
                    .insert("unknown".to_owned(), serde_json::json!(1));
                (pointer, versioned(super::STATE_VERSION, &grown))
            })
            .collect();
        cases.push((
            "the value".to_owned(),
            format!(
                r#"{{"v":{},"entry":{},"unknown":1}}"#,
                super::STATE_VERSION,
                entry
            )
            .into_bytes()
            .into(),
        ));
        for (pointer, value) in cases {
            let grown = StateRecord {
                key: record.key.clone(),
                value,
            };
            assert!(
                matches!(
                    StateEntry::from_record(&grown),
                    Err(StateError::MalformedValue { .. })
                ),
                "{} with a field at {pointer:?}",
                record.key
            );
        }
    }
}

#[test]
fn a_state_record_of_the_previous_format_is_refused() {
    for record in records_of(&sample_state()) {
        let value: serde_json::Value = serde_json::from_slice(&record.value).unwrap();
        let previous = StateRecord {
            key: record.key.clone(),
            value: versioned(super::STATE_VERSION - 1, &value["entry"]),
        };
        assert_eq!(
            StateEntry::from_record(&previous),
            Err(StateError::UnsupportedVersion {
                key: record.key.clone(),
                version: super::STATE_VERSION - 1
            })
        );
    }
}

#[test]
fn a_state_record_lacking_any_member_it_writes_is_refused() {
    let mut state = sample_state();
    if let Some(table) = state.tables.values_mut().next() {
        table.sequences = Some(Sequences::Engine);
    }
    for record in records_of(&state) {
        let value: serde_json::Value = serde_json::from_slice(&record.value).unwrap();
        rdlt_testkit::required::every_member_required::<StateEntry>(&value["entry"]);
    }
}

/// A state value of format `v` holding `entry`, its format first, as this crate writes them.
fn versioned(v: u16, entry: &serde_json::Value) -> Bytes {
    format!(r#"{{"v":{v},"entry":{entry}}}"#)
        .into_bytes()
        .into()
}

#[test]
fn a_state_value_naming_its_entry_before_its_format_is_refused() {
    let epoch = StateEntry::Epoch(Epoch(1)).to_record();
    let value: serde_json::Value = serde_json::from_slice(&epoch.value).unwrap();
    let entry = &value["entry"];
    let cases = [
        format!(r#"{{"entry":{entry},"v":{}}}"#, super::STATE_VERSION),
        format!(r#"{{"entry":{entry}}}"#),
        format!(r#"{{"v":{}}}"#, super::STATE_VERSION),
        format!(r#"{{"v":{},"entry":{entry},"v":2}}"#, super::STATE_VERSION),
    ];
    for case in cases {
        let record = StateRecord {
            key: epoch.key.clone(),
            value: case.clone().into_bytes().into(),
        };
        assert!(
            matches!(
                StateEntry::from_record(&record),
                Err(StateError::MalformedValue { .. })
            ),
            "{case}"
        );
    }
    // Written by this crate, its format comes first and it reads back.
    assert_eq!(epoch.value, versioned(super::STATE_VERSION, entry));
}

#[test]
fn a_state_value_that_is_not_an_object_is_refused_saying_what_one_is() {
    let epoch = StateEntry::Epoch(Epoch(1)).to_record();
    for case in ["[1]", "\"x\"", "7", "null"] {
        let record = StateRecord {
            key: epoch.key.clone(),
            value: Bytes::from_static(case.as_bytes()),
        };
        let Err(StateError::MalformedValue { reason, .. }) = StateEntry::from_record(&record)
        else {
            panic!("{case} was not refused as a malformed value");
        };
        assert!(
            reason.contains("expected a state value: its format, then its entry"),
            "{case}: {reason}"
        );
    }
}

#[test]
fn a_record_s_value_is_written_as_the_json_it_holds() {
    let entry = StateEntry::Partition {
        stream: stream("orders"),
        partition: partition("p0"),
        state: PartitionState::Cursor(Cursor::new(1, &vec![b'c'; 30_000]).unwrap()),
        load: load(1),
    };
    let record = entry.to_record();
    let json = serde_json::to_vec(&record).unwrap();
    let key = serde_json::to_string(&record.key).unwrap();
    let value = std::str::from_utf8(&record.value).unwrap();
    assert_eq!(
        String::from_utf8(json.clone()).unwrap(),
        format!(r#"{{"key":{key},"value":{value}}}"#)
    );
    // The cursor is base64 once: a third more than its bytes, and the record's text beside.
    assert!(json.len() <= 30_000 / 3 * 4 + 512, "{} bytes", json.len());
    assert_eq!(
        serde_json::from_slice::<StateRecord>(&json).unwrap(),
        record
    );
    // A value that is not JSON text alone is no record's.
    for value in [&b"\xff"[..], b"not json", b" {} ", b"", b"{}{}"] {
        let record = StateRecord {
            key: "k".to_owned(),
            value: Bytes::copy_from_slice(value),
        };
        assert!(serde_json::to_vec(&record).is_err(), "{value:?}");
    }
    assert!(serde_json::from_str::<StateRecord>(r#"{"key":"k","value":*}"#).is_err());
    assert!(serde_json::from_str::<StateRecord>(r#"{"key":"k"}"#).is_err());
}

#[test]
fn a_destination_s_state_value_with_whitespace_around_it_reads_as_its_entry() {
    let epoch = StateEntry::Epoch(Epoch(1)).to_record();
    let mut spaced = b" \n".to_vec();
    spaced.extend_from_slice(&epoch.value);
    spaced.extend_from_slice(b"\t ");
    let record = StateRecord {
        key: epoch.key.clone(),
        value: spaced.into(),
    };
    let entry = StateEntry::from_record(&record).expect("the value is its entry's JSON");
    assert_eq!(entry, StateEntry::Epoch(Epoch(1)));
    // What the engine records of it again is the entry's own JSON, with nothing around it.
    assert_eq!(entry.to_record(), epoch);
}

/// The identifiers a name map property assigns from: two folding alike, so a destination that
/// keeps case tells them apart.
const IDENTIFIERS: [&str; 4] = ["x", "y", "X", "z"];

/// The columns a name map property maps: two sources, one nested, and a variant.
fn columns() -> [ColumnKey; 4] {
    [
        ColumnKey::Source(ColumnPath::from("a")),
        ColumnKey::Source(ColumnPath::from("b")),
        ColumnKey::Source(ColumnPath::new(["a", "b"]).expect("a nested path")),
        ColumnKey::Variant {
            column: ColumnPath::from("a"),
            kind: TypeKind::Json,
        },
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(256)))]

    /// Through any inserts a name map keeps one identifier for each column and one column for
    /// each identifier, keeps a column's first identifier, changes nothing on a refusal, and
    /// reads back from its record unchanged.
    #[test]
    fn a_name_map_stays_injective_and_append_only_through_any_inserts(
        inserts in proptest::collection::vec((0_usize..4, 0_usize..4), 0..16),
    ) {
        let columns = columns();
        let mut map = NameMap::default();
        let mut model: Vec<(usize, usize)> = Vec::new();
        for (column, identifier) in inserts {
            let before = map.clone();
            let inserted = map.insert(columns[column].clone(), IDENTIFIERS[identifier]);
            let mapped = model.iter().find(|(known, _)| *known == column).map(|(_, named)| *named);
            let owner = model.iter().find(|(_, named)| *named == identifier).map(|(known, _)| *known);
            match (mapped, owner) {
                (Some(named), _) if named == identifier => {
                    prop_assert_eq!(inserted, Ok(()));
                    prop_assert_eq!(&map, &before);
                }
                (Some(named), _) => {
                    prop_assert_eq!(inserted, Err(NameConflict::Remapped {
                        key: columns[column].clone(),
                        existing: IDENTIFIERS[named].to_owned(),
                    }));
                    prop_assert_eq!(&map, &before);
                }
                (None, Some(known)) => {
                    prop_assert_eq!(inserted, Err(NameConflict::Taken {
                        name: IDENTIFIERS[identifier].to_owned(),
                        owner: columns[known].clone(),
                    }));
                    prop_assert_eq!(&map, &before);
                }
                (None, None) => {
                    prop_assert_eq!(inserted, Ok(()));
                    model.push((column, identifier));
                }
            }
        }
        prop_assert_eq!(map.len(), model.len());
        for (column, identifier) in &model {
            prop_assert_eq!(map.get(&columns[*column]), Some(IDENTIFIERS[*identifier]));
            prop_assert_eq!(map.owner(IDENTIFIERS[*identifier]), Some(&columns[*column]));
        }
        let record: Vec<(ColumnKey, String)> =
            serde_json::from_value(serde_json::to_value(&map).unwrap()).unwrap();
        prop_assert_eq!(NameMap::try_from(record).ok(), Some(map));
    }

    /// A recorded name map is read back as exactly its pairs where no column and no identifier
    /// appears in it twice, and is refused where one does.
    #[test]
    fn a_recorded_name_map_is_read_only_where_it_is_injective(
        pairs in proptest::collection::vec((0_usize..4, 0_usize..4), 0..8),
    ) {
        let columns = columns();
        let record: Vec<(ColumnKey, String)> = pairs
            .iter()
            .map(|(column, identifier)| (columns[*column].clone(), IDENTIFIERS[*identifier].to_owned()))
            .collect();
        let distinct = |at: fn(&(usize, usize)) -> usize| {
            let mut seen = std::collections::BTreeSet::new();
            pairs.iter().all(|pair| seen.insert(at(pair)))
        };
        let injective = distinct(|pair| pair.0) && distinct(|pair| pair.1);
        let read = NameMap::try_from(record.clone());
        if injective {
            let read = read.expect("an injective record reads");
            let mut back: Vec<(ColumnKey, String)> =
                serde_json::from_value(serde_json::to_value(&read).unwrap()).unwrap();
            back.sort();
            let mut record = record;
            record.sort();
            prop_assert_eq!(back, record);
        } else {
            prop_assert!(read.is_err(), "{:?}", pairs);
        }
    }
}

#[test]
fn a_record_holds_the_json_its_entry_writes() {
    let cursor = Cursor::new(1, &[7; 300]).unwrap();
    let entries = [
        StateEntry::Partition {
            stream: stream("orders"),
            partition: partition("p0"),
            state: PartitionState::Cursor(cursor),
            load: LoadId::from_parts(UNIX_EPOCH, 1),
        },
        StateEntry::Phase {
            stream: stream("ü \"quoted\""),
            phase: 2,
        },
    ];
    for entry in entries {
        let written = serde_json::to_vec(&Writing {
            v: super::STATE_VERSION,
            entry: &entry,
        })
        .unwrap();
        assert_eq!(
            entry.to_record().value.as_ref(),
            written.as_slice(),
            "{entry:?}"
        );
    }
}

/// A table's schema, names and sequences, of a nested table with a struct, a list and a variant.
fn a_table_s_entries() -> [StateEntry; 3] {
    let table = TablePath::new(["orders", "lines"]).unwrap();
    let city = ColumnPath::new(["address", "city"]).unwrap();
    let address = Fields::new(vec![Field::new("city", LogicalType::Utf8, true)]).unwrap();
    let tags = Field::new("item", LogicalType::Utf8, true);
    let schema = TableSchema::new(vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("address", LogicalType::Struct(address), true),
        Field::new("tags", LogicalType::List(Box::new(tags)), true),
    ])
    .unwrap();
    let mut names = NameMap::default();
    names.insert(ColumnPath::from("id"), "id").unwrap();
    names
        .insert(
            ColumnKey::Variant {
                column: city.clone(),
                kind: TypeKind::Int64,
            },
            "address__city__int",
        )
        .unwrap();
    [
        StateEntry::Schema {
            table: table.clone(),
            version: SchemaVersion(3),
            schema,
            exact: ["id".into()].into(),
        },
        StateEntry::Names {
            table: table.clone(),
            physical: "orders__lines".into(),
            names,
        },
        StateEntry::Sequences {
            table,
            sequences: Sequences::Source,
            history: true,
            key: vec![ColumnPath::from("id"), city.clone()],
            change_time: Some(city),
            identity: Identity::Position,
        },
    ]
}

#[test]
fn a_table_s_records_are_this_json() {
    let expected = [
        (
            r#"{"schema":["orders","lines"]}"#,
            concat!(
                r#"{"v":4,"entry":{"schema":{"table":["orders","lines"],"version":3,"schema":"#,
                r#"{"fields":[[{"name":"id","nullable":false,"type":"int64"}],"#,
                r#"[{"name":"address","nullable":true,"type":{"struct":1}},"#,
                r#"{"name":"city","nullable":true,"type":"utf8"}],"#,
                r#"[{"name":"tags","nullable":true,"type":"list"},"#,
                r#"{"name":"item","nullable":true,"type":"utf8"}]]},"exact":["id"]}}}"#,
            ),
        ),
        (
            r#"{"names":["orders","lines"]}"#,
            concat!(
                r#"{"v":4,"entry":{"names":{"table":["orders","lines"],"#,
                r#""physical":"orders__lines","names":[[{"source":["id"]},"id"],"#,
                r#"[{"variant":{"column":["address","city"],"kind":"int64"}},"#,
                r#""address__city__int"]]}}}"#,
            ),
        ),
        (
            r#"{"sequences":["orders","lines"]}"#,
            concat!(
                r#"{"v":4,"entry":{"sequences":{"table":["orders","lines"],"#,
                r#""sequences":"source","history":true,"key":[["id"],["address","city"]],"#,
                r#""change_time":["address","city"],"identity":"position"}}}"#,
            ),
        ),
    ];
    for (entry, (key, value)) in a_table_s_entries().iter().zip(expected) {
        let record = entry.to_record();
        assert_eq!(record.key, key);
        assert_eq!(std::str::from_utf8(&record.value).unwrap(), value);
    }
}
