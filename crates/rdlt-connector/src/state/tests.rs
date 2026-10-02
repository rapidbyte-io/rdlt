use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use bytes::Bytes;
use proptest::prelude::*;

use super::{
    NameConflict, NameMap, PartitionState, PipelineState, Sequences, StateChange, StateEntry,
    StateError, StateKey, StateRecord, StreamState, TableState,
};
use crate::commit::Receipt;
use crate::cursor::Cursor;
use crate::id::{
    CommitSeq, Epoch, GenerationId, LoadId, PartitionId, SchemaVersion, StreamName, TablePath,
};
use crate::schema::{ColumnKey, ColumnPath, TableSchema};
use crate::types::{Field, LogicalType, TypeKind};

fn stream(name: &str) -> StreamName {
    StreamName::new(name).unwrap()
}

fn partition(id: &str) -> PartitionId {
    PartitionId::parse(id).unwrap()
}

fn cursor(offset: u64) -> PartitionState {
    PartitionState::Cursor(Cursor::encode(1, &offset).unwrap())
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
            exact: ["id".into()].into(),
        },
    );
    state
}

#[test]
fn state_round_trips_through_records() {
    let state = sample_state();
    assert_eq!(
        PipelineState::from_records(&state.to_records()).unwrap(),
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
    let record = state
        .to_records()
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
        value: serde_json::to_vec(&json).unwrap().into(),
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
    assert!(state.tables[&table].schema.is_some());
    let engine = StateEntry::Sequences {
        table: table.clone(),
        sequences: Sequences::Engine,
        history: false,
        key: Vec::new(),
        change_time: None,
    };
    state.apply(&StateChange::Put(engine.to_record())).unwrap();
    assert_eq!(state.tables[&table].sequences, Some(Sequences::Engine));
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
                value: Bytes::from_static(b"{\"v\":3,\"entry\":{}}"),
            },
            StateError::UnsupportedVersion {
                key: epoch.key.clone(),
                version: 3,
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
    };
    let canonical = entry.to_record();
    let rewritten = r#"{"partition":[{"name":"s"},"p"]}"#;
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
fn a_sequences_record_without_its_key_is_refused_naming_the_field() {
    // As written before a table recorded the key it is merged by: there is no reading it
    // otherwise, as no state written so is kept.
    let earlier = StateRecord {
        key: StateKey::Sequences(TablePath::new(["orders"]).unwrap()).encode(),
        value: Bytes::from_static(
            br#"{"v":1,"entry":{"sequences":{"table":["orders"],"sequences":"engine"}}}"#,
        ),
    };
    let refused = StateEntry::from_record(&earlier).unwrap_err();
    let StateError::MalformedValue { reason, .. } = &refused else {
        panic!("{refused}");
    };
    assert!(reason.contains("missing field `key`"), "{reason}");
}

#[test]
fn applying_changes_puts_and_deletes_entries() {
    let mut state = PipelineState::default();
    let put = StateEntry::Partition {
        stream: stream("orders"),
        partition: partition("p0"),
        state: cursor(5),
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
    for record in full.clone().to_records() {
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
        })
        .prop_flat_map(|state| (Just(state), tables()))
        .prop_map(|(mut state, tables)| {
            state.tables = tables;
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
        prop_assert_eq!(PipelineState::from_records(&state.to_records()).unwrap(), state);
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
fn a_records_value_is_json_text_a_third_longer_than_its_bytes() {
    let value: Vec<u8> = (0..=255).cycle().take(30_000).collect();
    let record = StateRecord {
        key: "k".to_owned(),
        value: Bytes::from(value),
    };
    let json = serde_json::to_vec(&record).unwrap();
    // Base64 text, not a number a byte.
    assert!(json.len() <= 30_000 * 4 / 3 + 64, "{} bytes", json.len());
    assert_eq!(
        serde_json::from_slice::<StateRecord>(&json).unwrap(),
        record
    );
    // Text that is not base64 is no record.
    assert!(serde_json::from_str::<StateRecord>(r#"{"key":"k","value":"*"}"#).is_err());
    let empty = StateRecord {
        key: "k".to_owned(),
        value: Bytes::new(),
    };
    let json = serde_json::to_string(&empty).unwrap();
    assert_eq!(json, r#"{"key":"k","value":""}"#);
    assert_eq!(serde_json::from_str::<StateRecord>(&json).unwrap(), empty);
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
            let fields = crate::types::Fields::new(vec![Field::new("a", logical, true)]).unwrap();
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
    let limit = usize::try_from(crate::limits::MAX_NESTING_DEPTH).unwrap();
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
    let limit = usize::try_from(crate::limits::MAX_NESTING_DEPTH).unwrap();
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
    for record in sample_state().to_records() {
        let value: serde_json::Value = serde_json::from_slice(&record.value).unwrap();
        let mut found = Vec::new();
        objects(&value, String::new(), &mut found);
        for pointer in found {
            let mut grown = value.clone();
            grown
                .pointer_mut(&pointer)
                .and_then(serde_json::Value::as_object_mut)
                .unwrap()
                .insert("unknown".to_owned(), serde_json::json!(1));
            let grown = StateRecord {
                key: record.key.clone(),
                value: serde_json::to_vec(&grown).unwrap().into(),
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
    for record in sample_state().to_records() {
        let mut value: serde_json::Value = serde_json::from_slice(&record.value).unwrap();
        value["v"] = serde_json::json!(1);
        let previous = StateRecord {
            key: record.key.clone(),
            value: serde_json::to_vec(&value).unwrap().into(),
        };
        assert_eq!(
            StateEntry::from_record(&previous),
            Err(StateError::UnsupportedVersion {
                key: record.key.clone(),
                version: 1
            })
        );
    }
}
