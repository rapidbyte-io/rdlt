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
fn a_schema_recorded_without_exact_columns_has_none_and_deleting_it_forgets_them() {
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
    let older = StateRecord {
        key: record.key.clone(),
        value: serde_json::to_vec(&json).unwrap().into(),
    };
    let mut read = sample_state();
    read.apply(&StateChange::Put(older)).unwrap();
    assert!(read.tables[&table].exact.is_empty());
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
    assert!(state.tables[&table].schema.is_some());
    let engine = StateEntry::Sequences {
        table: table.clone(),
        sequences: Sequences::Engine,
        history: false,
    };
    state.apply(&StateChange::Put(engine.to_record())).unwrap();
    assert_eq!(state.tables[&table].sequences, Some(Sequences::Engine));
}

#[test]
fn a_table_s_sequences_say_whether_it_keeps_history_and_older_records_say_it_does_not() {
    let table = TablePath::new(["orders"]).unwrap();
    let history = StateEntry::Sequences {
        table: table.clone(),
        sequences: Sequences::Source,
        history: true,
    };
    let record = history.to_record();
    assert_eq!(StateEntry::from_record(&record).unwrap(), history);
    // A record written before tables kept history holds no flag.
    let older = record.value.clone();
    let text = String::from_utf8(older.to_vec()).unwrap();
    let text = text.replace(r#","history":true"#, "");
    assert!(!text.contains("history"), "{text}");
    let older = StateRecord {
        key: record.key.clone(),
        value: text.into_bytes().into(),
    };
    let Ok(StateEntry::Sequences { history, .. }) = StateEntry::from_record(&older) else {
        panic!("an older record reads");
    };
    assert!(!history);
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
                value: Bytes::from_static(b"{\"v\":2,\"entry\":{}}"),
            },
            StateError::UnsupportedVersion {
                key: epoch.key.clone(),
                version: 2,
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
                    // Only a table whose sequences are recorded records its history.
                    history: sequences >= 3,
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
