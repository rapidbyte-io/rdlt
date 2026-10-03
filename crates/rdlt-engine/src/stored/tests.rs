use std::time::UNIX_EPOCH;

use bytes::Bytes;
use proptest::prelude::*;
use rdlt_connector::wire::{answer_bytes, commit_bytes, record_bytes};
use rdlt_connector::{
    CommitMeta, CommitSeq, Epoch, LoadId, SegmentSet, StateChange, StateKey, StateRecord,
};

use super::{StateLimits, Stored};
use crate::config::EngineConfig;
use crate::error::ErrorKind;

/// Bytes: what the tests' state, and requests, may take.
const ROOM: u64 = 768 << 10;

/// Limits of `stored` bytes of records beside what an empty open's answer holds, saturating, and
/// `request` bytes of a commit's request.
fn limits(stored: u64, request: u64) -> StateLimits {
    StateLimits {
        stored: stored.saturating_add(answer_bytes(&[])),
        request,
    }
}

/// The most bytes of a value for which `measure` is within `room`.
fn largest(room: u64, measure: impl Fn(usize) -> u64) -> usize {
    let (mut fits, mut passes) = (0, usize::try_from(room).unwrap());
    assert!(measure(passes) > room);
    while passes - fits > 1 {
        let middle = fits + (passes - fits) / 2;
        if measure(middle) <= room {
            fits = middle;
        } else {
            passes = middle;
        }
    }
    fits
}

impl Stored {
    /// Bytes: what the stored records add to an empty open's answer.
    fn held(&self) -> u64 {
        self.total() - answer_bytes(&[])
    }
}

fn record(key: &str, bytes: usize) -> StateRecord {
    StateRecord {
        key: key.to_owned(),
        value: Bytes::from(vec![7; bytes]),
    }
}

impl Stored {
    /// Admits `meta`, of no new child table.
    fn admit_all(&self, meta: &CommitMeta) -> Result<(), crate::error::Error> {
        self.admit(meta, &[])
    }
}

fn meta(delta: Vec<StateChange>) -> CommitMeta {
    CommitMeta {
        load_id: LoadId::from_parts(UNIX_EPOCH, 1),
        commit_seq: CommitSeq::FIRST,
        epoch: Epoch(1),
        segments: SegmentSet::new(),
        state_delta: delta,
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
    }
}

#[test]
fn stored_state_is_what_its_records_take_in_an_open_s_answer() {
    let records = [record("a", 10), record("b", 300)];
    let stored = Stored::of(&records, limits(ROOM, ROOM));
    assert_eq!(stored.held(), records.iter().map(record_bytes).sum::<u64>());
    assert_eq!(Stored::of(&[], limits(ROOM, ROOM)).held(), 0);
}

#[test]
fn a_commit_is_admitted_while_it_fits_and_refused_a_byte_past() {
    let stored = Stored::of(&[record("a", 1000)], limits(u64::MAX, ROOM));
    let room = ROOM;
    // Replacing `a` with a record whose commit's request takes all the room, and a byte more:
    // the request carries the record and the commit's numbers beside.
    let replacing = |bytes| meta(vec![StateChange::Put(record("a", bytes))]);
    let bytes = largest(room, |bytes| commit_bytes(&replacing(bytes)));
    assert!(commit_bytes(&replacing(bytes)) <= room);
    assert!(commit_bytes(&replacing(bytes + 1)) > room);
    assert!(record_bytes(&record("a", bytes)) < room);
    stored.admit_all(&replacing(bytes)).unwrap();
    let error = stored.admit_all(&replacing(bytes + 1)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Config);
    assert_eq!(error.code(), Some("state_bytes_exceeded"));
    assert!(!error.is_retryable());
}

#[test]
fn a_commit_whose_state_passes_the_limit_is_refused_though_its_request_fits() {
    // What is stored already takes most of the room: a small record more passes it.
    let room = ROOM;
    let bytes = largest(room, |bytes| record_bytes(&record("a", bytes)));
    let full = Stored::of(&[record("a", bytes)], limits(ROOM, u64::MAX));
    assert!(full.held() <= room && room - full.held() < record_bytes(&record("b", 0)));
    full.admit_all(&meta(vec![StateChange::Delete("b".to_owned())]))
        .unwrap();
    let error = full
        .admit_all(&meta(vec![StateChange::Put(record("b", 0))]))
        .unwrap_err();
    assert_eq!(error.code(), Some("state_bytes_exceeded"));
    // Deleting what is stored makes room as the commit lands.
    let freeing = meta(vec![
        StateChange::Delete("a".to_owned()),
        StateChange::Put(record("b", 1000)),
    ]);
    full.admit_all(&freeing).unwrap();
}

#[test]
fn a_commit_that_grows_no_state_is_admitted_though_state_is_past_the_limit() {
    // State stored under a larger limit, before the memory was lowered.
    let records = [record("a", 2000), record("b", 2000)];
    let over = Stored::of(&records, limits(1000, ROOM));
    assert!(over.total() > 1000);
    // A commit that frees some of it, or replaces a record with one of its size, is admitted.
    over.admit_all(&meta(vec![StateChange::Delete("a".to_owned())]))
        .unwrap();
    over.admit_all(&meta(vec![StateChange::Put(record("a", 2000))]))
        .unwrap();
    let shrinking = meta(vec![
        StateChange::Delete("a".to_owned()),
        StateChange::Put(record("c", 1500)),
    ]);
    over.admit_all(&shrinking).unwrap();
    // One that grows it by a byte is refused.
    let error = over
        .admit_all(&meta(vec![StateChange::Put(record("a", 2001))]))
        .unwrap_err();
    assert_eq!(error.code(), Some("state_bytes_exceeded"));
}

#[test]
fn past_the_limit_the_receipt_taking_a_byte_more_grows_no_state() {
    let receipt = StateKey::Receipt.encode();
    let records = [record("a", 2000), record(&receipt, 100)];
    let over = Stored::of(&records, limits(1000, ROOM));
    let replacing = |bytes, receipt_bytes| {
        meta(vec![
            StateChange::Put(record("a", bytes)),
            StateChange::Put(record(&receipt, receipt_bytes)),
        ])
    };
    over.admit_all(&replacing(2000, 101)).unwrap();
    over.admit_all(&replacing(1999, 200)).unwrap();
    // Another record growing is refused, though the receipt shrinks by as much.
    let error = over.admit_all(&replacing(2001, 99)).unwrap_err();
    assert_eq!(error.code(), Some("state_bytes_exceeded"));
}

#[test]
fn a_commit_whose_request_passes_the_limit_is_refused_though_its_state_fits() {
    // A delete of a key nothing stores leaves the state as it is, yet takes room in the request.
    let stored = Stored::of(&[], limits(u64::MAX, ROOM));
    let key = "k".repeat(usize::try_from(ROOM).unwrap());
    let deleting = meta(vec![StateChange::Delete(key)]);
    assert!(commit_bytes(&deleting) > ROOM);
    let error = stored.admit_all(&deleting).unwrap_err();
    assert_eq!(error.code(), Some("state_bytes_exceeded"));
}

#[test]
fn landed_changes_replace_and_remove_what_is_stored() {
    let mut stored = Stored::of(&[record("a", 10), record("b", 20)], limits(ROOM, ROOM));
    let delta = [
        StateChange::Put(record("a", 500)),
        StateChange::Delete("b".to_owned()),
        StateChange::Put(record("c", 30)),
        StateChange::Delete("missing".to_owned()),
    ];
    stored.apply(&delta);
    let expected = record_bytes(&record("a", 500)) + record_bytes(&record("c", 30));
    assert_eq!(stored.held(), expected);
    // A later change sees what landed: `b` is gone, so deleting it frees nothing more.
    stored.apply(&[StateChange::Delete("b".to_owned())]);
    assert_eq!(stored.held(), expected);
    stored.apply(&[StateChange::Delete("a".to_owned())]);
    assert_eq!(stored.held(), record_bytes(&record("c", 30)));
}

#[test]
fn a_key_changed_twice_in_one_commit_counts_its_last_change() {
    let stored = Stored::of(&[], limits(ROOM, ROOM));
    let twice = meta(vec![
        StateChange::Put(record("a", 100_000)),
        StateChange::Delete("a".to_owned()),
    ]);
    stored.admit_all(&twice).unwrap();
    let mut landed = Stored::of(&[], limits(ROOM, ROOM));
    landed.apply(&twice.state_delta);
    assert_eq!(landed.held(), 0);
}

#[test]
fn stored_state_and_requests_are_held_to_the_state_limit_the_engine_advertises() {
    let config = |memory: u64| EngineConfig::builder().memory(memory).build().unwrap();
    for memory in [EngineConfig::least_memory(16), 64 << 20, 4 << 30] {
        let config = config(memory);
        let advertised = config.limits().state_bytes;
        assert_eq!(config.state_limit(), advertised);
        let held = StateLimits::of(&config);
        assert_eq!((held.stored, held.request), (advertised, advertised));
        // A stream's child tables are as many as the state holds tables of a few columns.
        let tables = usize::try_from(advertised / 4096).unwrap();
        assert_eq!(config.child_table_limit(), tables.clamp(1, 1024));
    }
    // An operator's lower state limit holds both.
    let operated = EngineConfig::builder()
        .limits(rdlt_wire::Limits {
            state_bytes: 64 << 10,
            ..rdlt_wire::Limits::default()
        })
        .build()
        .unwrap();
    assert_eq!(StateLimits::of(&operated).stored, 64 << 10);
    assert_eq!(operated.child_table_limit(), 16);
}

#[test]
fn an_empty_state_holds_the_answer_alone_and_a_record_its_bytes_and_more() {
    let empty = Stored::of(&[], limits(ROOM, ROOM));
    assert_eq!(empty.total(), answer_bytes(&[]));
    let one = empty.after(&[StateChange::Put(record("key", 1000))]);
    assert!(one >= empty.total() + 1000 + 3, "{one}");
}

#[test]
fn state_past_the_limit_only_with_new_child_tables_is_refused_for_them() {
    let stream = rdlt_connector::StreamName::new("orders").unwrap();
    let room = record_bytes(&record("a", 600)) + record_bytes(&record("b", 100)) + 10;
    let full = Stored::of(&[record("a", 600)], limits(room, ROOM));
    let born = [(stream.clone(), "child".to_owned())];
    let error = full
        .admit(
            &meta(vec![
                StateChange::Put(record("b", 100)),
                StateChange::Put(record("child", 500)),
            ]),
            &born,
        )
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Schema);
    assert_eq!(error.code(), Some("child_tables_exceeded"));
    assert_eq!(error.stream(), Some(&stream));
    assert!(!error.is_retryable());
    // Without its child tables the state still passes the limit: it is the state's.
    let error = full
        .admit(
            &meta(vec![
                StateChange::Put(record("b", 500)),
                StateChange::Put(record("child", 100)),
            ]),
            &born,
        )
        .unwrap_err();
    assert_eq!(error.code(), Some("state_bytes_exceeded"));
    // A commit within the limit is admitted, child tables and all.
    let commit = meta(vec![StateChange::Put(record("child", 100))]);
    full.admit(&commit, &born).unwrap();
}

#[test]
fn a_commit_past_the_limit_is_relieved_by_the_fewest_records_deleted_in_order() {
    let records = [record("a", 300), record("b", 300), record("c", 300)];
    let bytes = |key: &str| record_bytes(&record(key, 300));
    let small = record_bytes(&record("d", 10));
    let room = 3 * bytes("a") + small;
    let stored = Stored::of(&records, limits(room, ROOM));
    let limit = room + answer_bytes(&[]);
    // A commit that fits needs nothing deleted.
    let fits = meta(vec![StateChange::Put(record("d", 10))]);
    assert_eq!(stored.relief(&fits, &["a", "b", "c"]), Some(0));
    // One that passes the limit by a byte more than one record takes needs two.
    let one = bytes("a");
    let past = largest(small + one, |value| record_bytes(&record("d", value))) + 1;
    let grown = meta(vec![StateChange::Put(record("d", past))]);
    let over = stored.after(&grown.state_delta) - limit;
    assert!(
        over > bytes("a") && over <= bytes("a") + bytes("b"),
        "{over}"
    );
    assert_eq!(stored.relief(&grown, &["a", "b", "c"]), Some(2));
    // Records it does not store free nothing, and too few records relieve nothing.
    assert_eq!(stored.relief(&grown, &["x", "a"]), None);
    assert_eq!(stored.relief(&grown, &[]), None);
    let mut relieved = grown.clone();
    for key in ["a", "b"] {
        relieved
            .state_delta
            .push(StateChange::Delete(key.to_owned()));
    }
    stored.admit_all(&relieved).unwrap();
}

fn change() -> impl Strategy<Value = StateChange> {
    let key = prop::sample::select(vec!["a", "b", "c", "", "partition/p0"]);
    prop_oneof![
        (key.clone(), 0..40_usize).prop_map(|(key, bytes)| StateChange::Put(record(key, bytes))),
        key.prop_map(|key| StateChange::Delete(key.to_owned())),
    ]
}

/// `records` with `delta` committed, by key.
fn committed(records: &[StateRecord], delta: &[StateChange]) -> Vec<StateRecord> {
    let mut state: std::collections::BTreeMap<String, StateRecord> = records
        .iter()
        .map(|record| (record.key.clone(), record.clone()))
        .collect();
    for change in delta {
        match change {
            StateChange::Put(record) => {
                state.insert(record.key.clone(), record.clone());
            }
            StateChange::Delete(key) => {
                state.remove(key);
            }
        }
    }
    state.into_values().collect()
}

proptest! {
    #[test]
    fn what_is_stored_is_what_an_open_s_answer_carrying_it_holds_decoded(
        first in prop::collection::vec(change(), 0..8),
        then in prop::collection::vec(change(), 0..8),
    ) {
        let records = committed(&[], &first);
        let mut stored = Stored::of(&records, limits(ROOM, ROOM));
        prop_assert_eq!(stored.total(), answer_bytes(&records));
        let after = committed(&records, &then);
        prop_assert_eq!(stored.after(&then), answer_bytes(&after));
        stored.apply(&then);
        prop_assert_eq!(stored.total(), answer_bytes(&after));
    }
}
