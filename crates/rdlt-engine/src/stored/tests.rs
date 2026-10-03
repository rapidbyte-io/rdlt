use std::time::UNIX_EPOCH;

use bytes::Bytes;
use rdlt_connector::wire::{commit_bytes, record_bytes};
use rdlt_connector::{CommitMeta, CommitSeq, Epoch, LoadId, SegmentSet, StateChange, StateRecord};

use super::{StateLimits, Stored};
use crate::config::{EngineConfig, GrowthLimits};
use crate::error::ErrorKind;

/// Bytes: what the tests' state, and requests, may take.
const ROOM: u64 = 768 << 10;

/// Limits of `stored` bytes of state and `request` bytes of a commit's request.
fn limits(stored: u64, request: u64) -> StateLimits {
    StateLimits { stored, request }
}

fn record(key: &str, bytes: usize) -> StateRecord {
    StateRecord {
        key: key.to_owned(),
        value: Bytes::from(vec![7; bytes]),
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
    assert_eq!(
        stored.total(),
        records.iter().map(record_bytes).sum::<u64>()
    );
    assert_eq!(Stored::of(&[], limits(ROOM, ROOM)).total(), 0);
}

#[test]
fn a_commit_is_admitted_while_it_fits_and_refused_a_byte_past() {
    let stored = Stored::of(&[record("a", 1000)], limits(u64::MAX, ROOM));
    let room = ROOM;
    // Replacing `a` with a record whose commit's request takes all the room, and a byte more:
    // the request carries the record and the commit's numbers beside.
    let replacing = |bytes| meta(vec![StateChange::Put(record("a", bytes))]);
    let mut bytes = usize::try_from(room).unwrap() - 128;
    while commit_bytes(&replacing(bytes)) < room {
        bytes += 1;
    }
    assert_eq!(commit_bytes(&replacing(bytes)), room);
    assert!(record_bytes(&record("a", bytes)) < room);
    stored.admit(&replacing(bytes)).unwrap();
    let error = stored.admit(&replacing(bytes + 1)).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Config);
    assert_eq!(error.code(), Some("state_bytes_exceeded"));
    assert!(!error.is_retryable());
}

#[test]
fn a_commit_whose_state_passes_the_limit_is_refused_though_its_request_fits() {
    // What is stored already takes most of the room: a small record more passes it.
    let room = ROOM;
    let mut bytes = usize::try_from(room).unwrap() - 128;
    while record_bytes(&record("a", bytes)) < room {
        bytes += 1;
    }
    let full = Stored::of(&[record("a", bytes)], limits(ROOM, u64::MAX));
    assert_eq!(full.total(), room);
    full.admit(&meta(vec![StateChange::Delete("b".to_owned())]))
        .unwrap();
    let error = full
        .admit(&meta(vec![StateChange::Put(record("b", 0))]))
        .unwrap_err();
    assert_eq!(error.code(), Some("state_bytes_exceeded"));
    // Deleting what is stored makes room as the commit lands.
    let freeing = meta(vec![
        StateChange::Delete("a".to_owned()),
        StateChange::Put(record("b", 1000)),
    ]);
    full.admit(&freeing).unwrap();
}

#[test]
fn a_commit_whose_request_passes_the_limit_is_refused_though_its_state_fits() {
    // A delete of a key nothing stores leaves the state as it is, yet takes room in the request.
    let stored = Stored::of(&[], limits(u64::MAX, ROOM));
    let key = "k".repeat(usize::try_from(ROOM).unwrap());
    let deleting = meta(vec![StateChange::Delete(key)]);
    assert!(commit_bytes(&deleting) > ROOM);
    let error = stored.admit(&deleting).unwrap_err();
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
    assert_eq!(stored.total(), expected);
    // A later change sees what landed: `b` is gone, so deleting it frees nothing more.
    stored.apply(&[StateChange::Delete("b".to_owned())]);
    assert_eq!(stored.total(), expected);
    stored.apply(&[StateChange::Delete("a".to_owned())]);
    assert_eq!(stored.total(), record_bytes(&record("c", 30)));
}

#[test]
fn a_key_changed_twice_in_one_commit_counts_its_last_change() {
    let stored = Stored::of(&[], limits(ROOM, ROOM));
    let twice = meta(vec![
        StateChange::Put(record("a", 100_000)),
        StateChange::Delete("a".to_owned()),
    ]);
    stored.admit(&twice).unwrap();
    let mut landed = Stored::of(&[], limits(ROOM, ROOM));
    landed.apply(&twice.state_delta);
    assert_eq!(landed.total(), 0);
}

#[test]
fn stored_state_is_held_to_its_share_of_the_memory_and_requests_to_a_message() {
    let config = |memory: u64| EngineConfig::builder().memory(memory).build().unwrap();
    let least = EngineConfig::least_memory(16);
    let held = StateLimits::of(&config(least));
    assert_eq!(held.stored, least / 16);
    assert_eq!(held.request, (16 << 20) - (256 << 10));
    // A large budget is held to what a message carrying state may take.
    let large = StateLimits::of(&config(4 << 30));
    assert_eq!(large.stored, (16 << 20) - (256 << 10));
    let growth = GrowthLimits::new(1024, 128, 512 << 20).unwrap();
    let raised = EngineConfig::builder()
        .memory(4 << 30)
        .growth(growth)
        .build()
        .unwrap();
    assert_eq!(StateLimits::of(&raised).stored, (4 << 30) / 16);
    // A stream's child tables are as many as the state holds tables of a few columns.
    assert_eq!(
        config(least).child_table_limit(),
        usize::try_from(least / 16 / 4096).unwrap()
    );
    assert_eq!(config(256 << 20).child_table_limit(), 1024);
}
