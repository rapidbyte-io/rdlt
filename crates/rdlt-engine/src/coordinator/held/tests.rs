use bytes::Bytes;
use proptest::prelude::*;
use rdlt_connector::{StateChange, StateRecord};

use super::{HeldState, answered};

fn record(key: &str, value: &[u8]) -> StateRecord {
    StateRecord {
        key: key.to_owned(),
        value: Bytes::copy_from_slice(value),
    }
}

fn change() -> impl Strategy<Value = StateChange> {
    let key = prop::sample::select(vec!["a", "b", "c", "", "partition/p0"]);
    prop_oneof![
        (key.clone(), prop::collection::vec(any::<u8>(), 0..40))
            .prop_map(|(key, value)| StateChange::Put(record(key, &value))),
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
    fn what_the_state_holds_is_what_the_scan_counts_of_an_open_s_answer_carrying_it(
        first in prop::collection::vec(change(), 0..8),
        then in prop::collection::vec(change(), 0..8),
    ) {
        let records = committed(&[], &first);
        let mut held = HeldState::of(&records);
        prop_assert_eq!(held.held, answered(&records));
        let after = committed(&records, &then);
        prop_assert_eq!(held.after(&then), answered(&after));
        held.apply(&then);
        prop_assert_eq!(held.held, answered(&after));
    }
}

#[test]
fn an_empty_state_holds_the_answer_alone_and_a_record_its_bytes_and_more() {
    let empty = HeldState::of(&[]);
    assert_eq!(empty.held, answered(&[]));
    let value = vec![7; 1_000];
    let one = empty.after(&[StateChange::Put(record("key", &value))]);
    assert!(one >= empty.held + 1_000 + 3, "{one}");
}
