//! What a pipeline's committed state holds decoded, as an open's answer carries it back.
//!
//! The engine commits no state that would hold more decoded than a message of state may: a
//! pipeline can always open what it committed.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use rdlt_connector::{StateChange, StateRecord};
use rdlt_wire::prost::Message as _;
use rdlt_wire::scan::{decoded, response};
use rdlt_wire::v1;

/// What each record of the committed state holds decoded in an open's answer, and the answer in
/// all.
#[derive(Debug, Default)]
pub(crate) struct HeldState {
    records: BTreeMap<String, u64>,
    held: u64,
}

impl HeldState {
    /// What the state of `records`, as an open answered them, holds.
    pub(crate) fn of(records: &[StateRecord]) -> Self {
        let mut held = Self {
            records: BTreeMap::new(),
            held: answered(&[]),
        };
        for record in records {
            held.put(record);
        }
        held
    }

    /// What an open's answer would hold decoded once `delta` is committed.
    pub(crate) fn after(&self, delta: &[StateChange]) -> u64 {
        let mut changed: BTreeMap<&str, u64> = BTreeMap::new();
        for change in delta {
            let (key, bytes) = match change {
                StateChange::Put(record) => (record.key.as_str(), held(record)),
                StateChange::Delete(key) => (key.as_str(), 0),
            };
            changed.insert(key, bytes);
        }
        changed.into_iter().fold(self.held, |held, (key, bytes)| {
            let before = self.records.get(key).copied().unwrap_or(0);
            held.saturating_sub(before).saturating_add(bytes)
        })
    }

    /// Commits `delta`.
    pub(crate) fn apply(&mut self, delta: &[StateChange]) {
        for change in delta {
            match change {
                StateChange::Put(record) => self.put(record),
                StateChange::Delete(key) => {
                    let before = self.records.remove(key).unwrap_or(0);
                    self.held = self.held.saturating_sub(before);
                }
            }
        }
    }

    fn put(&mut self, record: &StateRecord) {
        let bytes = held(record);
        let before = self.records.insert(record.key.clone(), bytes).unwrap_or(0);
        self.held = self.held.saturating_sub(before).saturating_add(bytes);
    }
}

/// What an open's answer of `records` holds decoded, as the host's scan counts it.
fn answered(records: &[StateRecord]) -> u64 {
    let answer = v1::OpenResponse {
        state: records
            .iter()
            .map(|record| v1::StateRecord {
                key: record.key.clone(),
                value: record.value.clone(),
            })
            .collect(),
        ..v1::OpenResponse::default()
    };
    let count = response("Open").map_or(usize::MAX, |form| {
        decoded(form, &answer.encode_to_vec(), usize::MAX).unwrap_or(usize::MAX)
    });
    u64::try_from(count).unwrap_or(u64::MAX)
}

/// What `record` adds to an open's answer decoded.
fn held(record: &StateRecord) -> u64 {
    answered(std::slice::from_ref(record)).saturating_sub(answered(&[]))
}
