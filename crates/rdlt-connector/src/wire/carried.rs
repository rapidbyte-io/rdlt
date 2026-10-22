//! What the messages that carry a pipeline's state hold decoded, as the host's scan counts them,
//! by which an engine holds its stored state to what an open's answer, a commit's request, a
//! plan's request and a report of committed positions may hold.

#[cfg(test)]
mod tests;

use std::sync::LazyLock;

use bytes::Bytes;
use rdlt_wire::prost::Message as _;
use rdlt_wire::scan::{self, Form, decoded, request, response};

use super::{destination, v1};
use crate::commit::CommitMeta;
use crate::state::{StateChange, StateRecord};

/// Bytes: what an open's answer carrying `records` holds decoded.
pub fn answer_bytes(records: &[StateRecord]) -> u64 {
    let answer = v1::OpenResponse {
        state: records.iter().map(v1::StateRecord::from).collect(),
        ..v1::OpenResponse::default()
    };
    counted(response("Open"), &answer.encode_to_vec())
}

/// Bytes: what `record` adds to an open's answer decoded, as one of the records it carries.
pub fn record_bytes(record: &StateRecord) -> u64 {
    // A record of no key and no value: its entry in the answer alone.
    static ENTRY: LazyLock<u64> = LazyLock::new(|| {
        let record = StateRecord {
            key: String::new(),
            value: Bytes::new(),
        };
        answer_bytes(&[record]).saturating_sub(answer_bytes(&[]))
    });
    ENTRY
        .saturating_add(present(record.key.len()))
        .saturating_add(present(record.value.len()))
}

/// Bytes: what a commit's request carrying `meta` holds decoded.
pub fn commit_bytes(meta: &CommitMeta) -> u64 {
    let request_message = v1::CommitRequest {
        session: u64::MAX,
        meta: Some(destination::unchanged(meta)),
    };
    let others = counted(request("Commit"), &request_message.encode_to_vec());
    meta.state_delta
        .iter()
        .map(change_bytes)
        .fold(others, u64::saturating_add)
}

/// Bytes: what `change` adds to a commit's request decoded, as one of its state changes.
fn change_bytes(change: &StateChange) -> u64 {
    use v1::state_change::Change;
    static PUT: LazyLock<u64> = LazyLock::new(|| one(Change::Put(v1::StateRecord::default())));
    static DELETE: LazyLock<u64> =
        LazyLock::new(|| one(Change::Delete(String::new())).saturating_sub(held(0)));
    match change {
        StateChange::Put(record) => PUT
            .saturating_add(present(record.key.len()))
            .saturating_add(present(record.value.len())),
        // A delete is one of a choice of members, written however short its key.
        StateChange::Delete(key) => DELETE.saturating_add(held(key.len())),
    }
}

/// Bytes: what a commit's request holds decoded for `change`, its one state change.
fn one(change: v1::state_change::Change) -> u64 {
    let request_message = |state_delta| v1::CommitRequest {
        session: 0,
        meta: Some(v1::CommitMeta {
            state_delta,
            ..v1::CommitMeta::default()
        }),
    };
    let changed = vec![v1::StateChange {
        change: Some(change),
    }];
    let with = counted(request("Commit"), &request_message(changed).encode_to_vec());
    let without = counted(
        request("Commit"),
        &request_message(Vec::new()).encode_to_vec(),
    );
    with.saturating_sub(without)
}

/// Bytes: what a text or bytes field of `length` bytes holds decoded, where the encoding leaves
/// out an empty one.
fn present(length: usize) -> u64 {
    if length == 0 { 0 } else { held(length) }
}

/// Bytes: what a text or bytes field of `length` bytes holds decoded.
fn held(length: usize) -> u64 {
    u64::try_from(scan::field_bytes(length)).unwrap_or(u64::MAX)
}

/// Bytes: what `message`, of `form`, holds decoded; all a number holds where it does not scan.
fn counted(form: Option<&Form>, message: &[u8]) -> u64 {
    let count = form.map_or(usize::MAX, |form| {
        decoded(form, message, usize::MAX).unwrap_or(usize::MAX)
    });
    u64::try_from(count).unwrap_or(u64::MAX)
}
