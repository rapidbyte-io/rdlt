//! What the messages that carry a pipeline's state hold decoded, as the host's scan counts them,
//! by which an engine holds its stored state to what an open's answer, a commit's request, a
//! plan's request and a report of committed positions may hold.

#[cfg(test)]
mod tests;

use rdlt_wire::prost::Message as _;
use rdlt_wire::scan::{Form, decoded, request, response};

use super::v1;
use crate::commit::CommitMeta;
use crate::state::StateRecord;

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
    answer_bytes(std::slice::from_ref(record)).saturating_sub(answer_bytes(&[]))
}

/// Bytes: what a commit's request carrying `meta` holds decoded.
pub fn commit_bytes(meta: &CommitMeta) -> u64 {
    let request_message = v1::CommitRequest {
        session: u64::MAX,
        meta: Some(v1::CommitMeta::from(meta)),
    };
    counted(request("Commit"), &request_message.encode_to_vec())
}

/// Bytes: what `message`, of `form`, holds decoded; all a number holds where it does not scan.
fn counted(form: Option<&Form>, message: &[u8]) -> u64 {
    let count = form.map_or(usize::MAX, |form| {
        decoded(form, message, usize::MAX).unwrap_or(usize::MAX)
    });
    u64::try_from(count).unwrap_or(u64::MAX)
}
