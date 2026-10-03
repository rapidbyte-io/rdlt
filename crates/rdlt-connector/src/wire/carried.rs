//! What the messages that carry a pipeline's state take on the wire, by which an engine holds its
//! stored state to what an open's answer, a commit's request, a plan's request and a report of
//! committed positions may take.

#[cfg(test)]
mod tests;

use rdlt_wire::prost::Message as _;
use rdlt_wire::prost::encoding::encoded_len_varint;

use super::v1;
use crate::commit::CommitMeta;
use crate::state::StateRecord;

/// Bytes: what `record` adds to an open's answer, as one of the records it carries.
pub fn record_bytes(record: &StateRecord) -> u64 {
    let field = |len: usize| match len {
        0 => 0,
        len => 1 + encoded_len_varint(wide(len)) + len,
    };
    let body = field(record.key.len()) + field(record.value.len());
    wide(1 + encoded_len_varint(wide(body)) + body)
}

/// Bytes: what a commit's request carrying `meta` takes, beside its session's handle.
pub fn commit_bytes(meta: &CommitMeta) -> u64 {
    let request = v1::CommitRequest {
        session: 0,
        meta: Some(v1::CommitMeta::from(meta)),
    };
    wide(request.encoded_len())
}

fn wide(len: usize) -> u64 {
    u64::try_from(len).unwrap_or(u64::MAX)
}
