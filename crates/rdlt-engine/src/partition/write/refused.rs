//! The errors a write refuses a batch or a row with.

use rdlt_connector::SchemaError;

use super::pieces::RowTooLarge;
use crate::error::{Error, ErrorKind, codes};
use crate::partition::PartitionJob;

/// The error for a batch of `job`'s stream holding a column with no table schema.
pub(super) fn schema_invalid(job: &PartitionJob, error: &SchemaError) -> Error {
    Error::schema(format!(
        "stream {}: a batch has no table schema: {error}",
        job.stream
    ))
    .with_code(codes::BATCH_SCHEMA_INVALID)
    .with_stream(&job.stream)
}

/// The error for a row that alone takes more to lower than a request may take of the budget:
/// no piece of it can be reserved.
pub(super) fn row_too_large(job: &PartitionJob, row: &RowTooLarge) -> Error {
    Error::new(
        ErrorKind::Source,
        format!(
            "stream {}: lowering one row takes more than {} bytes, beyond the {} one request may \
             take of the memory budget",
            job.stream, row.expanded, row.limit
        ),
    )
    .with_code(codes::ROW_EXCEEDS_BUDGET)
    .with_stream(&job.stream)
}

/// Bytes: what one request of the budget holds beside a row that does not fit what it leaves.
#[derive(Clone, Copy, Debug)]
pub(super) struct RequestHeld {
    /// What one request may take of the budget.
    pub(super) request: u64,
    /// What building a JSON flush's batches took beyond their text.
    pub(super) built: u64,
    /// What is kept for the row's piece's split: what it holds, or may make.
    pub(super) held: u64,
}

/// The error for a row that takes more to lower than what a request leaves it beside what
/// `beside` says it holds: what the row takes against the room it had, and what took the rest.
pub(super) fn row_beside(job: &PartitionJob, row: &RowTooLarge, beside: RequestHeld) -> Error {
    let mut taken = Vec::new();
    if beside.built > 0 {
        let built = beside.built;
        taken.push(format!(
            "the {built} its JSON's batches take beyond their text"
        ));
    }
    if beside.held > 0 {
        let held = beside.held;
        taken.push(format!("the {held} kept for its piece's split"));
    }
    let taken = if taken.is_empty() {
        String::new()
    } else {
        format!(", beside {}", taken.join(" and "))
    };
    Error::new(
        ErrorKind::Source,
        format!(
            "stream {}: lowering one row takes at least {} bytes, more than the {} left to it of \
             the {} one request may take of the memory budget{taken}",
            job.stream, row.expanded, row.limit, beside.request
        ),
    )
    .with_code(codes::ROW_EXCEEDS_BUDGET)
    .with_stream(&job.stream)
}
