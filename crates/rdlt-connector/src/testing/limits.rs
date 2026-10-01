//! Every limit certification keeps: what it waits, holds, renders and reports of a connector.

use std::time::Duration;

/// How long any single connector call may take during certification.
pub(super) const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// The longest one clause takes, all its calls together: one that takes longer fails, rather
/// than hold the certification, whichever of its awaits never ends.
pub(super) const CLAUSE_TIMEOUT: Duration = Duration::from_secs(600);

/// Bytes of UTF-8 a [`Reason`](super::Reason) holds at most, its mark of a cut included.
pub const REASON_BYTES: usize = 2048;

/// How many rows a reason shows of the rows a connector sent, before it only counts them.
pub(super) const SHOWN_ROWS: usize = 8;

/// How many of a partition's checkpoints a resume is checked from, spread over its whole read.
pub(super) const RESUME_SAMPLES: usize = 5;

/// Rows a clause reads of one table a destination published, at most: a clause publishes tens,
/// and a read-back of more fails it before any row is expanded.
pub(super) const PUBLISHED_ROWS: usize = 10_000;

/// Bytes the columns a clause reads of one read-back may take once each row holds its own value,
/// whatever encoding shared it: the rows times each column's widest value, summed.
pub(super) const PUBLISHED_BYTES: usize = 16 << 20;
