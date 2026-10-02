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

/// Bytes one read-back may take once each row holds its own value, whatever encoding shared it:
/// what its batches expand to, as the cost model measures a batch whose values stay as they are.
pub(super) const PUBLISHED_BYTES: usize = 16 << 20;

/// Bytes a source clause holds of what its reads send, all its reads together: each push's
/// bytes as a budget charges them, each cursor's, and [`HELD_EVENT_BYTES`] for each.
///
/// A source that sends more leaves the clause unobserved, its read stopped.
pub(super) const HELD_BYTES: usize = 64 << 20;

/// Bytes each push and checkpoint a clause holds is charged beside its own, for what holds it.
pub(super) const HELD_EVENT_BYTES: usize = 256;

/// Rows a source clause holds of what its reads send, all its reads together: rows of nothing
/// cost no bytes, and comparing them costs time all the same.
pub(super) const HELD_ROWS: usize = 1 << 20;

/// Bytes of text a clause renders of the rows it compares, at most: rendering stops there, and
/// the clause is left unobserved.
pub const RENDERED_BYTES: usize = 64 << 20;

/// Rows rendered between two yields to the runtime, so a clause's bound can end the rendering.
pub(super) const YIELD_ROWS: usize = 4096;

/// Bytes one record of a JSON push takes at most: a record is parsed whole to compare it, and a
/// parsed record takes many times its text, so a push holding a larger one leaves the clause
/// unobserved.
pub(super) const RECORD_BYTES: usize = 1 << 20;

/// Bytes of a JSON push scanned, or made canonical, between two yields to the runtime.
pub(super) const YIELD_BYTES: usize = 1 << 20;
