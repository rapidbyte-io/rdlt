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
