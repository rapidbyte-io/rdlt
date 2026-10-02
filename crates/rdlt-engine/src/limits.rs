//! Limits the engine enforces on what it holds, beside its memory budget.

use std::time::Duration;

/// A fraction of the memory budget, its denominator: the cursors of the seals waiting for a
/// commit make one due once they take a quarter of the budget.
///
/// Seals' cursors are charged to the budget and only a commit releases them, so a policy that
/// commits by rows alone would otherwise let them fill it.
pub(crate) const CURSOR_SHARE: u64 = 4;

/// How long a request waits for room in the memory budget by default, before the attempt fails
/// with what held the budget: an hour, longer than any call of a destination may take by default,
/// the thirty minutes of a commit.
///
/// A wait ends sooner once what is in flight is written or a commit lands; one that lasts this
/// long waits for bytes nothing will release.
pub(crate) const BUDGET_WAIT: Duration = Duration::from_secs(3600);

/// The code of the error a wait on the memory budget ends with at its deadline.
pub(crate) const BUDGET_WAIT_EXCEEDED: &str = "memory_budget_wait_exceeded";
