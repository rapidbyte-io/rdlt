//! Limits the engine enforces on what it holds, beside its memory budget.

/// A fraction of the memory budget, its denominator: the cursors of the seals waiting for a
/// commit make one due once they take a quarter of the budget.
///
/// Seals' cursors are charged to the budget and only a commit releases them, so a policy that
/// commits by rows alone would otherwise let them fill it.
pub(crate) const CURSOR_SHARE: u64 = 4;
