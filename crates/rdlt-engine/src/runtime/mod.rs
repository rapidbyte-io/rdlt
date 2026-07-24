//! Run orchestration, bounded channels, retry driver, cancellation.
//! All `pub(crate)` — the engine is one deep module (plan.md).

pub(crate) mod channel;
pub(crate) mod lock;
pub(crate) mod run;
