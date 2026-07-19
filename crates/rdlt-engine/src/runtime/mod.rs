//! Task graph, bounded channels, retry driver, cancellation.
//! All `pub(crate)` — the engine is one deep module (plan.md).

pub(crate) mod channel;
pub(crate) mod graph;
pub(crate) mod lock;
