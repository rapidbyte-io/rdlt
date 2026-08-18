//! Run orchestration, bounded channels, retry driver, cancellation.
//! All `pub(crate)` — the engine is one deep module whose only public surface
//! is `Engine`/`EngineConfig` at the crate root.

mod drain;
mod extract;
mod lock;
pub(crate) mod recover;
pub(crate) mod run;
mod validate;

#[cfg(test)]
pub(crate) use run::STAGE_MSG_CAPACITY;
