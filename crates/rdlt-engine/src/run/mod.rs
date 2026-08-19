//! Running a pipeline: [`retry`] is the driver (each attempt a full run from
//! committed state), [`once`] one attempt's graph, [`validate`] the plan-time
//! stream checks, [`lock`] the one-process-per-workdir lock, [`recover`] the
//! session open with state recovery and WAL replay, [`extract`] one stream's
//! read+shred task, and [`drain`] the loader drive that settles the outcome.

mod drain;
mod extract;
mod lock;
pub(crate) mod once;
mod recover;
pub(crate) mod retry;
pub(crate) mod validate;
