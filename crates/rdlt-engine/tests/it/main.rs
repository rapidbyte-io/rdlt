//! Integration tests for the engine.

#![forbid(unsafe_code)]
#![expect(
    clippy::disallowed_methods,
    reason = "tests drive tokio's paused clock directly"
)]

mod acknowledging;
mod admitted;
mod bound;
mod budget;
mod change_limits;
mod change_tables;
mod changes;
mod continuous;
mod destinations;
mod engine;
mod exactness;
mod following;
mod following_changes;
mod history;
mod json;
mod ledger;
mod lowering;
mod merge;
mod normalize;
mod normalized;
mod owned;
mod phases;
mod placement;
mod replanning;
mod reset;
mod schema;
mod signals;
mod support;
mod unbounded;
mod wal;
mod wal_changes;

/// Tracks the heap's peak, for the memory bound.
#[global_allocator]
static HEAP: peak_alloc::PeakAlloc = peak_alloc::PeakAlloc;
