//! Integration tests for the engine.

#![expect(
    clippy::disallowed_methods,
    reason = "tests drive tokio's paused clock directly"
)]

mod change_limits;
mod change_tables;
mod changes;
mod destinations;
mod engine;
mod exactness;
mod json;
mod ledger;
mod merge;
mod normalize;
mod normalized;
mod owned;
mod phases;
mod placement;
mod schema;
mod support;
mod unbounded;

/// Tracks the heap's peak, for the memory bound.
#[global_allocator]
static HEAP: peak_alloc::PeakAlloc = peak_alloc::PeakAlloc;
