//! Integration tests for the engine.

#![expect(
    clippy::disallowed_methods,
    reason = "tests drive tokio's paused clock directly"
)]

mod destinations;
mod engine;
mod ledger;
mod merge;
mod schema;
mod support;

/// Tracks the heap's peak, for the memory bound.
#[global_allocator]
static HEAP: peak_alloc::PeakAlloc = peak_alloc::PeakAlloc;
