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
mod carried;
mod change_limits;
mod change_tables;
mod changes;
mod checked;
mod continuous;
mod destinations;
mod engine;
mod exactness;
mod excluded;
mod exponents;
mod following;
mod following_changes;
mod growth;
mod history;
mod json;
mod keys;
mod landed;
mod ledger;
mod lowering;
mod merge;
mod normalize;
mod normalized;
mod owned;
mod phases;
mod placement;
mod recorded;
mod replanning;
mod reserved;
mod reset;
mod schema;
mod shredding;
mod signals;
mod split;
mod stored;
mod support;
mod told;
mod unbounded;
mod unheld;
mod widened;

/// Tracks the heap's peak, for the memory bound.
#[global_allocator]
static HEAP: peak_alloc::PeakAlloc = peak_alloc::PeakAlloc;
