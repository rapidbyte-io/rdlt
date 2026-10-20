//! Integration tests for the simulation harness.

#![forbid(unsafe_code)]

mod changes;
mod determinism;
mod exactly_once;
mod hostile;
mod stress;

/// Counts each thread's heap, so a seed's attempts are held to what their runs reserve.
#[global_allocator]
static HEAP: alloc_tracker::Allocator<std::alloc::System> = alloc_tracker::Allocator::system();
