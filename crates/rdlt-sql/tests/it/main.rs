//! rdlt-sql against its bounds, DataFusion's executor, the `T` clauses and the engine.

#![forbid(unsafe_code)]

mod arithmetic;
mod bounds;
mod certified;
mod composed;
mod differential;
mod engine;
mod peaks;
mod queries;
mod served;

/// Tracks the heap's peak, for the bound a query declares on all it holds at once.
#[global_allocator]
static HEAP: peak_alloc::PeakAlloc = peak_alloc::PeakAlloc;
