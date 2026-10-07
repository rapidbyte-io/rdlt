//! The bench module's allocation counter in a process whose global allocator it is.

#![forbid(unsafe_code)]

use std::alloc::System;
use std::hint::black_box;

use rdlt_engine::bench::counted;
use stats_alloc::{INSTRUMENTED_SYSTEM, StatsAlloc};

#[global_allocator]
static HEAP: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

#[test]
fn a_run_counts_what_the_global_counting_allocator_served_it() {
    let allocated = counted(HEAP, || {
        let mut grown = black_box(Vec::<u8>::with_capacity(64));
        grown.reserve_exact(1024);
        black_box(grown);
    });
    assert!(allocated.allocations >= 1.0, "{allocated:?}");
    assert!(allocated.reallocations >= 1.0, "{allocated:?}");
    assert!(allocated.bytes >= 1024.0, "{allocated:?}");
}
