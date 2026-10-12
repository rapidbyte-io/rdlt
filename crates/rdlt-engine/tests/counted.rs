//! What the bench module's allocation counter sees and what the log's frames allocate and hold,
//! in a process whose global allocator counts.

#![forbid(unsafe_code)]

use std::alloc::System;
use std::hint::black_box;

use rdlt_engine::bench::counted;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

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

#[test]
fn a_batch_frame_allocates_at_most_two_and_six_tenths_times_its_bytes() {
    let batch = rdlt_engine::bench::Passthrough::batch(0, 12_000);
    let mut len = 0;
    let allocated = counted(HEAP, || {
        len = rdlt_engine::bench::batch_frame(batch.clone()).len();
    });
    let ratio = allocated.bytes / f64::from(u32::try_from(len).expect("a frame within 4 GiB"));
    assert!(ratio <= 2.6, "{ratio} of a {len}-byte frame");
}

#[test]
fn a_json_frame_once_encoded_holds_no_more_than_its_bytes() {
    let region = Region::new(HEAP);
    let frame = rdlt_engine::bench::schema_frame(1_500);
    let change = region.change();
    let held = change
        .bytes_allocated
        .saturating_sub(change.bytes_deallocated);
    assert!(frame.len() > 32 * 1024, "a {}-byte frame", frame.len());
    assert!(
        held <= frame.len() + 1024,
        "{held} bytes held by a {}-byte frame",
        frame.len()
    );
}
