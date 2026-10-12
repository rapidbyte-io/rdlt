//! What the bench module's allocation counter sees, and what the log's frames, shredding rows of
//! arrays and checking escaped JSON allocate and hold, in a process whose global allocator counts.

#![forbid(unsafe_code)]

use std::alloc::System;
use std::hint::black_box;
use std::num::NonZeroU64;

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

#[test]
fn shredding_rows_of_arrays_allocates_no_observation_a_row() {
    let pushes = rdlt_engine::bench::Corpus::WithArrays.pushes(1 << 20);
    let rows: usize = pushes
        .iter()
        .map(|push| memchr::memchr_iter(b'\n', push).count())
        .sum();
    let rows = NonZeroU64::new(u64::try_from(rows).unwrap()).unwrap();
    let allocated = counted(HEAP, || {
        rdlt_engine::bench::shred(&pushes, rdlt_engine::bench::CHUNK_BYTES).unwrap();
    });
    let a_row = allocated.per(rows).allocations;
    assert!(a_row <= 0.1, "{a_row} allocations a row");
}

#[test]
fn checking_a_column_of_escaped_json_allocates_nothing_a_row() {
    let batch = rdlt_engine::bench::json_column(8_192, true);
    let allocated = counted(HEAP, || rdlt_engine::bench::check_json(&batch).unwrap());
    assert!(allocated.allocations < 64.0, "{allocated:?}");
}

#[test]
fn the_exact_parse_allocates_no_string_a_number() {
    // `j`, beyond 64 bits, has each chunk parsed again exactly; sonic-rs's tree of a record
    // allocates alike for one number as for ten.
    let exact = |row: &str| {
        let push = bytes::Bytes::from(format!("{row}\n").repeat(1_000));
        counted(HEAP, || {
            rdlt_engine::bench::shred(&[push], 1 << 20).unwrap();
        })
        .allocations
    };
    let ten = exact(
        r#"{"a":1.5,"b":2.5,"c":3.5,"d":4.5,"e":5.5,"f":6.5,"g":7.5,"h":8.5,"i":9.5,"j":18446744073709551616}"#,
    );
    let one = exact(r#"{"j":18446744073709551616}"#);
    assert!(ten - one < 1_000.0, "{ten} allocations against {one}");
}
