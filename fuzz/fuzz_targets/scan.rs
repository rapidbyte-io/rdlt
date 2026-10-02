//! Whatever a protocol message decodes from, the scan takes, and counts no less than decoding it
//! holds at its peak, for every message the protocol's calls carry.

#![forbid(unsafe_code)]
#![no_main]

use libfuzzer_sys::fuzz_target;
use rdlt_wire::scan::differential;

#[global_allocator]
static HEAP: peak_alloc::PeakAlloc = peak_alloc::PeakAlloc;

/// What `call`, run once, held on the heap at its peak.
fn peak_of(call: &mut dyn FnMut()) -> usize {
    HEAP.reset_peak_usage();
    let before = HEAP.current_usage();
    call();
    HEAP.peak_usage().saturating_sub(before)
}

fuzz_target!(|input: (u8, Vec<u8>)| {
    let (which, bytes) = input;
    let Some(decoding) = differential::decoding(usize::from(which), &bytes, &peak_of) else {
        return;
    };
    if !decoding.decoded {
        return;
    }
    let name = decoding.form.name;
    let counted = decoding.counted.unwrap_or_else(|unscanned| {
        panic!("{name} decoded from {bytes:02x?}, which the scan refused: {unscanned}")
    });
    assert!(
        decoding.held <= counted,
        "{name} held {} decoded from {bytes:02x?}, the scan counted {counted}",
        decoding.held
    );
});
