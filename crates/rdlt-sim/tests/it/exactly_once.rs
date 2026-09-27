use rdlt_sim::{Seed, check_exactly_once, seeds};

/// Seeds that each found a defect when first run, kept so they stay green.
const FOUND: [u64; 4] = [
    // Over the network: a served writer that panicked ended its write as though it were done.
    19,
    // Over the network: a host whose handshake a partition cut short held its connection, and
    // its connector's stop, forever.
    9_576,
    // A pushed float written to a JSON variant column with more digits than it needs.
    21_118,
    // A batch of a normalized merge stream whose every row a new array dropped: its key's type
    // was never resolved, so it met no refusal.
    163_723,
];

#[test]
fn every_row_lands_exactly_once_through_faults_crashes_and_concurrent_runs() {
    for seed in seeds(200) {
        let _ = check_exactly_once(seed);
    }
}

#[test]
fn seeds_that_once_found_a_defect_pass() {
    for seed in FOUND {
        let _ = check_exactly_once(Seed::new(seed));
    }
}
