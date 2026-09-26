use rdlt_sim::{Seed, check_exactly_once, seeds};

/// Seeds that each found a defect when first run, kept so they stay green.
const FOUND: [u64; 2] = [
    // A pushed float written to a JSON variant column with more digits than it needs.
    21_118,
    // A batch of a normalized merge stream whose every row a new array dropped: its key's type
    // was never resolved, so it met no refusal.
    163_723,
];

#[test]
fn every_row_lands_exactly_once_through_faults_crashes_and_concurrent_runs() {
    for seed in seeds(200) {
        check_exactly_once(seed);
    }
}

#[test]
fn seeds_that_once_found_a_defect_pass() {
    for seed in FOUND {
        check_exactly_once(Seed::new(seed));
    }
}
