use rdlt_sim::{Seed, check_exactly_once, seeds};

/// Seeds that each found a defect when first run, kept so they stay green.
const FOUND: [u64; 7] = [
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
    // Over the network: a dropped connection's HTTP/2 task outlived it until its pings timed out
    // (rdlt-host's liveness tests guard it).
    157_941,
    // Over the network: a commit held until the network healed landed after the runs were judged
    // (the network's tests guard it).
    386_903,
    // Logged: a failed load's commit of a new full read, replayed in the next phase, landed rows
    // the model had not counted, until a phase waited for every log to be replayed.
    8_746,
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

/// A seed whose run acknowledged rows its destination then failed to commit, so only the
/// write-ahead log held them: with replay disabled, the source never serves them again and every
/// later run fails.
#[test]
fn a_seed_whose_rows_only_the_log_held_passes() {
    let _ = check_exactly_once(Seed::new(61));
}
