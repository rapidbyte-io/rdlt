use rdlt_sim::{check_exactly_once, seeds};

#[test]
fn every_row_lands_exactly_once_through_faults_crashes_and_concurrent_runs() {
    for seed in seeds(200) {
        check_exactly_once(seed);
    }
}
