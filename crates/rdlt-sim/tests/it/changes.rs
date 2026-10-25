use rdlt_sim::{Weight, check_changes, for_each_seed, seeds};

#[test]
fn every_change_lands_as_the_model_says_through_faults_crashes_and_concurrent_runs() {
    for_each_seed("changes", seeds(20), Weight::One, check_changes);
}
