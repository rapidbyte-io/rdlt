use rdlt_sim::{Seed, check_changes, seeds};

#[test]
fn every_change_lands_as_the_model_says_through_faults_crashes_and_concurrent_runs() {
    for seed in seeds(200) {
        let _ = check_changes(seed);
    }
}

#[test]
fn a_change_simulation_replays_exactly_from_its_seed() {
    let seed = Seed::new(7);
    assert_eq!(check_changes(seed), check_changes(seed));
}

/// A seed whose source sends changes again across a hard delete, which without tombstones would
/// bring its row back.
#[test]
fn a_seed_that_once_found_a_defect_passes() {
    let _ = check_changes(Seed::new(22));
}
