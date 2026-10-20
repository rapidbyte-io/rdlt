use rdlt_sim::{Reach, Seed, Weight, check_changes, for_each_seed, seeds};

#[test]
fn every_change_lands_as_the_model_says_through_faults_crashes_and_concurrent_runs() {
    for_each_seed("changes", seeds(20), Weight::One, check_changes);
}

/// A seed whose round ran again only to replay a log the run before it left: without replay, the
/// log stays and the round never converges.
#[test]
fn a_seed_whose_transition_only_the_log_held_passes() {
    assert!(check_changes(Seed::new(9)).reached.has(Reach::Replayed));
}
