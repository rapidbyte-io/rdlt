use rdlt_sim::{Seed, Weight, check_changes, for_each_seed, seeds};

#[test]
fn every_change_lands_as_the_model_says_through_faults_crashes_and_concurrent_runs() {
    for_each_seed("changes", seeds(20), Weight::One, check_changes);
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

/// A seed whose source forgets what it acknowledged and whose transition to the changes only the
/// write-ahead log held: without the log's transition, the changes are read again from their
/// start, which the source no longer serves.
#[test]
fn a_seed_whose_transition_only_the_log_held_passes() {
    let _ = check_changes(Seed::new(130));
}

/// A seed whose log's mark and one of its chunks were each made again after an attempt given up,
/// which landed once the log was removed: the log opened again held a chunk naming chunks the
/// removal deleted, which no replay could read.
#[test]
fn a_seed_whose_removed_log_was_opened_again_by_a_late_mark_passes() {
    let _ = check_changes(Seed::new(133_238));
}

/// A seed whose replay listed a log before its load published a chunk, then fenced it at a
/// number the load had since freed: it called the log finished and removed it, with a commit
/// the source was told of, which no run could read again.
#[test]
fn a_seed_whose_replay_fenced_a_log_below_its_last_chunk_passes() {
    let _ = check_changes(Seed::new(446_159));
}
