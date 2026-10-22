use std::collections::{BTreeMap, BTreeSet};

use super::Plans;

fn named(partitions: &[usize]) -> BTreeSet<usize> {
    partitions.iter().copied().collect()
}

#[test]
fn a_plan_naming_other_partitions_of_a_stream_than_its_last_is_a_replan() {
    let plans = Plans::default();
    plans.answered(0, named(&[0, 1]));
    plans.answered(0, named(&[0, 1]));
    plans.answered(1, named(&[0]));
    assert!(!plans.findings().replanned);
    plans.answered(0, named(&[1, 2]));
    assert!(plans.findings().replanned);
}

#[test]
fn reads_of_a_partition_one_after_another_are_not_doubled_and_two_at_once_are() {
    let plans = Plans::default();
    drop(plans.read(0, 0));
    let first = plans.read(0, 0);
    let other = plans.read(0, 1);
    assert!(plans.findings().doubled.is_empty());
    let second = plans.read(0, 0);
    assert_eq!(plans.findings().doubled, BTreeSet::from([(0, 0)]));
    drop((first, other, second));
}

#[test]
fn a_read_is_late_by_what_it_sent_once_a_plan_since_it_began_dropped_its_partition() {
    let plans = Plans::default();
    plans.answered(0, named(&[0, 1]));
    let early = plans.read(0, 0);
    let kept = plans.read(0, 1);
    early.pushed();
    plans.answered(0, named(&[1]));
    early.pushed();
    early.took(4);
    kept.pushed();
    drop(early);
    // A read begun once the plan had dropped its partition answers to a later plan alone.
    let later = plans.read(0, 0);
    later.pushed();
    drop(later);
    plans.answered(0, named(&[0, 1]));
    let again = plans.read(0, 0);
    plans.answered(0, named(&[1]));
    again.pushed();
    assert_eq!(plans.findings().late, BTreeMap::from([((0, 0), 2)]));
    drop((kept, again));
}

#[test]
fn a_read_begun_before_its_partition_was_dropped_stays_late_once_it_is_named_again() {
    let plans = Plans::default();
    plans.answered(0, named(&[0]));
    let stopped = plans.read(0, 0);
    plans.answered(0, named(&[]));
    stopped.pushed();
    plans.answered(0, named(&[0]));
    stopped.pushed();
    let resumed = plans.read(0, 0);
    resumed.pushed();
    assert_eq!(plans.findings().late, BTreeMap::from([((0, 0), 2)]));
    drop((stopped, resumed));
}

#[test]
fn drained_names_the_furthest_checkpoint_taken_of_each_partition_a_plan_dropped() {
    let plans = Plans::default();
    plans.answered(0, named(&[0, 1]));
    let dropped = plans.read(0, 0);
    let kept = plans.read(0, 1);
    dropped.took(5);
    dropped.took(3);
    kept.took(4);
    plans.answered(0, named(&[1]));
    assert_eq!(plans.drained(), vec![((0, 0), 5)]);
    drop((dropped, kept));
}

#[test]
fn a_partition_named_again_drains_what_reads_begun_before_its_last_drop_took() {
    let plans = Plans::default();
    plans.answered(0, named(&[0, 1]));
    let first = plans.read(0, 0);
    let other = plans.read(0, 1);
    first.took(3);
    other.took(2);
    plans.answered(0, named(&[]));
    drop((first, other));
    plans.answered(0, named(&[0, 1]));
    let resumed = plans.read(0, 0);
    let again = plans.read(0, 1);
    resumed.took(8);
    again.took(6);
    plans.answered(0, named(&[0]));
    // Partition 0 is named once more: what its read since took is the run's to commit, not the
    // drop's.
    assert_eq!(plans.drained(), vec![((0, 0), 3), ((0, 1), 6)]);
    drop((resumed, again));
}
