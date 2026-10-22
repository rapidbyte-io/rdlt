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
    assert_eq!(plans.findings().late, BTreeMap::from([((0, 0), 2)]));
    drop((kept, later));
}

#[test]
fn drained_names_the_furthest_checkpoint_taken_of_each_partition_a_plan_dropped() {
    let plans = Plans::default();
    plans.answered(0, named(&[0, 1]));
    let dropped = plans.read(0, 0);
    let kept = plans.read(0, 1);
    dropped.took(3);
    dropped.took(5);
    kept.took(4);
    plans.answered(0, named(&[1]));
    assert_eq!(plans.drained(), vec![((0, 0), 5)]);
    drop((dropped, kept));
}
