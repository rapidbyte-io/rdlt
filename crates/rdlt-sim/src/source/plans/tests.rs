use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use tokio::time::{Instant, advance};

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
fn a_plan_that_stops_naming_a_partition_being_read_and_names_another_swaps() {
    // Dropping one being read and naming another swaps; dropping one with no read running, or
    // dropping one being read and naming none, does not.
    for (reading, after, swapped) in [
        (Some(0), [1, 2], true),
        (None, [1, 2], false),
        (Some(0), [1, 1], false),
    ] {
        let plans = Plans::default();
        plans.answered(0, named(&[0, 1]));
        let read = reading.map(|partition| plans.read(0, partition));
        plans.answered(0, named(&after));
        assert_eq!(
            plans.findings().swapped,
            swapped,
            "{reading:?} read, then {after:?}"
        );
        drop(read);
    }
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

#[tokio::test(start_paused = true)]
async fn a_partition_waits_for_a_read_from_when_it_is_named_or_its_last_read_ended() {
    let ms = Duration::from_millis;
    let plans = Plans::default();
    plans.answered(0, named(&[0, 1]));
    let first = plans.read(0, 0);
    advance(ms(100)).await;
    let other = plans.read(0, 1);
    advance(ms(50)).await;
    drop(first);
    advance(ms(300)).await;
    // Partition 0 waits from its read's end; partition 1, read after 100 ms, waits no more.
    assert_eq!(
        plans.waits(Instant::now(), &BTreeMap::new()),
        BTreeMap::from([((0, 0), ms(300)), ((0, 1), ms(100))])
    );
    let again = plans.read(0, 0);
    advance(ms(500)).await;
    assert_eq!(
        plans.waits(Instant::now(), &BTreeMap::new())[&(0, 0)],
        ms(300)
    );
    drop((again, other));
}

#[tokio::test(start_paused = true)]
async fn a_partition_no_plan_names_waits_for_no_read() {
    let ms = Duration::from_millis;
    let plans = Plans::default();
    plans.answered(0, named(&[0]));
    drop(plans.read(0, 0));
    advance(ms(100)).await;
    plans.answered(0, named(&[]));
    advance(ms(900)).await;
    assert_eq!(
        plans.waits(Instant::now(), &BTreeMap::new()),
        BTreeMap::from([((0, 0), Duration::ZERO)])
    );
    // Named again, it waits from then.
    plans.answered(0, named(&[0]));
    advance(ms(200)).await;
    assert_eq!(
        plans.waits(Instant::now(), &BTreeMap::new())[&(0, 0)],
        ms(200)
    );
}

#[tokio::test(start_paused = true)]
async fn a_partition_the_source_named_since_its_stream_s_last_plan_waits_from_then() {
    let ms = Duration::from_millis;
    let plans = Plans::default();
    plans.answered(0, named(&[0]));
    let read = plans.read(0, 0);
    let since = Instant::now();
    advance(ms(400)).await;
    // No plan here named p1 of stream 0, nor planned stream 1.
    let source = BTreeMap::from([((0, 0), since), ((0, 1), since), ((1, 0), since)]);
    assert_eq!(
        plans.waits(Instant::now(), &source),
        BTreeMap::from([((0, 0), Duration::ZERO), ((0, 1), ms(400))])
    );
    // Once a plan names it, it waits from the plan's answer.
    plans.answered(0, named(&[0, 1]));
    advance(ms(100)).await;
    assert_eq!(plans.waits(Instant::now(), &source)[&(0, 1)], ms(100));
    drop(read);
}

#[tokio::test(start_paused = true)]
async fn gone_names_each_partition_a_plan_dropped_since_then_until_one_names_it_again() {
    let plans = Plans::default();
    plans.answered(0, named(&[0, 1]));
    let dropped = plans.read(0, 0);
    let kept = plans.read(0, 1);
    dropped.took(5);
    kept.took(4);
    advance(Duration::from_millis(100)).await;
    let dropping = Instant::now();
    plans.answered(0, named(&[1]));
    drop((dropped, kept));
    assert_eq!(plans.gone(), vec![((0, 0), dropping, 5)]);
    plans.answered(0, named(&[0, 1]));
    assert_eq!(plans.gone(), Vec::new());
}
