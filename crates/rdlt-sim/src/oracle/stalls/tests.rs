use std::collections::BTreeMap;
use std::time::Duration;

use rdlt_engine::{CommitPolicy, EngineConfig};
use tokio::time::Instant;

use super::{bound, undrained, unread};

#[test]
fn a_partition_of_a_followed_stream_that_waited_past_the_bound_for_a_read_is_unread() {
    let ms = Duration::from_millis;
    let waits = BTreeMap::from([
        ((0, 0), ms(400)),
        ((0, 1), ms(401)),
        ((1, 0), ms(900)),
        ((2, 0), ms(0)),
    ]);
    let follows = |stream: usize| stream != 1;
    assert_eq!(unread(&waits, follows, ms(400)), vec![((0, 1), ms(401))]);
}

#[test]
fn a_dropped_partition_past_the_bound_held_short_of_its_taken_checkpoint_is_undrained() {
    let ms = Duration::from_millis;
    let now = Instant::now() + ms(1000);
    let long_ago = now - ms(600);
    let gone = [
        ((0, 0), long_ago, 5),
        ((0, 1), long_ago, 5),
        ((0, 2), now - ms(100), 5),
        ((0, 3), long_ago, 2),
        ((0, 4), now - ms(500), 5),
        ((1, 1), long_ago, 5),
    ];
    let follows = |stream: usize| stream == 0;
    let held = |_: usize, partition: usize| match partition {
        0 => Some(5),
        1 => Some(4),
        _ => None,
    };
    assert_eq!(
        undrained(&gone, follows, (now, ms(500)), held),
        vec![((0, 1), 5, Some(4)), ((0, 3), 2, None)]
    );
}

#[test]
fn a_following_run_waits_three_plans_its_interval_and_two_barriers_at_most() {
    let ms = Duration::from_millis;
    let configured = |every: Option<Duration>| {
        let policy = CommitPolicy::new(every, Some(10), None).unwrap();
        let builder = EngineConfig::builder().replan(ms(250)).barrier_wait(ms(10));
        builder.commit(policy).build().unwrap()
    };
    assert_eq!(bound(&configured(None)), ms(770));
    assert_eq!(bound(&configured(Some(ms(1000)))), ms(1770));
}
