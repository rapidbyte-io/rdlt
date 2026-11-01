use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use super::super::{Held, State};
use super::{
    GROUPS, Lingering, OWNED, REMAINING, StopsSpawned, enter, join, leave, remember, reserve, room,
};
use crate::local::sandbox::Stops;

#[test]
fn a_process_owns_up_to_its_groups_and_no_more() {
    assert!(room(0).is_ok());
    assert!(room(OWNED - 1).is_ok());
    assert!(room(OWNED).is_err());
    assert!(room(usize::MAX).is_err());
}

#[test]
fn the_latest_groups_that_kept_a_member_are_remembered_and_no_more() {
    let mut remaining = Vec::new();
    let most = u32::try_from(REMAINING).expect("it fits");
    for id in 0..most {
        remember(&mut remaining, id);
    }
    assert_eq!(remaining.len(), REMAINING);
    remember(&mut remaining, most);
    assert_eq!(remaining.len(), REMAINING);
    assert_eq!((remaining[0], remaining[REMAINING - 1]), (1, most));
}

/// A group's handle with no leader: stopping it does nothing.
fn idle() -> Arc<Held> {
    let state = State {
        leader: None,
        stdin: None,
        stops: Stops::BySignal,
        grace: Duration::ZERO,
        killing: None,
    };
    Arc::new(Held {
        state: Mutex::new(state),
    })
}

#[test]
fn a_process_owning_as_many_groups_as_it_may_reserves_no_more() {
    let most = u32::try_from(OWNED).expect("it fits");
    let live = (1..=most).map(|id| (id, idle()));
    GROUPS.lock().get_or_insert_default().live.extend(live);
    assert!(reserve().is_err());
    GROUPS.lock().get_or_insert_default().live.remove(&1);
    assert!(reserve().is_ok());
    GROUPS.lock().get_or_insert_default().live.clear();
}

/// The live groups and the places held by starts, read under one lock.
fn counted() -> (usize, usize) {
    let mut guard = GROUPS.lock();
    let groups = guard.get_or_insert_default();
    (groups.live.len(), groups.reserved)
}

#[test]
fn a_start_holds_its_place_until_its_group_is_entered_or_the_place_is_given_back() {
    let most = u32::try_from(OWNED).expect("it fits");
    let live = (1..most).map(|id| (id, idle()));
    GROUPS.lock().get_or_insert_default().live.extend(live);
    let first = reserve().expect("one place is left");
    // While the first start launches, the place it holds is no other start's.
    assert!(reserve().is_err());
    drop(first);
    let second = reserve().expect("the first start gave its place back");
    enter(second, most, idle(), std::thread::spawn(|| {}));
    assert_eq!(counted(), (OWNED, 0));
    assert!(reserve().is_err());
    GROUPS.lock().get_or_insert_default().live.clear();
    join(GROUPS.lock());
}

#[test]
fn of_starts_racing_for_the_last_place_one_alone_takes_it() {
    let most = u32::try_from(OWNED).expect("it fits");
    let live = (1..most).map(|id| (id, idle()));
    GROUPS.lock().get_or_insert_default().live.extend(live);
    let (tell, told) = std::sync::mpsc::channel();
    let starts: Vec<_> = (0..8)
        .map(|_| {
            let tell = tell.clone();
            std::thread::spawn(move || tell.send(reserve()).expect("the test listens"))
        })
        .collect();
    // Every place taken is kept until all are counted, so none is given back to another.
    let places: Vec<_> = (0..8)
        .map(|_| {
            told.recv_timeout(Duration::from_secs(10))
                .expect("each start answers")
        })
        .collect();
    for start in starts {
        start.join().expect("it ends");
    }
    assert_eq!(places.iter().filter(|place| place.is_ok()).count(), 1);
    drop(places);
    assert_eq!(counted(), (OWNED - 1, 0));
    GROUPS.lock().get_or_insert_default().live.clear();
}

#[test]
fn a_start_that_panics_holding_its_place_gives_it_back() {
    let panicked = std::thread::spawn(|| {
        let _place = reserve().expect("a place is free");
        panic!("the launch panics");
    })
    .join();
    assert!(panicked.is_err());
    assert_eq!(counted(), (0, 0));
    assert!(reserve().is_ok());
}

#[test]
fn a_group_that_kept_a_member_is_reported_by_the_next_stop_alone() {
    leave(7, false);
    let lingering = Lingering { groups: vec![7] };
    assert_eq!(StopsSpawned::within(Duration::ZERO).stop(), Err(lingering));
    assert_eq!(StopsSpawned::within(Duration::ZERO).stop(), Ok(()));
}
