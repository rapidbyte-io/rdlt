use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::super::{Held, State};
use super::{Lingering, OWNED, REMAINING, StopsSpawned, groups, has_room, leave, remember, room};
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
fn a_process_owning_as_many_groups_as_it_may_has_room_for_no_more() {
    let most = u32::try_from(OWNED).expect("it fits");
    let live = (1..=most).map(|id| (id, idle()));
    groups().get_or_insert_default().live.extend(live);
    assert!(has_room().is_err());
    groups().get_or_insert_default().live.remove(&1);
    assert!(has_room().is_ok());
    groups().get_or_insert_default().live.clear();
}

#[test]
fn a_group_that_kept_a_member_is_reported_by_the_next_stop_alone() {
    leave(7, false);
    let lingering = Lingering { groups: vec![7] };
    assert_eq!(StopsSpawned::within(Duration::ZERO).stop(), Err(lingering));
    assert_eq!(StopsSpawned::within(Duration::ZERO).stop(), Ok(()));
}
