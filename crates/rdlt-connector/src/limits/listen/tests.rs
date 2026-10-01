use std::time::Duration;

use super::{ListenLimits, TooFewDescriptors, UnfairSessions};

#[test]
fn the_defaults_keep_unauthenticated_peers_far_below_the_sessions_they_could_starve() {
    let limits = ListenLimits::default();
    assert_eq!(limits.unauthenticated, 64);
    assert_eq!(limits.handshake, Duration::from_secs(5));
    assert_eq!(limits.sessions, 256);
    assert_eq!(limits.host_sessions, 256);
    assert_eq!(limits.waiting, 64);
    assert_eq!(limits.wait, Duration::from_secs(10));
    assert_eq!(limits.session_descriptors, 4);
    assert_eq!(limits.own_descriptors, 64);
    assert_eq!(limits.report_every, Duration::from_secs(10));
    assert_eq!(limits.descriptors(), 64 + 1 + 64 + 256 * 4 + 64);
}

#[test]
fn every_stage_counts_towards_the_descriptors_needed() {
    let limits = ListenLimits {
        unauthenticated: 2,
        sessions: 5,
        waiting: 7,
        session_descriptors: 11,
        own_descriptors: 13,
        ..ListenLimits::default()
    };
    assert_eq!(limits.descriptors(), 2 + 1 + 7 + 5 * 11 + 13);
    let huge = ListenLimits {
        sessions: usize::MAX,
        session_descriptors: usize::MAX,
        ..limits
    };
    assert_eq!(huge.descriptors(), u64::MAX);
}

#[test]
fn a_process_that_may_open_too_few_descriptors_is_refused_with_both_counts() {
    let limits = ListenLimits {
        unauthenticated: 2,
        sessions: 5,
        waiting: 7,
        session_descriptors: 11,
        own_descriptors: 13,
        ..ListenLimits::default()
    };
    assert_eq!(limits.admit_descriptors(78), Ok(()));
    assert_eq!(limits.admit_descriptors(u64::MAX), Ok(()));
    assert_eq!(
        limits.admit_descriptors(77),
        Err(TooFewDescriptors {
            limit: 77,
            needed: 78
        })
    );
}

#[test]
fn named_hosts_share_the_sessions_so_that_none_is_left_without_one() {
    let limits = ListenLimits::default();
    let shared = |hosts, sessions, host_sessions| {
        let shared = limits.shared(hosts, sessions, host_sessions);
        shared.map(|limits| (limits.sessions, limits.host_sessions))
    };
    // An equal share where none is given: the hosts but one, full, leave a session.
    assert_eq!(shared(1, None, None), Ok((256, 256)));
    assert_eq!(shared(0, None, None), Ok((256, 256)));
    assert_eq!(shared(2, None, None), Ok((256, 128)));
    assert_eq!(shared(3, None, None), Ok((256, 85)));
    assert_eq!(shared(2, Some(8), None), Ok((8, 4)));
    assert_eq!(shared(8, Some(8), None), Ok((8, 1)));
    // A share given is kept where it leaves every host a session.
    assert_eq!(shared(2, Some(8), Some(7)), Ok((8, 7)));
    assert_eq!(shared(1, Some(8), Some(8)), Ok((8, 8)));
    assert_eq!(shared(3, Some(9), Some(4)), Ok((9, 4)));
    let unfair = |hosts, sessions, host_sessions| {
        Err(UnfairSessions {
            hosts,
            sessions,
            host_sessions,
        })
    };
    assert_eq!(shared(2, Some(8), Some(8)), unfair(2, 8, 8));
    assert_eq!(shared(3, Some(8), Some(4)), unfair(3, 8, 4));
    assert_eq!(shared(9, Some(8), None), unfair(9, 8, 0));
    assert_eq!(shared(1, Some(8), Some(0)), unfair(1, 8, 0));
    assert_eq!(shared(2, Some(0), None), unfair(2, 0, 0));
    // Nothing else of the limits changes.
    let changed = limits.shared(2, Some(8), None).expect("a fair share");
    let restored = ListenLimits {
        sessions: 256,
        host_sessions: 256,
        ..changed
    };
    assert_eq!(restored, limits);
}
