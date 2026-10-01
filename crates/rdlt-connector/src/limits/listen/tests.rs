use std::time::Duration;

use super::{ListenLimits, TooFewDescriptors};

#[test]
fn the_defaults_keep_unauthenticated_peers_far_below_the_sessions_they_could_starve() {
    let limits = ListenLimits::default();
    assert_eq!(limits.unauthenticated, 64);
    assert_eq!(limits.handshake, Duration::from_secs(5));
    assert_eq!(limits.sessions, 256);
    assert_eq!(limits.host_sessions, 64);
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
