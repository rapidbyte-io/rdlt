use nix::sys::resource::{Resource, getrlimit};

use super::reserve;
use crate::limits::{ListenLimits, TooFewDescriptors};

/// Limits that need `descriptors` file descriptors.
fn needing(descriptors: u64) -> ListenLimits {
    let limits = ListenLimits {
        unauthenticated: 0,
        waiting: 0,
        sessions: 1,
        session_descriptors: 0,
        own_descriptors: usize::try_from(descriptors - 1).expect("a small count"),
        ..ListenLimits::default()
    };
    assert_eq!(limits.descriptors(), descriptors);
    limits
}

fn limit() -> (u64, u64) {
    getrlimit(Resource::RLIMIT_NOFILE).expect("the limit reads")
}

#[test]
fn a_limit_that_suffices_is_left_as_it_is() {
    let (soft, hard) = limit();
    assert_eq!(reserve(&needing(soft)), Ok(()));
    assert_eq!(limit(), (soft, hard));
}

#[test]
fn a_soft_limit_is_raised_as_far_as_needed_where_the_hard_limit_allows() {
    let (soft, hard) = limit();
    if soft == hard {
        // Nothing to raise: more than the hard limit is refused, and nothing changes.
        let refused = reserve(&needing(hard + 1));
        let expected = TooFewDescriptors {
            limit: hard,
            needed: hard + 1,
        };
        assert_eq!(refused, Err(expected));
        assert_eq!(limit(), (soft, hard));
        return;
    }
    assert_eq!(reserve(&needing(soft + 1)), Ok(()));
    assert_eq!(limit(), (soft + 1, hard));
}

#[test]
fn more_than_the_hard_limit_is_refused_and_changes_nothing() {
    let (soft, hard) = limit();
    if hard == u64::MAX {
        return;
    }
    let refused = reserve(&needing(hard + 1));
    let expected = TooFewDescriptors {
        limit: hard,
        needed: hard + 1,
    };
    assert_eq!(refused, Err(expected));
    assert_eq!(limit(), (soft, hard));
}
