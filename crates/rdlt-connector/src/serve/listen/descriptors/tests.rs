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

/// One test: each step reads or changes the limit of the process it runs in, which tests run
/// side by side in one process would share.
#[test]
fn a_limit_is_left_where_it_suffices_raised_as_far_as_needed_and_refused_beyond_the_hard_one() {
    let (soft, hard) = limit();
    // What suffices is left as it is.
    assert_eq!(reserve(&needing(soft)), Ok(()));
    assert_eq!(limit(), (soft, hard));
    // More than the hard limit is refused, and changes nothing.
    if hard != u64::MAX {
        let expected = TooFewDescriptors {
            limit: hard,
            needed: hard + 1,
        };
        assert_eq!(reserve(&needing(hard + 1)), Err(expected));
        assert_eq!(limit(), (soft, hard));
    }
    // A soft limit below the hard one is raised as far as needed, and no further.
    if soft < hard {
        assert_eq!(reserve(&needing(soft + 1)), Ok(()));
        assert_eq!(limit(), (soft + 1, hard));
    }
}
