use super::{Limit, beyond, found, unexceeded};
use crate::protocol::Found;

fn unexceeded_limit() -> Limit {
    unexceeded("cursor", 9000, 100)
}

fn unsent() -> Limit {
    Limit::Unsent("no partition".to_owned())
}

#[test]
fn the_clause_is_kept_when_a_limit_was_and_says_why_none_could_be_checked() {
    let kept = [
        (Limit::Kept, Limit::Kept),
        (Limit::Kept, unexceeded_limit()),
        (unexceeded_limit(), Limit::Kept),
        (Limit::Kept, unsent()),
        (unsent(), Limit::Kept),
    ];
    for (first, second) in kept {
        assert!(matches!(found(first, second), Found::Kept));
    }
    // What could not be sent was not observed, whatever the other limit declares.
    for (first, second) in [
        (unsent(), unexceeded_limit()),
        (unexceeded_limit(), unsent()),
        (unsent(), unsent()),
    ] {
        let Found::Unobserved(reason) = found(first, second) else {
            panic!("a limit nothing was sent beyond was observed");
        };
        assert_eq!(reason, "no partition");
    }
    // Limits this host cannot exceed do not apply to it: the reason carries both.
    let Found::Inapplicable(reason) =
        found(unexceeded("configuration", 7000, 50), unexceeded_limit())
    else {
        panic!("limits no host exceeds were checked");
    };
    for number in ["7000", "50", "9000", "100"] {
        assert!(reason.contains(number), "{reason}");
    }
}

#[test]
fn a_limit_is_exceeded_by_a_byte_only_where_this_host_sends_as_much() {
    assert_eq!(beyond(0, 100), None);
    assert_eq!(beyond(1, 100), Some(2));
    assert_eq!(beyond(100, 100), Some(101));
    assert_eq!(beyond(101, 100), None);
    assert_eq!(beyond(u64::MAX, u64::MAX), None);
    assert_eq!(unexceeded_limit(), Limit::Unexceeded(unexceeded_text()));
}

fn unexceeded_text() -> String {
    match unexceeded_limit() {
        Limit::Unexceeded(reason) => reason,
        other => panic!("{other:?}"),
    }
}
