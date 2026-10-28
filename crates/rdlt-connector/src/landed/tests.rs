use std::time::{Duration, UNIX_EPOCH};

use proptest::prelude::*;

use super::{Answer, Fence, answer};
use crate::commit::{CommitKey, CommitMeta, Receipt, SegmentSet};
use crate::error::{COMMIT_STALE, ConnectorErrorKind};
use crate::id::Epoch;

fn key(epoch: u64, seq: u64) -> CommitKey {
    CommitKey {
        epoch: Epoch(epoch),
        seq,
    }
}

fn receipt(landed: CommitKey) -> Receipt {
    Receipt {
        key: landed,
        committed_at: UNIX_EPOCH + Duration::from_micros(7),
        rows: 3,
        bytes: 24,
    }
}

fn fence(epoch: u64, landed: Option<CommitKey>) -> Fence {
    Fence {
        epoch: Epoch(epoch),
        landed: landed.map(receipt),
    }
}

fn meta(committed: CommitKey, after: Option<CommitKey>) -> CommitMeta {
    CommitMeta {
        key: committed,
        after,
        segments: SegmentSet::new(),
        abandoned: SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
    }
}

/// What the answer says, without the receipt it carries.
#[derive(Debug, PartialEq, Eq)]
enum Said {
    Landed,
    Fenced,
    Stale,
    Apply,
}

fn said(answered: Answer<'_>) -> Said {
    match answered {
        Answer::Landed(_) => Said::Landed,
        Answer::Fenced => Said::Fenced,
        Answer::Stale => Said::Stale,
        Answer::Apply => Said::Apply,
    }
}

#[test]
fn a_commit_is_answered_by_its_key_then_its_epoch_then_what_it_expects_landed() {
    let cases = [
        // The landed key, from its own session and from a newer one.
        (
            fence(2, Some(key(2, 3))),
            key(2, 3),
            Some(key(2, 2)),
            Said::Landed,
        ),
        (
            fence(3, Some(key(2, 3))),
            key(2, 3),
            Some(key(2, 2)),
            Said::Landed,
        ),
        // Another epoch, whatever it expects.
        (
            fence(3, Some(key(2, 3))),
            key(2, 4),
            Some(key(2, 3)),
            Said::Fenced,
        ),
        (fence(3, None), key(2, 1), None, Said::Fenced),
        // Its epoch, expecting a key other than the landed key.
        (fence(2, Some(key(2, 3))), key(2, 1), None, Said::Stale),
        (
            fence(2, Some(key(2, 3))),
            key(2, 5),
            Some(key(2, 4)),
            Said::Stale,
        ),
        (fence(2, None), key(2, 2), Some(key(2, 1)), Said::Stale),
        // Its epoch, expecting the key landed.
        (
            fence(2, Some(key(2, 3))),
            key(2, 4),
            Some(key(2, 3)),
            Said::Apply,
        ),
        (
            fence(3, Some(key(2, 3))),
            key(3, 1),
            Some(key(2, 3)),
            Said::Apply,
        ),
        (fence(1, None), key(1, 1), None, Said::Apply),
    ];
    for (fence, committed, after, expected) in cases {
        assert_eq!(
            said(answer(&fence, committed, after)),
            expected,
            "{fence:?}, {committed:?} after {after:?}"
        );
    }
}

#[test]
fn a_landed_key_is_answered_with_the_receipt_the_fence_row_holds() {
    let fence = fence(4, Some(key(3, 2)));
    let Answer::Landed(stored) = answer(&fence, key(3, 2), None) else {
        panic!("the landed key is answered as landed");
    };
    assert_eq!(stored, &receipt(key(3, 2)));
}

#[test]
fn a_destination_decides_a_receipt_nothing_or_a_fenced_refusal() {
    let fence = fence(2, Some(key(2, 3)));
    assert_eq!(
        fence.decide(&meta(key(2, 3), Some(key(2, 2)))).unwrap(),
        Some(receipt(key(2, 3)))
    );
    assert_eq!(
        fence.decide(&meta(key(2, 4), Some(key(2, 3)))).unwrap(),
        None
    );
    let fenced = fence.decide(&meta(key(1, 4), Some(key(2, 3)))).unwrap_err();
    assert_eq!(
        (fenced.kind(), fenced.code()),
        (ConnectorErrorKind::Fenced, None)
    );
    let stale = fence.decide(&meta(key(2, 1), None)).unwrap_err();
    assert_eq!(
        (stale.kind(), stale.code()),
        (ConnectorErrorKind::Fenced, Some(COMMIT_STALE))
    );
}

#[test]
fn a_stale_refusal_names_the_key_expected_and_the_key_that_landed() {
    let landed = fence(2, Some(key(2, 3)));
    let stale = landed.decide(&meta(key(2, 1), None)).unwrap_err();
    assert_eq!(
        stale.to_string(),
        "commit 1 of epoch 2 expects no commit landed, and commit 3 of epoch 2 did"
    );
    let none = fence(2, None);
    let stale = none.decide(&meta(key(2, 4), Some(key(2, 3)))).unwrap_err();
    assert_eq!(
        stale.to_string(),
        "commit 4 of epoch 2 expects commit 3 of epoch 2 landed, and no commit did"
    );
}

#[test]
fn keys_order_by_epoch_then_place() {
    assert!(key(1, 9) < key(2, 1));
    assert!(key(2, 1) < key(2, 2));
}

fn small_key() -> impl Strategy<Value = CommitKey> {
    (0u64..3, 0u64..4).prop_map(|(epoch, seq)| key(epoch, seq))
}

proptest! {
    /// A commit applies exactly where its key is not the landed key, its epoch is the fence's
    /// and it expects the key landed; otherwise the first of those that fails is the answer.
    #[test]
    fn a_commit_applies_only_where_no_check_refuses_it(
        epoch in 0u64..3,
        landed in proptest::option::of(small_key()),
        committed in small_key(),
        after in proptest::option::of(small_key()),
    ) {
        let fence = fence(epoch, landed);
        let expected = if landed == Some(committed) {
            Said::Landed
        } else if Epoch(epoch) != committed.epoch {
            Said::Fenced
        } else if landed != after {
            Said::Stale
        } else {
            Said::Apply
        };
        prop_assert_eq!(said(answer(&fence, committed, after)), expected);
    }
}
