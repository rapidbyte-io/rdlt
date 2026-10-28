//! Proofs that a destination applies a commit only where its fence row refuses it for none of
//! the three reasons it can, and answers each refusal by its reason.

use std::time::UNIX_EPOCH;

use super::{Answer, Fence, answer};
use crate::commit::{CommitKey, Receipt};
use crate::id::Epoch;

/// Any key, of any epoch and place.
fn any_key() -> CommitKey {
    CommitKey {
        epoch: Epoch(kani::any()),
        seq: kani::any(),
    }
}

/// Any key, or none.
fn any_key_or_none() -> Option<CommitKey> {
    kani::any::<bool>().then(any_key)
}

/// Any fence row: any epoch, and the receipt of any landed key, or none.
fn any_fence() -> Fence {
    Fence {
        epoch: Epoch(kani::any()),
        landed: any_key_or_none().map(|key| Receipt {
            key,
            committed_at: UNIX_EPOCH,
            rows: kani::any(),
            bytes: kani::any(),
        }),
    }
}

/// A commit applies only where its key is not the landed key, its epoch is the fence row's, and
/// the key it expects landed is the landed key: neither a landed key, a stale `after` nor a
/// fenced epoch ever applies.
///
/// Bound: every epoch and place of the fence row's, the commit's and the expected key.
#[kani::proof]
fn neither_a_landed_key_a_stale_after_nor_a_fenced_epoch_applies() {
    let fence = any_fence();
    let landed = fence.landed.as_ref().map(|receipt| receipt.key);
    let (key, after) = (any_key(), any_key_or_none());
    if answer(&fence, key, after) == Answer::Apply {
        assert!(landed != Some(key));
        assert!(fence.epoch == key.epoch);
        assert!(landed == after);
    }
}

/// A landed key is answered with the receipt the fence row holds, from whichever session sends
/// it; a refusal for another epoch is `Fenced`, and one for another key landed `Stale`.
///
/// Bound: every epoch and place of the fence row's, the commit's and the expected key.
#[kani::proof]
fn a_landed_key_is_answered_with_its_receipt_and_each_refusal_by_its_reason() {
    let fence = any_fence();
    let landed = fence.landed.as_ref().map(|receipt| receipt.key);
    let (key, after) = (any_key(), any_key_or_none());
    match answer(&fence, key, after) {
        Answer::Landed(receipt) => assert!(receipt.key == key && landed == Some(key)),
        Answer::Fenced => assert!(landed != Some(key) && fence.epoch != key.epoch),
        Answer::Stale => {
            assert!(landed != Some(key) && fence.epoch == key.epoch && landed != after);
        }
        Answer::Apply => {}
    }
}
