use std::time::{Duration, UNIX_EPOCH};

use pyo3::exceptions::PyValueError;
use rdlt_connector::{CommitKey, Epoch, Receipt};

use super::answer;
use crate::embedded;

fn key(epoch: u64, seq: u64) -> String {
    let key = CommitKey {
        epoch: Epoch(epoch),
        seq,
    };
    serde_json::to_string(&key).unwrap()
}

fn receipt(epoch: u64, seq: u64) -> String {
    let receipt = Receipt {
        key: CommitKey {
            epoch: Epoch(epoch),
            seq,
        },
        committed_at: UNIX_EPOCH + Duration::from_secs(5),
        rows: 3,
        bytes: 24,
    };
    serde_json::to_string(&receipt).unwrap()
}

#[test]
fn a_commit_is_answered_as_the_rust_sdk_answers_it_by_a_fence_at_epoch_2() {
    for (landed, keyed, after, answered) in [
        (None, key(2, 1), None, "apply"),
        (Some(receipt(2, 1)), key(2, 1), None, "landed"),
        (Some(receipt(1, 4)), key(1, 4), None, "landed"),
        (None, key(1, 1), None, "fenced"),
        (Some(receipt(2, 1)), key(2, 3), Some(key(2, 2)), "stale"),
        (None, key(2, 2), Some(key(2, 1)), "stale"),
        (Some(receipt(2, 1)), key(2, 2), Some(key(2, 1)), "apply"),
    ] {
        let decided = answer(2, landed.as_deref(), &keyed, after.as_deref()).unwrap();
        assert_eq!(decided, answered, "{landed:?} {keyed} {after:?}");
    }
}

#[test]
fn a_key_or_a_receipt_rdlt_cannot_read_is_refused_rather_than_compared() {
    let next = key(2, 2);
    embedded::with(|py| {
        for (landed, keyed, after) in [
            (None, r#"{"epoch": "2", "seq": 1}"#, None),
            (None, r#"{"epoch": 2}"#, None),
            (None, next.as_str(), Some(r#"{"epoch": 2, "seq": -1}"#)),
            (
                Some(r#"{"key": {"epoch": 2, "seq": 1}}"#),
                next.as_str(),
                None,
            ),
        ] {
            let refused = answer(2, landed, keyed, after).expect_err("refused");
            assert!(
                refused.is_instance_of::<PyValueError>(py),
                "{landed:?} {keyed} {after:?}"
            );
        }
    });
}
