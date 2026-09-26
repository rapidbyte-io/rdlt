use std::collections::BTreeSet;

use rdlt_engine::ErrorKind;

use super::{Failure, unexplained};
use crate::oracle::refusals::{FROZEN, Prediction};

fn failure(kind: ErrorKind, code: Option<&str>, stream: &str) -> Failure {
    Failure {
        kind,
        code: code.map(ToOwned::to_owned),
        stream: Some(stream.to_owned()),
        text: format!("{kind:?} {code:?} in {stream}"),
    }
}

#[test]
fn a_run_without_faults_may_meet_only_the_refusals_the_model_predicts() {
    let prediction = Prediction {
        may: BTreeSet::from([("s0".to_owned(), FROZEN)]),
        must: BTreeSet::new(),
    };
    let refusal = failure(ErrorKind::Schema, Some(FROZEN), "s0");
    let internal = failure(ErrorKind::Internal, None, "s1");
    assert!(unexplained(std::slice::from_ref(&refusal), &prediction).is_none());
    // Another pipeline's failure beside a predicted refusal is still a finding.
    let failures = [refusal, internal];
    let found = unexplained(&failures, &prediction).map(|failure| failure.text.as_str());
    assert_eq!(found, Some(failures[1].text.as_str()));
}
