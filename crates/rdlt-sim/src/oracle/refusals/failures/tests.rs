use std::collections::BTreeSet;

use rdlt_engine::ErrorKind;

use super::{Failure, unexplained, violation};
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

#[test]
fn a_run_that_breaks_the_wire_protocol_is_a_finding_whatever_the_faults() {
    let lost = failure(ErrorKind::Source, Some("connector_lost"), "s0");
    let injected = failure(ErrorKind::Destination, None, "s0");
    assert!(violation(&[lost.clone(), injected.clone()]).is_none());
    let broken = failure(ErrorKind::Destination, Some("invalid_message"), "s1");
    let failures = [lost, broken, injected];
    let found = violation(&failures).map(|failure| failure.text.as_str());
    assert_eq!(found, Some(failures[1].text.as_str()));
}
