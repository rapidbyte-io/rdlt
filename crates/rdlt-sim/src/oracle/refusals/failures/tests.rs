use std::collections::BTreeSet;

use rdlt_connector::wire::INVALID_MESSAGE;
use rdlt_engine::{Code, ErrorKind, Origin, codes};
use rdlt_host::CONNECTOR_LOST;

use super::{Failure, unexplained, violation};
use crate::oracle::refusals::Prediction;

/// A failure of `stream` the engine found, of `kind` and coded `code`.
fn engine(kind: ErrorKind, code: Option<Code>, stream: &str) -> Failure {
    Failure {
        kind,
        origin: Origin::Engine,
        code,
        connector_code: None,
        stream: Some(stream.to_owned()),
        text: format!("{kind:?} {code:?} in {stream}"),
    }
}

/// A failure of `stream` a connector on `origin`'s side, or its host, raised, of `kind` and
/// carrying `code`.
fn connector(kind: ErrorKind, origin: Origin, code: Option<&str>, stream: &str) -> Failure {
    Failure {
        kind,
        origin,
        code: None,
        connector_code: code.map(ToOwned::to_owned),
        stream: Some(stream.to_owned()),
        text: format!("{kind:?} {origin:?} {code:?} in {stream}"),
    }
}

#[test]
fn a_run_without_faults_may_meet_only_the_refusals_the_model_predicts() {
    let prediction = Prediction {
        may: BTreeSet::from([("s0".to_owned(), codes::SCHEMA_FROZEN)]),
        must: BTreeSet::new(),
    };
    let refusal = engine(ErrorKind::Schema, Some(codes::SCHEMA_FROZEN), "s0");
    let internal = engine(ErrorKind::Internal, None, "s1");
    assert!(unexplained(std::slice::from_ref(&refusal), &prediction).is_none());
    // Another pipeline's failure beside a predicted refusal is still a finding.
    let failures = [refusal, internal];
    let found = unexplained(&failures, &prediction).map(|failure| failure.text.as_str());
    assert_eq!(found, Some(failures[1].text.as_str()));
}

#[test]
fn a_connector_naming_the_engine_s_refusal_is_no_refusal() {
    let prediction = Prediction {
        may: BTreeSet::from([("s0".to_owned(), codes::SCHEMA_FROZEN)]),
        must: BTreeSet::new(),
    };
    let forged = connector(
        ErrorKind::Schema,
        Origin::Destination,
        Some(codes::SCHEMA_FROZEN.as_str()),
        "s0",
    );
    let failures = [forged];
    let found = unexplained(&failures, &prediction).map(|failure| failure.text.as_str());
    assert_eq!(found, Some(failures[0].text.as_str()));
}

#[test]
fn a_run_that_breaks_the_wire_protocol_is_a_finding_whatever_the_faults() {
    let lost = connector(
        ErrorKind::Source,
        Origin::Source,
        Some(CONNECTOR_LOST),
        "s0",
    );
    let injected = connector(ErrorKind::Destination, Origin::Destination, None, "s0");
    assert!(violation(&[lost.clone(), injected.clone()]).is_none());
    let broken = connector(
        ErrorKind::Destination,
        Origin::Destination,
        Some(INVALID_MESSAGE),
        "s1",
    );
    let failures = [lost, broken, injected];
    let found = violation(&failures).map(|failure| failure.text.as_str());
    assert_eq!(found, Some(failures[1].text.as_str()));
}
