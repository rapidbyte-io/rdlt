//! Connectors that break one rule of the protocol each: its clause fails, and the others hold.

pub(crate) mod fake;

use rdlt_certify::{
    Outcome, PROTOCOL_CLAUSES, Target, Unprobed, certify_destination, certify_source,
};

use rdlt_connector::serve::Served;
use rdlt_connector::source_factory;
use rdlt_connector_reference::MemorySource;
use rdlt_host::Options;
use rdlt_wire::Limits;

use fake::{BROKEN, Fault, served};

#[tokio::test(flavor = "multi_thread")]
async fn each_protocol_clause_fails_a_connector_that_breaks_it_and_only_that() {
    // Each certification waits out the credit clause's quiet second, so the faults run at once.
    let mut certifying = tokio::task::JoinSet::new();
    for (fault, broken) in BROKEN {
        certifying.spawn(async move {
            let target = Target::connected(move || Box::pin(async move { served(fault) }));
            (
                fault,
                broken,
                certify_source(&target, serde_json::json!({})).await,
            )
        });
    }
    while let Some(certified) = certifying.join_next().await {
        let (fault, broken, report) = certified.expect("the certification ends");
        for clause in PROTOCOL_CLAUSES {
            let outcome = report.outcome(clause.id);
            if clause.id == broken {
                assert!(
                    matches!(outcome, Some(Outcome::Failed(_))),
                    "{fault:?} breaks {broken}: {report}"
                );
            } else {
                assert!(
                    matches!(
                        outcome,
                        Some(Outcome::Passed | Outcome::Inapplicable(_) | Outcome::Unobserved(_))
                    ),
                    "{fault:?} breaks {broken} alone, not {}: {report}",
                    clause.id
                );
            }
        }
    }
}

#[tokio::test]
async fn limits_beyond_any_this_host_sends_are_not_exceeded() {
    let target =
        Target::connected(|| Box::pin(async { served(Fault::Vast) })).credit_watch(crate::BRIEF);
    let report = certify_source(&target, serde_json::json!({})).await;
    let Some(Outcome::Inapplicable(reason)) = report.outcome("P-LIMITS") else {
        panic!("P-LIMITS ran: {report}");
    };
    // The reason carries the limit the connector declares.
    assert!(reason.contains(&(u64::MAX - 1).to_string()), "{reason}");
}

#[tokio::test]
async fn limits_this_host_keeps_below_the_connectors_are_not_exceeded() {
    let options = Options {
        limits: Limits {
            config_bytes: 1024,
            cursor_bytes: 16,
            ..Limits::default()
        },
        ..Options::default()
    };
    let served = Served::new().with_source(source_factory::<MemorySource>());
    let target = Target::served(served)
        .options(options)
        .credit_watch(crate::BRIEF);
    let report = certify_source(&target, serde_json::json!({ "streams": { "items": [] } })).await;
    let Some(Outcome::Inapplicable(reason)) = report.outcome("P-LIMITS") else {
        panic!("P-LIMITS ran: {report}");
    };
    // The reason carries the limit this host keeps.
    assert!(reason.contains("1024"), "{reason}");
}

#[tokio::test]
async fn a_destination_that_takes_or_miscodes_bad_batches_fails_malformed_and_limits() {
    for fault in [Fault::LenientFrames, Fault::MiscodedFrames] {
        let target = Target::connected(move || Box::pin(async move { served(fault) }));
        let report = certify_destination(&target, serde_json::json!({}), &Unprobed).await;
        for id in ["P-MALFORMED", "P-LIMITS"] {
            assert!(
                matches!(report.outcome(id), Some(Outcome::Failed(_))),
                "{fault:?} breaks {id}: {report}"
            );
        }
    }
}

#[tokio::test]
async fn a_heartbeat_whose_answers_stay_open_after_the_pings_is_kept() {
    let target = Target::connected(|| Box::pin(async { served(Fault::Lingering) }))
        .credit_watch(crate::BRIEF);
    let report = certify_source(&target, serde_json::json!({})).await;
    assert_eq!(
        report.outcome("P-HEARTBEAT"),
        Some(&Outcome::Passed),
        "{report}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_connector_that_answers_no_heartbeat_is_certified_in_bounded_time() {
    let target = Target::connected(|| Box::pin(async { served(Fault::Mute) }));
    let started = tokio::time::Instant::now();
    let report = certify_source(&target, serde_json::json!({})).await;
    assert!(
        matches!(report.outcome("P-HEARTBEAT"), Some(Outcome::Failed(_))),
        "{report}"
    );
    // Every clause of a connector that answers no heartbeat ends in time, the host's own
    // liveness included.
    let took = started.elapsed();
    assert!(took.as_secs() < 600, "certification took {took:?}");
}

#[tokio::test]
async fn a_connector_that_refuses_a_feature_it_does_not_know_fails_its_handshake() {
    let target = Target::connected(|| Box::pin(async { served(Fault::RefusesUnknownFeatures) }))
        .credit_watch(crate::BRIEF);
    let report = certify_source(&target, serde_json::json!({})).await;
    assert!(
        matches!(report.outcome("P-HANDSHAKE"), Some(Outcome::Failed(_))),
        "{report}"
    );
}

#[tokio::test]
async fn a_destination_of_short_identifiers_keeps_the_protocols_clauses() {
    let target = Target::connected(|| Box::pin(async { served(Fault::ShortNames) }));
    let report = certify_destination(&target, serde_json::json!({}), &Unprobed).await;
    for id in ["P-MALFORMED", "P-LIMITS"] {
        assert_eq!(report.outcome(id), Some(&Outcome::Passed), "{id}: {report}");
    }
}
