//! Connectors that cannot be certified as asked: silent, refusing the configuration, serving
//! another role, or vouched for by no authority the host trusts.

use std::sync::{Arc, Mutex};

use rdlt_certify::{
    DESTINATION_CLAUSES, Outcome, PROTOCOL_CLAUSES, SOURCE_CLAUSES, Target, Unprobed, Verdict,
    certify_destination, certify_source,
};
use rdlt_connector::serve::Served;
use rdlt_connector::{ConnectorId, source_factory};
use rdlt_connector_reference::MemorySource;
use rdlt_host::{ConnectorRef, Local};
use rdlt_testkit::tls::Pki;
use serde_json::json;

use crate::listening::{listening, target};

fn failed(outcome: Option<&Outcome>) -> bool {
    matches!(outcome, Some(Outcome::Failed(_)))
}

#[tokio::test(start_paused = true)]
async fn a_connector_that_never_answers_fails_every_clause_in_time() {
    // Each connection reaches a connector that takes every byte and never answers.
    let silent = Arc::new(Mutex::new(Vec::new()));
    let target = Target::connected(move || {
        let silent = Arc::clone(&silent);
        Box::pin(async move {
            let (host, connector) = tokio::io::duplex(1 << 20);
            silent.lock().expect("the silent ends").push(connector);
            Ok(Box::new(host) as Box<dyn rdlt_host::Stream>)
        })
    });
    let started = tokio::time::Instant::now();
    let report = certify_source(&target, json!({})).await;
    assert_eq!(
        report.results.len(),
        // And `K-SOURCE`.
        PROTOCOL_CLAUSES.len() + SOURCE_CLAUSES.len() + 1
    );
    assert!(
        report
            .results
            .iter()
            .all(|result| failed(Some(&result.outcome))),
        "{report}"
    );
    let took = started.elapsed();
    assert!(took < std::time::Duration::from_secs(600), "{took:?}");
}

#[tokio::test]
async fn a_configuration_the_connector_refuses_fails_every_clause() {
    let target = Target::served(Served::new().with_source(source_factory::<MemorySource>()));
    let report = certify_source(&target, json!({ "streams": 5 })).await;
    assert!(
        report
            .results
            .iter()
            .all(|result| failed(Some(&result.outcome))),
        "{report}"
    );
}

#[tokio::test]
async fn no_clause_applies_to_a_role_the_connector_does_not_serve() {
    let target = Target::served(Served::new().with_source(source_factory::<MemorySource>()));
    let report = certify_destination(&target, json!({}), &Unprobed).await;
    assert_eq!(
        report.results.len(),
        // And `K-DESTINATION`.
        PROTOCOL_CLAUSES.len() + DESTINATION_CLAUSES.len() + 1
    );
    assert!(
        report
            .results
            .iter()
            .all(|result| matches!(result.outcome, Outcome::Inapplicable(_))),
        "{report}"
    );
    assert_eq!(
        report.verdict(),
        Verdict::Incomplete,
        "nothing was certified: {report}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_connector_no_trusted_authority_vouches_for_fails_every_clause() {
    let (_connector, endpoint) = listening(&Pki::new("theirs")).await;
    let report = certify_source(&target(&Pki::new("ours"), &endpoint), json!({})).await;
    assert!(
        report
            .results
            .iter()
            .all(|result| failed(Some(&result.outcome))),
        "{report}"
    );
}

#[tokio::test]
async fn a_spawned_connector_that_ends_at_once_is_heard_in_every_failure() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let script = directory.path().join("connector");
    std::fs::write(
        &script,
        "#!/bin/sh\necho 'error: DATABASE_URL is not set' >&2\nexit 3\n",
    )
    .expect("the script writes");
    std::fs::set_permissions(&script, std::os::unix::fs::PermissionsExt::from_mode(0o755))
        .expect("the script is executable");
    let id = ConnectorId::parse("test.ends").expect("a valid id");
    let target = Target::spawned(
        Local::trusting_binaries(),
        ConnectorRef::new(id).path(&script),
    );
    let report = certify_source(&target, json!({})).await;
    for id in SOURCE_CLAUSES.iter().map(|clause| clause.id) {
        let Some(Outcome::Failed(reason)) = report.outcome(id) else {
            panic!("{id} did not fail: {report}");
        };
        assert!(reason.contains("DATABASE_URL is not set"), "{id}: {reason}");
    }
}

#[test]
fn a_certification_that_was_cut_keeps_what_it_found_and_says_where_it_was_cut() {
    use rdlt_certify::{ClauseResult, Observed, unfinished};
    use rdlt_connector::Role;
    let target = Target::connected(|| Box::pin(std::future::pending()));
    for found in [0, 1, 9] {
        let observed = Observed::new();
        let clauses = PROTOCOL_CLAUSES.iter().chain(SOURCE_CLAUSES);
        for clause in clauses.clone().take(found) {
            observed.tell(ClauseResult {
                clause: *clause,
                outcome: Outcome::Passed,
                note: None,
            });
        }
        observed.named("io.test.cut");
        let report = unfinished(&target, Role::Source, &observed, "the bound passed");
        assert_eq!(report.connector, "io.test.cut");
        // Every clause of the role, in order: those found, the clause that was cut, and the rest.
        let ids: Vec<&str> = report
            .results
            .iter()
            .map(|result| result.clause.id)
            .collect();
        let every: Vec<&str> = clauses
            .map(|clause| clause.id)
            .chain(["K-SOURCE"])
            .collect();
        assert_eq!(ids, every);
        for (index, result) in report.results.iter().enumerate() {
            match &result.outcome {
                Outcome::Passed => assert!(index < found, "{report}"),
                Outcome::Failed(reason) => {
                    assert_eq!(index, found, "{report}");
                    assert!(reason.contains("the bound passed"), "{reason}");
                }
                Outcome::Unobserved(_) => assert!(index > found, "{report}"),
                Outcome::Inapplicable(_) => panic!("{report}"),
            }
        }
        assert_eq!(report.verdict(), Verdict::Failed);
    }
    // Before the connector's id is known, the report is named as the target is.
    let unnamed = unfinished(
        &target,
        Role::Destination,
        &Observed::new(),
        "the bound passed",
    );
    assert_eq!(unnamed.connector, target.describe());
    assert_eq!(
        unnamed.results.len(),
        PROTOCOL_CLAUSES.len() + DESTINATION_CLAUSES.len() + 1
    );
}
