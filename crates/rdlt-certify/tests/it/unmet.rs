//! Connectors that cannot be certified as asked: silent, refusing the configuration, serving
//! another role, or vouched for by no authority the host trusts.

use std::sync::{Arc, Mutex};

use rdlt_certify::{
    DESTINATION_CLAUSES, Outcome, PROTOCOL_CLAUSES, SOURCE_CLAUSES, Target, Unprobed,
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
        PROTOCOL_CLAUSES.len() + SOURCE_CLAUSES.len()
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
async fn a_role_the_connector_does_not_serve_skips_every_clause() {
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
            .all(|result| matches!(result.outcome, Outcome::Skipped(_))),
        "{report}"
    );
    assert!(!report.passed(), "nothing was certified: {report}");
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
    let target = Target::spawned(Local::new(), ConnectorRef::new(id).path(&script));
    let report = certify_source(&target, json!({})).await;
    for id in SOURCE_CLAUSES.iter().map(|clause| clause.id) {
        let Some(Outcome::Failed(reason)) = report.outcome(id) else {
            panic!("{id} did not fail: {report}");
        };
        assert!(reason.contains("DATABASE_URL is not set"), "{id}: {reason}");
    }
}
