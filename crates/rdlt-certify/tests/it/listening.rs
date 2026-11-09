//! A connector listening at an endpoint over mutual TLS, certified through the protocol.

use rdlt_certify::{Outcome, Target, certify_destination, certify_source};
use rdlt_connector::ConnectorId;
use rdlt_host::{ConnectorRef, Identity, Remote};
use rdlt_testkit::tls::Pki;
use serde_json::json;
use tokio::process::Child;

use crate::spawned::{SqliteProbe, example};

/// The reference connectors' binary, listening over mutual TLS with a CA made for it; its
/// process, and its endpoint.
pub(crate) async fn listening(pki: &Pki) -> (Child, String) {
    rdlt_testkit::tls::listening(pki, &example("serve_reference")).await
}

pub(crate) fn target(pki: &Pki, endpoint: &str) -> Target {
    let host = pki.client("host");
    let identity = Identity {
        cert: host.cert,
        key: host.key,
    };
    let id = ConnectorId::parse("io.rapidbyte.reference").expect("a valid id");
    Target::listening(
        Remote::new(identity, pki.ca()),
        ConnectorRef::new(id).endpoint(endpoint),
    )
    .credit_watch(crate::BRIEF)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_listening_connector_is_certified_over_mutual_tls() {
    let pki = Pki::new("ca");
    let (_connector, endpoint) = listening(&pki).await;
    let target = target(&pki, &endpoint);
    let config = json!({ "streams": { "users": [{"id": 1}, {"id": 2}] }, "page_size": 1 });
    let source = certify_source(&target, config).await;
    // Two rows end before a kill lands.
    assert_eq!(crate::unobserved(&source), ["K-SOURCE"], "{source}");
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("listening.db");
    let report = certify_destination(&target, json!({ "path": path }), &SqliteProbe(path)).await;
    report.assert_passed();
    // Its connections cut, as a host cuts a connector it cannot kill.
    assert_eq!(
        report.outcome("K-DESTINATION"),
        Some(&Outcome::Passed),
        "{report}"
    );
    // A cut is what the report says the clause passed on: no process was seen to stop.
    let note = report.note("K-DESTINATION").expect("a note");
    assert!(note.starts_with("cut: "), "{report}");
    assert!(
        report
            .to_string()
            .contains(&format!("  pass K-DESTINATION ({note})\n"))
    );
}
