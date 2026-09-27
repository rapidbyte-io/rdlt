//! Connectors that cannot be reached.

use rdlt_certify::{Outcome, PROTOCOL_CLAUSES, SOURCE_CLAUSES, Target, certify_source};
use rdlt_connector::ConnectorId;
use rdlt_host::{ConnectorRef, Identity, Local, Remote};
use rdlt_testkit::tls::Pki;

fn reference() -> ConnectorRef {
    ConnectorRef::new(ConnectorId::parse("test.unreached").expect("a valid id"))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_connector_that_cannot_be_reached_fails_every_clause_and_is_named_as_given() {
    let pki = Pki::new("ca");
    let host = pki.client("host");
    let identity = Identity {
        cert: host.cert,
        key: host.key,
    };
    let endpoint = "grpcs://127.0.0.1:1";
    let targets = [
        (
            Target::spawned(Local::new(), reference().path("/bin/true")),
            "/bin/true",
            "Spawned",
        ),
        (
            Target::spawned(Local::new(), reference().path("/nonexistent/connector")),
            "test.unreached",
            "Spawned",
        ),
        (
            Target::listening(
                Remote::new(identity, pki.ca()),
                reference().endpoint(endpoint),
            ),
            endpoint,
            "Listening",
        ),
        (
            Target::connected(|| {
                Box::pin(async { Err(std::io::Error::from(std::io::ErrorKind::ConnectionRefused)) })
            }),
            "a connector reached by a function",
            "Connected",
        ),
    ];
    for (target, named, shown) in targets {
        assert!(format!("{target:?}").contains(shown), "{target:?}");
        let report = certify_source(&target, serde_json::json!({})).await;
        assert_eq!(report.connector, named, "{report}");
        assert_eq!(
            report.results.len(),
            PROTOCOL_CLAUSES.len() + SOURCE_CLAUSES.len(),
            "{report}"
        );
        assert!(
            report
                .results
                .iter()
                .all(|result| matches!(result.outcome, Outcome::Failed(_))),
            "{report}"
        );
    }
}
