//! Connectors that cannot be reached, or whose clauses the registry lists.

use std::collections::BTreeSet;

use rdlt_certify::{
    DESTINATION_CLAUSES, Family, KILL_CLAUSES, Outcome, PROTOCOL_CLAUSES, SOURCE_CLAUSES, Target,
    certify_source, clauses, markdown,
};
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
    // A binary that exits at once, serving nothing, wherever the platform keeps its own.
    let directory = tempfile::tempdir().expect("a temporary directory");
    let exits = directory.path().join("exits");
    std::fs::write(&exits, "#!/bin/sh\nexit 0\n").expect("the script writes");
    std::fs::set_permissions(&exits, std::os::unix::fs::PermissionsExt::from_mode(0o755))
        .expect("the script is executable");
    let named = exits.display().to_string();
    let targets = [
        (
            Target::spawned(Local::new(), reference().path(&exits)),
            named.as_str(),
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
            // And `K-SOURCE`.
            PROTOCOL_CLAUSES.len() + SOURCE_CLAUSES.len() + 1,
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

#[test]
fn every_clause_is_registered_once_under_its_family() {
    let registered: Vec<_> = clauses().collect();
    assert_eq!(
        registered.len(),
        PROTOCOL_CLAUSES.len()
            + SOURCE_CLAUSES.len()
            + DESTINATION_CLAUSES.len()
            + KILL_CLAUSES.len()
    );
    let ids: BTreeSet<&str> = registered.iter().map(|(_, clause)| clause.id).collect();
    assert_eq!(ids.len(), registered.len(), "clause ids are distinct");
    for (family, clause) in registered {
        let prefix = match family {
            Family::Protocol => "P-",
            Family::Source => "S-",
            Family::Destination => "D-",
            Family::Kill => "K-",
        };
        assert!(clause.id.starts_with(prefix), "{family:?} {}", clause.id);
        assert!(markdown().contains(&format!("| `{}` |", clause.id)));
    }
}

#[tokio::test(start_paused = true)]
async fn a_connector_whose_stream_never_opens_fails_every_clause_instead_of_holding_them() {
    let target = Target::connected(|| Box::pin(std::future::pending()));
    let week = std::time::Duration::from_hours(7 * 24);
    let report = tokio::time::timeout(week, certify_source(&target, serde_json::json!({})))
        .await
        .expect("a stream that never opens holds no certification");
    assert!(
        report
            .results
            .iter()
            .all(|result| matches!(result.outcome, Outcome::Failed(_))),
        "{report}"
    );
}
