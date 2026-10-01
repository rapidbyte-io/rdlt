//! A connector listening at an endpoint over mutual TLS, certified through the protocol.

use std::process::Stdio;

use rdlt_certify::{Outcome, Target, certify_destination, certify_source};
use rdlt_connector::ConnectorId;
use rdlt_host::{ConnectorRef, Identity, Remote};
use rdlt_testkit::tls::Pki;
use serde_json::json;
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::process::{Child, Command};

use crate::spawned::{SqliteProbe, example};

/// The reference connectors' binary, listening over mutual TLS with a CA made for it; its
/// process, and its endpoint.
pub(crate) async fn listening(pki: &Pki) -> (Child, String) {
    let server = pki.server("server", &["localhost"]);
    let mut child = Command::new(example("serve_reference"))
        .args(["--listen", "127.0.0.1:0"])
        .arg("--tls-cert")
        .arg(&server.cert)
        .arg("--tls-key")
        .arg(&server.key)
        .arg("--tls-client-ca")
        .arg(pki.ca())
        .env(
            "LLVM_PROFILE_FILE",
            std::env::var_os("LLVM_PROFILE_FILE").unwrap_or_default(),
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("the connector starts");
    let stdout = child.stdout.take().expect("its output is piped");
    let line = BufReader::new(stdout)
        .lines()
        .next_line()
        .await
        .expect("its output reads")
        .expect("it announces its address");
    let address = line
        .strip_prefix("listening on ")
        .expect("the announcement names the address");
    let port = address.rsplit_once(':').expect("an address and port").1;
    (child, format!("grpcs://localhost:{port}"))
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
}

#[tokio::test(flavor = "multi_thread")]
async fn a_listening_connector_is_certified_over_mutual_tls() {
    let pki = Pki::new("ca");
    let (_connector, endpoint) = listening(&pki).await;
    let target = target(&pki, &endpoint);
    let config = json!({ "streams": { "users": [{"id": 1}, {"id": 2}] } });
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
}
