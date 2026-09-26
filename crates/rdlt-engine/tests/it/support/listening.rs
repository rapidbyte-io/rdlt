//! Reference destinations listening on the network, reached over mutual TLS, each for one run.

use std::process::Stdio;
use std::sync::Arc;

use rdlt_connector::{
    BoxFuture, Capabilities, ConnectorId, Destination, OpenContext, OpenedSession, Result,
};
use rdlt_host::{ConnectorRef, Identity, Provider as _, Remote};
use rdlt_testkit::tls::Pki;
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::process::{Child, Command};

/// A destination reached over the network, and the listening connector serving it, which is
/// killed when the destination is dropped.
struct Hosted {
    destination: Arc<dyn Destination>,
    _connector: Child,
    _pki: Pki,
}

impl Destination for Hosted {
    fn capabilities(&self) -> &Capabilities {
        self.destination.capabilities()
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        self.destination.check()
    }

    fn open<'a>(&'a self, context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        self.destination.open(context)
    }
}

/// Destination `id`, served by `example` listening on a free port over mutual TLS with a CA made
/// for it, and placed there with `config`.
pub(crate) async fn listening(
    id: &str,
    example: &str,
    config: &serde_json::Value,
) -> Arc<dyn Destination> {
    let pki = Pki::new("ca");
    let server = pki.server("server", &["localhost"]);
    let mut connector = Command::new(crate::support::example(example))
        .args(["--listen", "127.0.0.1:0"])
        .arg("--tls-cert")
        .arg(&server.cert)
        .arg("--tls-key")
        .arg(&server.key)
        .arg("--tls-client-ca")
        .arg(pki.ca())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("the connector starts");
    let stdout = connector.stdout.take().expect("its output is piped");
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
    let host = pki.client("host");
    let identity = Identity {
        cert: host.cert,
        key: host.key,
    };
    let reference = ConnectorRef::new(ConnectorId::parse(id).expect("a valid id"))
        .endpoint(format!("grpcs://localhost:{port}"));
    let placed = Remote::new(identity, pki.ca())
        .destination(&reference, config)
        .await
        .expect("the destination is placed");
    Arc::new(Hosted {
        destination: Arc::from(placed.connector),
        _connector: connector,
        _pki: pki,
    })
}
