//! The facade's placement matrix: one pipeline in process, as sandboxed local connectors and
//! remotely over mutual TLS, each placed where its host allows and loading every row once.

use std::path::Path;

use rdlt::arrow::array::RecordBatch;
use rdlt::{Connector, Host, Pipeline, Placement, Side, Stream};
use rdlt_connector_reference::{MemorySource, SqliteDestination};

use crate::support::within;

const ROWS: usize = 1000;

/// `source`, the memory connector, holding the matrix's rows of `events`.
fn rows(source: Connector) -> Side {
    let rows: Vec<serde_json::Value> = (0..ROWS)
        .map(|id| serde_json::json!({ "id": id }))
        .collect();
    Side::new(source).config(serde_json::json!({ "streams": { "events": rows } }))
}

/// Loads the matrix's rows into `database` on `host`, its connectors named by `source` and
/// `destination`, each placed as `placed` says; answers the rows the table holds.
async fn loaded(
    host: &Host,
    (source, destination): (Side, Side),
    placed: fn(&Placement) -> bool,
    database: &Path,
) -> usize {
    let pipeline = Pipeline::builder("matrix", source, destination)
        .stream(Stream::new("events"))
        .build(host);
    let pipeline = within(pipeline).await.expect("built");
    for connector in [pipeline.source(), pipeline.destination()] {
        let placement = &connector.placement;
        assert!(placed(placement), "{} runs {placement:?}", connector.id);
    }
    let outcome = within(pipeline.start()).await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    let published =
        rdlt_connector_reference::sqlite::published(database, "events").expect("published");
    published.iter().map(RecordBatch::num_rows).sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process() {
    let out = tempfile::tempdir().unwrap();
    let database = out.path().join("in-process.db");
    let host = Host::new()
        .source::<MemorySource>()
        .destination::<SqliteDestination>();
    let source = rows(Connector::new("io.rapidbyte.memory"));
    let destination = Side::new(Connector::new("io.rapidbyte.sqlite"))
        .config(serde_json::json!({ "path": database }));
    let in_process = |placement: &Placement| *placement == Placement::InProcess;
    let rows = loaded(&host, (source, destination), in_process, &database).await;
    assert_eq!(rows, ROWS);
}

/// The connectors spawned in the sandbox, each a copy with one name, since a build's output has
/// more than one and a write grant refuses a program that does.
#[cfg(feature = "local")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sandboxed_local() {
    if let Err(unusable) = rdlt_host::Bubblewrap::new().usable() {
        rdlt_testkit::process::without_sandbox(&unusable);
        return;
    }
    let connectors = tempfile::tempdir().unwrap();
    for name in ["memory", "sqlite"] {
        let copy = connectors.path().join(format!("rdlt-connector-{name}"));
        std::fs::copy(crate::support::example("reference_connector"), copy).unwrap();
    }
    let out = tempfile::tempdir().unwrap();
    let database = out.path().join("sandboxed.db");
    let host = Host::new()
        .connectors(connectors.path())
        .writable(out.path());
    let source = rows(Connector::new("io.rapidbyte.memory"));
    let destination = Side::new(Connector::new("io.rapidbyte.sqlite"))
        .config(serde_json::json!({ "path": database }))
        .access(rdlt::Access::new().write(out.path()));
    let spawned = |placement: &Placement| matches!(placement, Placement::Process { .. });
    let rows = loaded(&host, (source, destination), spawned, &database).await;
    assert_eq!(rows, ROWS);
}

#[cfg(feature = "remote")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_over_mutual_tls() {
    let pki = rdlt_testkit::tls::Pki::new("ca");
    let binary = crate::support::example("reference_connector");
    let (_connector, endpoint) = within(rdlt_testkit::tls::listening(&pki, &binary)).await;
    let client = pki.client("host");
    let tls = rdlt::Tls::from_files(&client.cert, &client.key, pki.ca())
        .await
        .expect("the host's TLS files open");
    let host = Host::new().remote(tls);
    let out = tempfile::tempdir().unwrap();
    let database = out.path().join("remote.db");
    let source = rows(Connector::new("io.rapidbyte.memory").endpoint(&endpoint));
    let destination = Side::new(Connector::new("io.rapidbyte.sqlite").endpoint(&endpoint))
        .config(serde_json::json!({ "path": database }));
    let remote = |placement: &Placement| matches!(placement, Placement::Remote { .. });
    let rows = loaded(&host, (source, destination), remote, &database).await;
    assert_eq!(rows, ROWS);
}
