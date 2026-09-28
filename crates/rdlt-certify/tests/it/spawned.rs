//! A connector binary, spawned for each connection, and certified through the protocol.

use std::path::PathBuf;

use rdlt_certify::{Outcome, Probe, Target, Unprobed, certify_destination, certify_source};
use rdlt_connector::serve::Served;
use rdlt_connector::{BoxFuture, ConnectorId, TableRef, source_factory};
use rdlt_connector_reference::{MemorySource, sqlite};
use rdlt_host::{ConnectorRef, Local};
use serde_json::json;

/// The example binary `name`, which the test build builds.
pub(crate) fn example(name: &str) -> PathBuf {
    let tests = std::env::current_exe().expect("the test binary has a path");
    tests
        .ancestors()
        .map(|dir| dir.join("examples").join(name))
        .find(|example| example.is_file())
        .expect("the test build builds the examples")
}

/// The reference connectors' binary, spawned for each connection, keeping the coverage variable.
pub(crate) fn reference() -> Target {
    let id = ConnectorId::parse("io.rapidbyte.reference").expect("a valid id");
    let local = Local::new().env_passthrough("LLVM_PROFILE_FILE");
    Target::spawned(
        local,
        ConnectorRef::new(id).path(example("serve_reference")),
    )
}

/// Reads what the SQLite destination published in the database at its path.
pub(crate) struct SqliteProbe(pub(crate) PathBuf);

impl Probe for SqliteProbe {
    fn published<'a>(
        &'a self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, rdlt_connector::Result<Vec<arrow_array::RecordBatch>>> {
        let batches = sqlite::published(&self.0, &table.name);
        Box::pin(async move { batches })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_spawned_source_binary_is_certified_through_the_protocol() {
    let config =
        json!({ "streams": { "users": [{"id": 1}, {"id": 2}, {"id": 3}] }, "page_size": 1 });
    let report = certify_source(&reference(), config).await;
    report.assert_passed();
    assert_eq!(report.connector, "io.rapidbyte.memory", "{report}");
    assert_eq!(
        report.outcome("P-CREDIT"),
        Some(&Outcome::Passed),
        "{report}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_spawned_destination_binary_is_certified_through_the_protocol() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("certify.db");
    let report =
        certify_destination(&reference(), json!({ "path": path }), &SqliteProbe(path)).await;
    report.assert_passed();
    assert_eq!(report.connector, "io.rapidbyte.sqlite", "{report}");
    for id in [
        "P-HANDSHAKE",
        "P-MALFORMED",
        "D-COMMIT",
        "D-FENCE",
        "K-DESTINATION",
    ] {
        assert_eq!(report.outcome(id), Some(&Outcome::Passed), "{id}: {report}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_destination_nothing_can_read_skips_the_clauses_that_read_what_it_published() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let config = json!({ "path": directory.path().join("unread.db") });
    let report = certify_destination(&reference(), config, &Unprobed).await;
    report.assert_passed();
    for id in ["D-CHECK", "D-EPOCH", "D-STATE"] {
        assert_eq!(report.outcome(id), Some(&Outcome::Passed), "{id}: {report}");
    }
    assert!(
        matches!(report.outcome("D-COMMIT"), Some(Outcome::Skipped(_))),
        "{report}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_spawned_connector_refusing_its_configuration_fails_as_it_does_in_process() {
    let refused = serde_json::json!({ "streams": 5 });
    let served = Target::served(Served::new().with_source(source_factory::<MemorySource>()));
    let in_process = certify_source(&served, refused.clone()).await;
    let spawned = certify_source(&reference(), refused).await;
    // A connector that refused, rather than ended, is not said to have ended.
    assert_eq!(spawned.outcome("S-CHECK"), in_process.outcome("S-CHECK"));
}
