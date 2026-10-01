//! A connector binary, spawned for each connection, and certified through the protocol.

use std::path::PathBuf;

use rdlt_certify::{
    Outcome, Probe, Target, Unprobed, Verdict, certify_destination, certify_source,
};
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
    // Three rows end before a kill lands.
    assert_eq!(crate::unobserved(&report), ["K-SOURCE"], "{report}");
    assert_eq!(report.connector, "io.rapidbyte.memory", "{report}");
    assert_eq!(
        report.outcome("P-CREDIT"),
        Some(&Outcome::Passed),
        "{report}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_spawned_source_killed_as_it_loads_is_spawned_again_and_resumes() {
    let id = ConnectorId::parse("io.rapidbyte.generator").expect("a valid id");
    let local = Local::new().env_passthrough("LLVM_PROFILE_FILE");
    let target = Target::spawned(local, ConnectorRef::new(id).path(example("serve_source")))
        .kill_seed(crate::killed::SETTLED_LATE);
    let config = json!({
        "seed": 11,
        "streams": [{ "name": "events", "rows": 20000, "partitions": 2, "batch_rows": 50 }],
    });
    let report = certify_source(&target, config).await;
    report.assert_passed();
    assert_eq!(
        report.outcome("K-SOURCE"),
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
        "D-MERGE",
        "D-DELETE",
        "D-PARTIAL",
        "D-TRUNCATE",
        "D-FENCE",
        "K-DESTINATION",
    ] {
        assert_eq!(report.outcome(id), Some(&Outcome::Passed), "{id}: {report}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_destination_nothing_can_read_is_certified_incompletely() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let config = json!({ "path": directory.path().join("unread.db") });
    let report = certify_destination(&reference(), config, &Unprobed).await;
    report.assert_none_failed();
    assert_eq!(report.verdict(), Verdict::Incomplete, "{report}");
    for id in ["D-CHECK", "D-EPOCH", "D-STATE"] {
        assert_eq!(report.outcome(id), Some(&Outcome::Passed), "{id}: {report}");
    }
    for id in ["D-COMMIT", "K-DESTINATION"] {
        let outcome = report.outcome(id);
        assert!(
            matches!(outcome, Some(Outcome::Unobserved(_))),
            "{id}: {report}"
        );
    }
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

/// A launcher in `directory` that starts the reference connectors' binary as a process of its
/// own, with the connection it was given, and waits; `detached` starts it in a session of its
/// own, beyond a kill of the launcher's process group.
#[cfg(target_os = "linux")]
fn launcher(directory: &std::path::Path, detached: bool) -> Target {
    let fifo = directory.join("input");
    let made = std::process::Command::new("mkfifo").arg(&fifo).status();
    assert!(made.expect("mkfifo runs").success());
    let connector = example("serve_reference");
    // Its input never ends, as a host's does not while it serves.
    let started = format!("'{}' \"$@\" <> '{}'", connector.display(), fifo.display());
    let script = if detached {
        format!("#!/bin/sh\n( setsid {started} & )\nexec sleep 86400\n")
    } else {
        format!("#!/bin/sh\n{started}\n")
    };
    let path = directory.join("launcher");
    std::fs::write(&path, script).expect("the launcher writes");
    std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o755))
        .expect("the launcher is executable");
    let id = ConnectorId::parse("io.rapidbyte.reference").expect("a valid id");
    let local = Local::new().env_passthrough("LLVM_PROFILE_FILE");
    Target::spawned(local, ConnectorRef::new(id).path(path)).kill_seed(7)
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn a_kill_clause_passes_only_when_a_kill_reached_the_connector() {
    for (detached, reached) in [(false, true), (true, false)] {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let target = launcher(directory.path(), detached);
        let path = directory.path().join("certify.db");
        let probe = SqliteProbe(path.clone());
        let report = certify_destination(&target, json!({ "path": path }), &probe).await;
        assert_eq!(report.failures().count(), 0, "{detached}: {report}");
        let killed = report.outcome("K-DESTINATION");
        if reached {
            // Killed with its launcher, the connector is started again, and loses nothing.
            assert_eq!(killed, Some(&Outcome::Passed), "{report}");
        } else {
            // The connector outlives each kill of its launcher: the answer the clause loses
            // by itself is no evidence of a kill.
            assert!(matches!(killed, Some(Outcome::Unobserved(_))), "{report}");
            assert_eq!(report.verdict(), Verdict::Incomplete, "{report}");
        }
    }
}
