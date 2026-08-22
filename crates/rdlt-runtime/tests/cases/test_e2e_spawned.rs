//! THE HEADLINE: a full engine run with the spawned
//! `rdlt-connector-reference` binary on BOTH sides of the wire — the
//! YAML `connector:` vocabulary through the facade's `Pipeline::from_document`,
//! the commit choreography (receipts, state, part events) crossing two
//! Unix sockets, rows landing exactly-once, and the spawned processes
//! dying with their guards. The reference connector is the spawn
//! subject because it lives beside the engine forever; the first-party
//! connectors prove this same choreography in their own repository's
//! suites.
//!
//! Plus ONE crash arm: SIGKILL the destination child mid-run — the run
//! fails with the typed transport-fatal destination error, nothing
//! uncommitted becomes visible, the WAL survives, and a fresh run (new
//! spawns) converges to exactly-once. The full kill MATRIX at every
//! message boundary is the certifier's; this is the single proving arm.
//!
//! Bin location and the `RDLT_BUILD_CONNECTOR_BINS` build guard are
//! shared with the spawn smoke (`test_spawned_bins::built_bin`).

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use rdlt::document::Document;
use rdlt::error::Error;
use rdlt::event::PipelineEvent;
use rdlt::pipeline::Pipeline;
use rdlt_connector_client::handshake::Requirement;
use rdlt_connector_client::{destination, source};
use rdlt_runtime::local::Local;
use rdlt_runtime::managed::Managed;
use rdlt_runtime::provider::{self, Provider};

use super::test_spawned_bins::built_bin;

/// One spawned child as the recording provider saw it hand the managed
/// object over: which role, its pid, and the socket its guard unlinks.
struct SpawnRecord {
    role: &'static str,
    pid: u32,
    socket: PathBuf,
}

/// [`Local`] by delegation, recording each
/// spawn's pid and socket path on the way past — the crash arm kills by
/// the recorded pid, and the headline's "processes are GONE after drop"
/// assertion polls it. Pure observation: the managed objects are handed
/// to the engine untouched, so the run under test IS the production
/// spawn path.
struct RecordingProvider {
    inner: Local,
    spawned: Mutex<Vec<SpawnRecord>>,
}

impl RecordingProvider {
    fn new() -> Self {
        Self {
            inner: Local::new(),
            spawned: Mutex::new(Vec::new()),
        }
    }

    fn recorded(&self, role: &str) -> Vec<(u32, PathBuf)> {
        self.spawned
            .lock()
            .expect("no recorded panic holds this lock")
            .iter()
            .filter(|record| record.role == role)
            .map(|record| (record.pid, record.socket.clone()))
            .collect()
    }
}

#[async_trait]
impl Provider for RecordingProvider {
    async fn source(
        &self,
        requirement: &Requirement,
        config: &serde_json::Value,
    ) -> Result<Managed<source::Remote>, provider::Error> {
        let managed = self.inner.source(requirement, config).await?;
        let guard = managed
            .guard()
            .expect("the local provider always attaches a guard");
        self.spawned
            .lock()
            .expect("no recorded panic holds this lock")
            .push(SpawnRecord {
                role: "source",
                pid: guard.pid().expect("a just-spawned child has a pid"),
                socket: guard.socket_path().to_path_buf(),
            });
        Ok(managed)
    }

    async fn destination(
        &self,
        requirement: &Requirement,
        config: &serde_json::Value,
    ) -> Result<Managed<destination::Remote>, provider::Error> {
        let managed = self.inner.destination(requirement, config).await?;
        let guard = managed
            .guard()
            .expect("the local provider always attaches a guard");
        self.spawned
            .lock()
            .expect("no recorded panic holds this lock")
            .push(SpawnRecord {
                role: "destination",
                pid: guard.pid().expect("a just-spawned child has a pid"),
                socket: guard.socket_path().to_path_buf(),
            });
        Ok(managed)
    }
}

/// Write `rows` jsonl fixture rows (`{"id": N, "name": "row-N"}`) as
/// `events.jsonl` — the stem names the reference source's one stream —
/// and return the file's path.
fn write_fixture(dir: &Path, rows: u64) -> PathBuf {
    let mut text = String::new();
    for id in 0..rows {
        text.push_str(&format!("{{\"id\":{id},\"name\":\"row-{id}\"}}\n"));
    }
    let path = dir.join("events.jsonl");
    std::fs::write(&path, text).expect("the fixture file writes");
    path
}

/// The pipeline document, verbatim YAML — the frozen `connector:`
/// vocabulary on BOTH sides, with an explicit path override to the
/// built bin (the bins live in target/, not on PATH).
fn spec_yaml(
    pipeline: &str,
    workdir: &Path,
    bin: &Path,
    fixture: &Path,
    out_dir: &Path,
    batch_rows: Option<u64>,
) -> String {
    let batch_policy = match batch_rows {
        Some(rows) => format!("batch_policy:\n  every_rows: {rows}\n"),
        None => String::new(),
    };
    format!(
        "pipeline: {pipeline}\n\
         workdir: {workdir}\n\
         {batch_policy}\
         source:\n\
        \x20 connector:\n\
        \x20   id: io.rapidbyte.reference\n\
        \x20   path: {bin}\n\
        \x20   config:\n\
        \x20     path: \"{fixture}\"\n\
         destination:\n\
        \x20 connector:\n\
        \x20   id: io.rapidbyte.reference\n\
        \x20   path: {bin}\n\
        \x20   config:\n\
        \x20     path: \"{out}\"\n",
        workdir = workdir.display(),
        bin = bin.display(),
        fixture = fixture.display(),
        out = out_dir.display(),
    )
}

fn parse_document(text: &str) -> Document {
    rdlt::document::parse(text).expect("the connector pipeline document parses")
}

/// Every PUBLISHED row's `id` under the output directory, sorted.
/// Committed data only, by the reference destination's own visibility
/// contract: published parts are `<table>-<load_id>-<part>.jsonl`, and
/// every underscore-prefixed name (the `_reference_*` bookkeeping
/// documents, `_staged-*` temporaries) is what a reader never sees, so
/// both are excluded — what remains is exactly what a reader of the
/// destination would see.
fn published_ids(out_dir: &Path) -> Vec<u64> {
    let mut ids = Vec::new();
    let Ok(entries) = std::fs::read_dir(out_dir) else {
        return ids;
    };
    for entry in entries {
        let entry = entry.expect("the output directory lists");
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('_') || !name.ends_with(".jsonl") {
            continue;
        }
        let text = std::fs::read_to_string(entry.path()).expect("a published part reads");
        for line in text.lines() {
            let row: serde_json::Value = serde_json::from_str(line)
                .expect("every published line is a complete JSON row — never torn");
            ids.push(row["id"].as_u64().expect("the fixture id survived"));
        }
    }
    ids.sort_unstable();
    ids
}

/// Poll until the process is genuinely dead — `/proc/<pid>` gone, or a
/// zombie awaiting tokio's background reaper (state `Z`, no longer
/// running anything). The guard's `start_kill` only SENDS the signal,
/// so a bounded wait is the honest shape, not a race.
async fn assert_process_dead(pid: u32, who: &str) {
    for _ in 0..200 {
        match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Err(_) => return,
            Ok(stat) => {
                // The state field follows the last ')' (comm may itself
                // contain anything).
                let state = stat
                    .rsplit_once(')')
                    .map(|(_, rest)| rest.trim_start().chars().next().unwrap_or('?'));
                if state == Some('Z') {
                    return;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("{who} process {pid} is still alive 5s after its guard dropped");
}

/// Poll until the guard's socket unlink has happened (drop order inside
/// the engine's teardown is not this test's contract; that the socket
/// is GONE shortly after the run returns is).
async fn assert_socket_unlinked(socket: &Path, who: &str) {
    for _ in 0..200 {
        if !socket.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!(
        "{who} socket {} still exists 5s after its guard dropped",
        socket.display()
    );
}

/// THE HEADLINE: jsonl fixture → spawned reference SOURCE → engine →
/// spawned reference DESTINATION, both sides resolved from the
/// `connector:` document. Asserts, in order: rows land exactly-once
/// (count AND content against the fixture), the report's totals match,
/// a second run through the DEFAULT provider immediately succeeds
/// reading nothing new (the cursor round-tripped and persisted), and
/// the spawned processes are gone with their sockets unlinked once the
/// pipeline is dropped.
#[tokio::test(flavor = "multi_thread")]
async fn the_headline_a_full_run_over_spawned_connectors_lands_exactly_once() {
    const ROWS: u64 = 500;
    let bin = built_bin();
    let dir = tempfile::tempdir().expect("tempdir");
    let src_dir = dir.path().join("src");
    let out_dir = dir.path().join("out");
    let workdir = dir.path().join("work");
    std::fs::create_dir_all(&src_dir).expect("fixture dir");
    std::fs::create_dir_all(&out_dir).expect("output dir");
    let fixture = write_fixture(&src_dir, ROWS);

    let spec = parse_document(&spec_yaml(
        "t8-headline",
        &workdir,
        &bin,
        &fixture,
        &out_dir,
        None,
    ));

    let provider = RecordingProvider::new();
    let pipeline = Pipeline::from_document_with(&spec, std::path::Path::new(""), &provider)
        .await
        .expect("both connector requirements spawn and handshake");
    let report = pipeline
        .run()
        .await
        .expect("the run over two spawned connectors succeeds");

    // Exactly-once, counted three ways: the report's totals, the
    // published row count, and the published CONTENT against the
    // fixture (every id exactly once).
    assert_eq!(report.total_rows(), ROWS, "the report's committed total");
    let events_table = report
        .tables
        .iter()
        .find(|(table, _)| table.as_str() == "events")
        .map(|(_, table_report)| *table_report)
        .expect("the stream's root table is in the report");
    assert_eq!(events_table.rows, ROWS);
    assert_eq!(
        published_ids(&out_dir),
        (0..ROWS).collect::<Vec<_>>(),
        "every fixture row exactly once, none missing, none duplicated"
    );

    // The run consumed the pipeline: every managed connector has
    // dropped, so both children die by guard kill and both sockets are
    // unlinked.
    let sources = provider.recorded("source");
    let destinations = provider.recorded("destination");
    assert_eq!((sources.len(), destinations.len()), (1, 1));
    for (pid, socket) in sources.iter().chain(destinations.iter()) {
        assert_process_dead(*pid, "connector").await;
        assert_socket_unlinked(socket, "connector").await;
    }

    // The cursor round-tripped: a SECOND run of the same document —
    // through the DEFAULT provider this time, the exact path
    // `Pipeline::from_document` gives embedders — succeeds and reads nothing
    // new (the reference source's byte cursor persisted through the
    // destination's state document).
    let report2 = Pipeline::from_document(&spec, std::path::Path::new(""))
        .await
        .expect("fresh spawns for the second run")
        .run()
        .await
        .expect("the second run succeeds");
    assert_eq!(
        report2.total_rows(),
        0,
        "the committed cursor crossed the wire back: the file is done, nothing re-reads"
    );
    assert_eq!(
        published_ids(&out_dir),
        (0..ROWS).collect::<Vec<_>>(),
        "the second run added nothing — still exactly-once"
    );
}

/// THE CRASH ARM: SIGKILL the destination child after the first
/// `BatchLoaded` event — rows are provably flowing — and assert the
/// run fails with the typed transport-fatal destination error, that
/// only committed data (possibly none) is visible at the destination,
/// that the WAL survives the abort, and that a FRESH run (new spawns)
/// then converges to exactly-once through the receipts/cursor
/// machinery answering across the wire. (The reference destination's
/// session lease is an OS advisory lock, released by the SIGKILL with
/// the process, so the fresh session proceeds immediately — and its
/// staging died with the killed process's memory by construction.)
#[tokio::test(flavor = "multi_thread")]
async fn sigkilling_the_destination_mid_run_fails_typed_and_a_fresh_run_converges() {
    // 2k rows over 100-row batches keeps ~20 destination RPCs, plenty
    // of run left after the kill, at gate-friendly cost. (The cell was
    // sized when the reference source checkpointed per LINE — two wire
    // frames per row, measured 2k rows ≈ 5 s and 20k ≈ 240 s through
    // the debug CLI; it now checkpoints at batch boundaries, and the
    // cell deliberately stays at its resized row count.)
    const ROWS: u64 = 2_000;
    let bin = built_bin();
    let dir = tempfile::tempdir().expect("tempdir");
    let src_dir = dir.path().join("src");
    let out_dir = dir.path().join("out");
    let workdir = dir.path().join("work");
    std::fs::create_dir_all(&src_dir).expect("fixture dir");
    std::fs::create_dir_all(&out_dir).expect("output dir");
    let fixture = write_fixture(&src_dir, ROWS);

    // Small write batches so the run is MANY destination RPCs long —
    // the kill after RPC 1 of ~20 lands far from the finish line.
    let spec = parse_document(&spec_yaml(
        "t8-crash",
        &workdir,
        &bin,
        &fixture,
        &out_dir,
        Some(100),
    ));

    let provider = RecordingProvider::new();
    let pipeline = Pipeline::from_document_with(&spec, std::path::Path::new(""), &provider)
        .await
        .expect("both connector requirements spawn and handshake");
    let (dest_pid, dest_socket) = provider.recorded("destination")[0].clone();
    let (source_pid, source_socket) = provider.recorded("source")[0].clone();

    // Kill the destination child the moment the first batch has
    // demonstrably reached it over the wire.
    let mut events = pipeline.events();
    let killer = tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            if matches!(event, PipelineEvent::BatchLoaded { .. }) {
                let status = std::process::Command::new("kill")
                    .args(["-KILL", &dest_pid.to_string()])
                    .status()
                    .expect("kill(1) runs");
                assert!(status.success(), "SIGKILL delivered");
                return true;
            }
        }
        false
    });

    let error = pipeline
        .run()
        .await
        .expect_err("a SIGKILLed destination must abort the run");
    assert!(
        killer.await.expect("the killer task must not panic"),
        "the kill fired on an observed BatchLoaded — rows were flowing"
    );

    // Typed, transport class, NOT retryable: the client maps a broken
    // transport to DestinationError::Fatal, and the engine surfaces it
    // as the non-retryable Destination arm.
    match &error {
        Error::Destination {
            message, retryable, ..
        } => {
            assert!(
                !retryable,
                "a dead transport is fatal, never retried in place"
            );
            assert!(
                message.contains("connector transport"),
                "the transport class is named in the cause: {message}"
            );
        }
        other => panic!("expected the typed destination error, got: {other}"),
    }

    // No partial publish: whatever is visible parses cleanly (the
    // reader in published_ids refuses torn lines) and is duplicate-free,
    // within the fixture range — committed loads only.
    let after_crash = published_ids(&out_dir);
    assert!(
        after_crash.len() as u64 <= ROWS,
        "never more rows than the fixture holds"
    );
    assert!(
        after_crash.iter().all(|id| *id < ROWS),
        "every visible id is a fixture id — nothing invented"
    );
    let mut deduped = after_crash.clone();
    deduped.dedup();
    assert_eq!(
        deduped, after_crash,
        "no id is visible twice after the crash"
    );

    // The WAL survived the abort — recovery has something to work from.
    assert!(
        workdir.join("wal").is_dir(),
        "the WAL directory survives a destination-fatal abort"
    );

    // The source child is also gone: its guard dropped with the failed
    // run, so ONE crash cannot leak the healthy half. And the failure
    // path unlinks BOTH sockets — the SIGKILLed child cannot clean up
    // after itself, so its socket file is the guard's to reclaim.
    assert_process_dead(dest_pid, "destination").await;
    assert_process_dead(source_pid, "source").await;
    assert_socket_unlinked(&dest_socket, "destination").await;
    assert_socket_unlinked(&source_socket, "source").await;

    // A fresh session (new pid, new spawns) converges over the crashed
    // session's remains: the receipts answer replay across the wire,
    // the persisted state feeds the source's resume cursor.
    let report = Pipeline::from_document(&spec, std::path::Path::new(""))
        .await
        .expect("fresh spawns for the recovery run")
        .run()
        .await
        .expect("the recovery run completes over the crashed session's remains");
    assert!(
        report.commits > 0,
        "the recovery run published at least once"
    );
    assert_eq!(
        published_ids(&out_dir),
        (0..ROWS).collect::<Vec<_>>(),
        "after recovery every fixture row is visible EXACTLY once — the receipts \
         and cursor machinery answered across the wire"
    );
}
