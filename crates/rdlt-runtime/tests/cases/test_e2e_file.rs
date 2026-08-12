//! THE 039 HEADLINE (T8): a full engine run with the spawned
//! `rdlt-connector-file` binary on BOTH sides of the wire — the YAML
//! `connector:` vocabulary through the facade's `build_pipeline`, the
//! D3 choreography (receipts, state, part events) crossing two Unix
//! sockets, rows landing exactly-once, and the spawned processes dying
//! with their guards.
//!
//! Plus ONE crash arm: SIGKILL the destination child mid-run — the run
//! fails with the typed transport-fatal destination error, nothing
//! uncommitted becomes visible, the WAL survives, and a fresh run (new
//! spawns) converges to exactly-once. The full kill MATRIX at every
//! message boundary is 040's; this is the single proving arm.
//!
//! Bin location and the `RDLT_BUILD_CONNECTOR_BINS` build guard are
//! shared with the T6 smoke (`test_spawned_bins::built_bin`).

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use rdlt::pipeline_spec::{Spec, build_pipeline, build_pipeline_with};
use rdlt::{PipelineEvent, RdltError};
use rdlt_runtime::{
    ConnectorProvider, ConnectorRequirement, LocalBinaryConnectorProvider, ManagedDestination,
    ManagedSource, ProviderError,
};

use super::test_spawned_bins::built_bin;

/// One spawned child as the recording provider saw it hand the managed
/// object over: which role, its pid, and the socket its guard unlinks.
struct SpawnRecord {
    role: &'static str,
    pid: u32,
    socket: PathBuf,
}

/// [`LocalBinaryConnectorProvider`] by delegation, recording each
/// spawn's pid and socket path on the way past — the crash arm kills by
/// the recorded pid, and the headline's "processes are GONE after drop"
/// assertion polls it. Pure observation: the managed objects are handed
/// to the engine untouched, so the run under test IS the production
/// spawn path.
struct RecordingProvider {
    inner: LocalBinaryConnectorProvider,
    spawned: Mutex<Vec<SpawnRecord>>,
}

impl RecordingProvider {
    fn new() -> Self {
        Self {
            inner: LocalBinaryConnectorProvider::new(),
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
impl ConnectorProvider for RecordingProvider {
    async fn source(
        &self,
        requirement: &ConnectorRequirement,
        config: &serde_json::Value,
    ) -> Result<ManagedSource, ProviderError> {
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
        requirement: &ConnectorRequirement,
        config: &serde_json::Value,
    ) -> Result<ManagedDestination, ProviderError> {
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

/// Write `rows` jsonl fixture rows (`{"id": N, "name": "row-N"}`) and
/// return the file's path.
fn write_fixture(dir: &Path, rows: u64) -> PathBuf {
    let mut text = String::new();
    for id in 0..rows {
        text.push_str(&format!("{{\"id\":{id},\"name\":\"row-{id}\"}}\n"));
    }
    let path = dir.join("rows.jsonl");
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
    fixture_glob: &str,
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
        \x20   id: io.rapidbyte.file\n\
        \x20   path: {bin}\n\
        \x20   config:\n\
        \x20     streams:\n\
        \x20       - name: events\n\
        \x20         format: jsonl\n\
        \x20         path: \"{fixture_glob}\"\n\
         destination:\n\
        \x20 connector:\n\
        \x20   id: io.rapidbyte.file\n\
        \x20   path: {bin}\n\
        \x20   config:\n\
        \x20     path: \"{out}\"\n\
        \x20     format: jsonl\n",
        workdir = workdir.display(),
        bin = bin.display(),
        out = out_dir.display(),
    )
}

fn parse_spec(text: &str) -> Spec {
    serde_yaml::from_str(text).expect("the connector pipeline document parses")
}

/// Every PUBLISHED row's `id` under the output root, sorted. Committed
/// data only, by the file destination's own visibility contract:
/// staged parts are dot-prefixed and bookkeeping documents are
/// `_rdlt_`-prefixed, so both are excluded — what remains is exactly
/// what a reader of the destination would see.
fn published_ids(out_dir: &Path) -> Vec<u64> {
    let mut ids = Vec::new();
    let mut stack = vec![out_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries {
            let entry = entry.expect("the output tree lists");
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') || name.starts_with("_rdlt") {
                continue;
            }
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "jsonl") {
                let text = std::fs::read_to_string(&path).expect("a published part reads");
                for line in text.lines() {
                    let row: serde_json::Value = serde_json::from_str(line)
                        .expect("every published line is a complete JSON row — never torn");
                    ids.push(row["id"].as_u64().expect("the fixture id survived"));
                }
            }
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

/// THE HEADLINE: jsonl fixture → spawned file SOURCE → engine →
/// spawned file DESTINATION, both sides resolved from the `connector:`
/// document. Asserts, in order: rows land exactly-once (count AND
/// content against the fixture), the report's totals match with
/// `output_bytes > 0` (the part-close events crossed the wire), a
/// second run through the DEFAULT provider immediately succeeds
/// reading nothing new (the lease was released and the cursor
/// round-tripped), and the spawned processes are gone with their
/// sockets unlinked once the pipeline is dropped.
#[tokio::test(flavor = "multi_thread")]
async fn the_headline_a_full_run_over_spawned_connectors_lands_exactly_once() {
    const ROWS: u64 = 500;
    let bin = built_bin("rdlt-connector-file");
    let dir = tempfile::tempdir().expect("tempdir");
    let src_dir = dir.path().join("src");
    let out_dir = dir.path().join("out");
    let workdir = dir.path().join("work");
    std::fs::create_dir_all(&src_dir).expect("fixture dir");
    std::fs::create_dir_all(&out_dir).expect("output dir");
    write_fixture(&src_dir, ROWS);

    let spec = parse_spec(&spec_yaml(
        "t8-headline",
        &workdir,
        &bin,
        &format!("{}/*.jsonl", src_dir.display()),
        &out_dir,
        None,
    ));

    let provider = RecordingProvider::new();
    let pipeline = build_pipeline_with(&spec, std::path::Path::new(""), &provider)
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
    assert!(
        events_table.output_bytes > 0,
        "output_bytes is fed by PartClosed events — zero would mean the \
         part-event callback never crossed the wire"
    );
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

    // The lease released + the cursor round-tripped: a SECOND run of
    // the same document — through the DEFAULT provider this time, the
    // exact path `build_pipeline` gives embedders — succeeds
    // immediately (no lease wait) and reads nothing new.
    let report2 = build_pipeline(&spec, std::path::Path::new(""))
        .await
        .expect("fresh spawns for the second run")
        .run()
        .await
        .expect("the second run acquires the released lease immediately");
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

/// Rewrite the crashed session's lease document as if its TTL had
/// elapsed. A SIGKILLed holder cannot release its lease; the recorded
/// 037 rule is that such a lease expires by `TTL_SECS` (300 s) — real
/// on the wall clock, unpayable in a gate — so the test ages the
/// STAMPS instead and lets the next session take the lease over
/// through the production stale-takeover path, exactly as it would
/// five minutes later.
fn age_the_crashed_lease(out_dir: &Path) {
    let mut aged = 0;
    for entry in std::fs::read_dir(out_dir).expect("the output root lists") {
        let entry = entry.expect("the output root lists");
        let name = entry.file_name().to_string_lossy().into_owned();
        if !(name.starts_with("_rdlt_lease.") && name.ends_with(".json")) {
            continue;
        }
        let text = std::fs::read_to_string(entry.path()).expect("the lease doc reads");
        let mut doc: serde_json::Value =
            serde_json::from_str(&text).expect("the crashed session's lease doc parses");
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is past the epoch")
            .as_millis() as u64;
        let stale_ms = now_ms.saturating_sub(400_000); // well past TTL_SECS = 300
        doc["renewed_at_ms"] = stale_ms.into();
        doc["acquired_at_ms"] = stale_ms.into();
        std::fs::write(entry.path(), serde_json::to_vec(&doc).expect("re-encodes"))
            .expect("the aged lease doc writes");
        aged += 1;
    }
    assert_eq!(
        aged, 1,
        "the SIGKILLed session must have left exactly its one lease document behind"
    );
}

/// THE CRASH ARM: SIGKILL the destination child after the first
/// `BatchLoaded` event — rows are provably flowing — and assert the
/// run fails with the typed transport-fatal destination error, that
/// only committed data (possibly none) is visible at the destination,
/// that the WAL survives the abort, and that a FRESH run (new spawns)
/// then converges to exactly-once through the receipts/cursor
/// machinery answering across the wire.
#[tokio::test(flavor = "multi_thread")]
async fn sigkilling_the_destination_mid_run_fails_typed_and_a_fresh_run_converges() {
    const ROWS: u64 = 20_000;
    let bin = built_bin("rdlt-connector-file");
    let dir = tempfile::tempdir().expect("tempdir");
    let src_dir = dir.path().join("src");
    let out_dir = dir.path().join("out");
    let workdir = dir.path().join("work");
    std::fs::create_dir_all(&src_dir).expect("fixture dir");
    std::fs::create_dir_all(&out_dir).expect("output dir");
    write_fixture(&src_dir, ROWS);

    // Small write batches so the run is MANY destination RPCs long —
    // the kill after RPC 1 of ~40 lands far from the finish line.
    let spec = parse_spec(&spec_yaml(
        "t8-crash",
        &workdir,
        &bin,
        &format!("{}/*.jsonl", src_dir.display()),
        &out_dir,
        Some(500),
    ));

    let provider = RecordingProvider::new();
    let pipeline = build_pipeline_with(&spec, std::path::Path::new(""), &provider)
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
        RdltError::Destination {
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

    // A fresh session (new pid, new lease owner) faces the crashed
    // session's unreleased lease; age it past its TTL rather than
    // waiting out the real 300 s, then let the production takeover
    // path do the rest.
    age_the_crashed_lease(&out_dir);

    let report = build_pipeline(&spec, std::path::Path::new(""))
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
