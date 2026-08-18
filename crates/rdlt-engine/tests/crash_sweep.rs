//! Deterministic crash-point sweep.
//!
//! For EVERY registered fail point: arm it (error-return AND panic), run a
//! multi-commit pipeline until it dies, run again still-armed (a crash DURING
//! recovery), then disarm and recover — asserting exactly-once visibility of all
//! 100 source rows every single time, in every write disposition the substrate
//! serves.
//!
//! The substrates are in-repo: the
//! testkit's in-memory destination carries all three dispositions (and the
//! keyed structured-merge arm), and the reference connector's jsonl
//! destination carries the durable-storage arm — real part files, a real
//! receipt log, recovery reading state from disk. The connector-owned crash
//! points (pq.*, duck.*, pg.* …) are swept where their crates live now: each
//! connector's own crash_sweep suite in the rdlt-connectors repository.
//!
//! Gate G2.2: `sweep_covers_entire_registry` pins the swept list against the
//! engine's own sources — an instrumented-but-unswept boundary fails here.

#![cfg(feature = "failpoints")]

use std::path::Path;
use std::path::PathBuf;

use rdlt_connector::destination::Destination;

use rdlt_connector::source::{Source, StreamSpec};
use rdlt_core::commit::WriteMode;
use rdlt_core::failpoint::fail;
use rdlt_engine::config::Config;
use rdlt_engine::engine::Engine;
use rdlt_testkit::memory;
use serde_json::json;

const TOTAL_ROWS: u64 = 100;

/// Engine-owned fail points (registry discipline, gate G2.2): every
/// `crash_point!` site in rdlt-engine MUST appear here — grep-auditable, and
/// `sweep_covers_entire_registry` pins the union against the expected list.
const ENGINE_POINTS: &[&str] = &[
    "wal.segment.write",
    "wal.segment.fsync",
    "wal.manifest.append",
    "wal.manifest.fsync",
    "session.after_ensure",
    "session.after_write",
    "session.after_commit",
];

/// 4 checkpointed batches × 25 rows → 4 commits under EveryCheckpoints(1):
/// every sweep iteration exercises multi-commit recovery, not a single commit.
fn source() -> memory::Source {
    let batches = (0..4)
        .map(|b| {
            memory::Batch::new(
                (0..25)
                    .map(|i| json!({"id": b * 25 + i, "name": format!("row-{b}-{i}")}))
                    .collect(),
            )
            .with_checkpoint(json!({"batch": b}))
        })
        .collect();
    memory::Source::new(vec![memory::Stream::new(StreamSpec::new("s"), batches)])
}

fn config(workdir: &Path, mode: &WriteMode) -> Config {
    let mut config = Config::new("sweep");
    config = config.with_workdir(workdir.to_path_buf());
    config = config.with_write_mode(mode.clone());
    config
}

/// Run one attempt; a panic anywhere inside the engine is contained and reported
/// as a crash (that's the point of the panic-action sweep).
async fn attempt<S, D>(workdir: &Path, source: S, dest: &D, mode: &WriteMode) -> Result<(), String>
where
    S: Source + 'static,
    D: Destination + Clone,
{
    let engine = Engine::new(config(workdir, mode), source, dest.clone());
    match tokio::spawn(engine.run()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(join) => Err(format!("panicked: {join}")),
    }
}

/// The sweep core: (point × action) → crash, crash-during-recovery, recover,
/// count. `expected_fired` pins which points MUST actually fail an armed
/// attempt under this (destination, mode) — the anti-vacuousness instrument
/// (005 review: a sweep that tolerates dead crash points proves nothing).
/// Points outside the pin are mode/destination-unreachable by design.
async fn sweep<S, MS, D, F, C>(
    points: &[&str],
    mode: WriteMode,
    make_source: MS,
    make_dest: F,
    count: C,
    expected_fired: &[&str],
) where
    S: Source + 'static,
    MS: Fn() -> S,
    D: Destination + Clone,
    F: Fn(&Path) -> D,
    C: Fn(&Path, &D) -> u64,
{
    let mut fired: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
    for &point in points {
        // "1*off->return": SKIP the point's first occurrence, fire on the
        // second — crashes BETWEEN commits (e.g. after a Replace table's first
        // truncate+publish landed durably). The continuous actions only ever
        // exercised each boundary's first hit, which is exactly how the
        // Replace-recovery data-loss bug class hid in two destinations.
        for action in ["return", "panic", "1*off->return"] {
            let dir = tempfile::tempdir().expect("tempdir");
            let workdir = dir.path().join("wal");
            let dest = make_dest(dir.path());

            fail::cfg(point, action).expect("configure fail point");
            // First run: dies at the point (or completes if the point is
            // unreachable under this destination/mode — pinned below).
            let armed1 = attempt(&workdir, make_source(), &dest, &mode).await;
            // Second run STILL armed: a crash during recovery itself.
            let armed2 = attempt(&workdir, make_source(), &dest, &mode).await;
            fail::remove(point);
            if armed1.is_err() || armed2.is_err() {
                fired.insert(point);
            }

            let recovered = attempt(&workdir, make_source(), &dest, &mode).await;
            assert!(
                recovered.is_ok(),
                "[{point} / {action} / {mode:?}] recovery failed: {recovered:?}"
            );
            assert_eq!(
                count(dir.path(), &dest),
                TOTAL_ROWS,
                "[{point} / {action} / {mode:?}] exactly-once violated"
            );
        }
    }
    let expected: std::collections::BTreeSet<&str> = expected_fired.iter().copied().collect();
    assert_eq!(
        fired, expected,
        "armed-fire pin diverged for {mode:?}: a missing point means its \
         crash_point! site went dead (vacuous sweep); an extra one means a \
         boundary became reachable — update the pin DELIBERATELY"
    );
}

/// All three shredded write dispositions against the in-memory destination —
/// the substrate that serves every mode (merge-capable by default). The
/// destination survives across the three attempts of an iteration, playing
/// the database server that outlives the crashing engine.
#[tokio::test(flavor = "multi_thread")]
async fn sweep_memory_destination() {
    for mode in [
        WriteMode::Append,
        WriteMode::Replace,
        WriteMode::Merge {
            key: vec!["id".into()],
        },
    ] {
        sweep(
            ENGINE_POINTS,
            mode,
            source,
            |_dir| memory::Destination::new(),
            |_dir, dest| dest.committed_rows("s").len() as u64,
            ENGINE_POINTS,
        )
        .await;
    }
}

/// The durable-storage arm: the reference connector's jsonl destination
/// (the sdk shell, in-process as a durable test double) — part files, an
/// on-disk receipt log and state document, so recovery here replays
/// against REAL durable state rather than shared memory. Append only:
/// the reference destination types-refuses Replace and declares no
/// merge, by design.
#[tokio::test(flavor = "multi_thread")]
async fn sweep_reference_destination() {
    let config_for = |dir: &Path| rdlt_connector_reference::destination::config::Config {
        path: dir.join("out").to_string_lossy().into_owned(),
    };
    sweep(
        ENGINE_POINTS,
        WriteMode::Append,
        source,
        |dir| {
            rdlt_connector_sdk::destination::Shell::<
                rdlt_connector_reference::destination::connector::Reference,
            >::new(config_for(dir))
            .expect("open")
        },
        |dir, _dest| count_reference_rows(&dir.join("out")),
        ENGINE_POINTS,
    )
    .await;
}

/// Rows across the reference destination's published parts for table `s`:
/// `s-<load_id>-<commit_seq>.jsonl`, one row per line. Underscore-prefixed
/// names are bookkeeping (receipts, state, staged temporaries), never data.
fn count_reference_rows(out: &PathBuf) -> u64 {
    let entries = match std::fs::read_dir(out) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return 0,
        Err(error) => panic!("reading {}: {error}", out.display()),
    };
    let mut rows = 0u64;
    for entry in entries {
        let entry = entry.expect("dir entry");
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("s-") && name.ends_with(".jsonl") {
            let text = std::fs::read_to_string(entry.path()).expect("part file");
            rows += text.lines().filter(|line| !line.trim().is_empty()).count() as u64;
        }
    }
    rows
}

/// Gate G2.2: the swept set IS the registry — no silently unswept boundary.
/// The engine's own list lives in THIS file, so the check greps the engine
/// sources for `crash_point!` call sites instead of comparing a const to
/// itself (that would be circular): every site found in src/ must appear in
/// ENGINE_POINTS, count-exact. Connector registries (pq.*, duck.*, pg.* …)
/// are pinned in their own crates' crash_sweep suites, which moved with the
/// crates to the rdlt-connectors repository.
#[test]
fn sweep_covers_entire_registry() {
    // Read the sources, not the const. The scanner is shared with every
    // connector that arms crash points, because a copied scanner fails
    // OPEN — it finds fewer sites and the assertion still passes, so one
    // implementation is the only arrangement where fixing it fixes every user.
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    rdlt_testkit::scanner::assert_registry_matches_sources(&src, &[ENGINE_POINTS]);
}

// ---- The KEYED structured-merge arm under
// the engine's fail points — the shredded sweeps above exercise only the
// identity-merge branch (contract merge-structured.md conformance). The
// merge-capable substrate is the in-memory destination now; the SQL
// destinations' own merge machinery is swept in their crates' suites in
// rdlt-connectors. ----

/// Structured stream with a declared key, resumable by batch index.
struct KeyedArrowSource;

#[async_trait::async_trait]
impl rdlt_connector::source::Source for KeyedArrowSource {
    fn spec(&self) -> rdlt_connector::spec::ConnectorSpec {
        rdlt_connector::spec::ConnectorSpec::new("keyed-arrow-sweep", "0.0.0")
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, rdlt_connector::error::SourceError> {
        Ok(vec![
            StreamSpec::new("s")
                .with_structured()
                .with_primary_key(["id"]),
        ])
    }

    async fn read(
        &self,
        mut req: rdlt_connector::source::ReadRequest,
    ) -> Result<(), rdlt_connector::error::SourceError> {
        use std::sync::Arc;

        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        let start = match &req.since {
            None => 0usize,
            Some(c) => c.as_value().as_u64().unwrap_or(0) as usize,
        };
        for b in start..4 {
            let ids: Vec<i64> = (0..25).map(|i| (b * 25 + i) as i64).collect();
            let names: Vec<String> = ids.iter().map(|i| format!("row-{i}")).collect();
            let batch = rdlt_connector::arrow::RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("name", DataType::Utf8, true),
                ])),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(StringArray::from(
                        names.iter().map(String::as_str).collect::<Vec<_>>(),
                    )),
                ],
            )
            .expect("batch");
            if req.out.arrow(batch).await.is_err() {
                return Ok(());
            }
            if req
                .out
                .checkpoint(rdlt_connector::core::cursor::Cursor::new((b + 1) as u64))
                .await
                .is_err()
            {
                return Ok(());
            }
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sweep_memory_keyed_structured_merge() {
    sweep(
        ENGINE_POINTS,
        WriteMode::Merge {
            key: vec!["id".into()],
        },
        || KeyedArrowSource,
        |_dir| memory::Destination::new(),
        |_dir, dest| dest.committed_rows("s").len() as u64,
        ENGINE_POINTS,
    )
    .await;
}
