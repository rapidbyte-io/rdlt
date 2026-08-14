//! Byte-exact pin of the WAL v3 on-disk format: a committed fixture (manifest
//! with per-line blake3 trailers plus Arrow IPC file segments) that every
//! future build must replay to identical rows and identical `_rdlt_id`s.
//!
//! The WAL format is PERSISTED: a process that dies mid-run leaves v3 bytes on
//! disk, and whatever build starts next — possibly a newer one — must replay
//! them. Every other WAL test in this crate writes and reads with the SAME
//! build, so a format drift (a serde rename, a container change, an encoding
//! default) keeps all of them green while breaking real recovery across an
//! upgrade. This fixture is the only cross-build oracle: it was captured from
//! the shipping build and is decoded literally.
//!
//! The version gate is pinned in ALL directions through the PUBLIC surface: a
//! v1 (headerless) manifest, the REAL committed v2 fixture (047 M4's bump left
//! genuine v2 residue in the world), and a v4 manifest are all refused —
//! recovery degrades to re-extraction and no refused fixture's rows may reach
//! the destination.
//!
//! Regenerate deliberately, never reflexively — a diff here means WAL residue
//! written by shipped builds no longer replays on this one:
//!
//! ```text
//! RDLT_REPIN=1 cargo nextest run -p rdlt-engine --test integration -E 'test(wal_format_pin)'
//! ```

use std::{
    fmt::Write as _,
    path::{Path, PathBuf},
};

use rdlt_connector::StreamSpec;
use rdlt_core::{CommitPolicy, schema::system_columns};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{CrashDestination, FaultPoint, MemoryBatch, MemoryDestination, MemorySource};
use serde_json::json;

use super::common::stream_with_batches;

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/wal_v3")
}

/// The RETIRED format's committed fixture, kept verbatim from before the
/// v2→v3 bump — the realest possible "surviving v2 WAL" for the degrade pin.
fn v2_fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/wal_v2")
}

/// A v3 manifest line, re-derived INDEPENDENTLY of the engine's encoder:
/// `{json}|{blake3-hex-of-the-json-bytes}`. Tests computing the envelope by
/// hand are the pin that the shipped encoder cannot drift.
fn v3_line(json: &str) -> String {
    format!("{json}|{}", blake3::hash(json.as_bytes()).to_hex())
}

/// Split one manifest line into its JSON half, tolerating the trailer-less
/// v1/v2 fixtures.
fn json_of(line: &str) -> &str {
    match line.char_indices().rev().nth(64) {
        Some((split, '|')) => &line[..split],
        _ => line,
    }
}

/// The corpus behind the fixture: nested rows so the segments carry child
/// tables and the full lineage column set, across two checkpointed batches in
/// ONE commit span.
fn corpus_source() -> MemorySource {
    let batches = vec![
        MemoryBatch::new(vec![
            json!({"id": 1, "name": "alpha", "tags": ["x", "y"]}),
            json!({"id": 2, "name": "beta", "tags": []}),
        ])
        .with_checkpoint(json!({"batch": 0})),
        MemoryBatch::new(vec![
            json!({"id": 3, "name": "gamma", "tags": ["z"]}),
            json!({"id": 4, "name": "delta"}),
        ])
        .with_checkpoint(json!({"batch": 1})),
    ];
    stream_with_batches(StreamSpec::new("items"), batches)
}

fn config(workdir: &Path) -> EngineConfig {
    EngineConfig::new("wal-format-pin")
        .with_workdir(workdir.to_path_buf())
        // Both checkpoints accumulate into one span, so the fixture pins a
        // multi-batch, multi-checkpoint replay rather than the smallest one.
        .with_commit_policy(CommitPolicy::every_checkpoints(2))
}

/// Copy every WAL file (manifest + segments) between directories, replacing
/// the destination's previous contents.
fn copy_wal(from: &Path, to: &Path) {
    if to.exists() {
        std::fs::remove_dir_all(to).expect("clear stale fixture");
    }
    std::fs::create_dir_all(to).expect("create fixture dir");
    for entry in std::fs::read_dir(from).expect("read wal dir") {
        let entry = entry.expect("dir entry");
        std::fs::copy(entry.path(), to.join(entry.file_name())).expect("copy wal file");
    }
}

/// Render the destination's committed contents as stable text: one line per
/// row, `table<TAB>row-json`, tables in name order, rows in committed order.
/// `serde_json::Map` renders keys sorted, so the line is deterministic.
fn render_committed(dest: &MemoryDestination) -> String {
    let mut out = String::new();
    for (table, rows) in dest.snapshot() {
        for row in rows {
            writeln!(out, "{table}\t{}", serde_json::Value::Object(row.clone()))
                .expect("render row");
        }
    }
    out
}

/// Run a fresh engine over the corpus source against `workdir`. When recovery
/// REPLAYS the fixture, its final checkpoint resumes the source past its last
/// batch, so the destination holds exactly the replayed rows — stamped with
/// the fixture's original `_rdlt_load_id`. When recovery REFUSES the fixture,
/// the source re-extracts from scratch and every row carries the new run's
/// load id instead. The load id is the witness for which path ran.
async fn recover_into_destination(workdir: &Path) -> MemoryDestination {
    let dest = MemoryDestination::new();
    Engine::new(config(workdir), corpus_source(), dest.clone())
        .run()
        .await
        .expect("recovery run");
    dest
}

/// The `load_id` a fixture's `Run` header names — the id replayed rows keep.
fn load_id_of(fixture: &Path) -> String {
    let manifest =
        std::fs::read_to_string(fixture.join("manifest.jsonl")).expect("fixture manifest");
    let header: serde_json::Value =
        serde_json::from_str(json_of(manifest.lines().next().expect("header line")))
            .expect("header json");
    header["load_id"]
        .as_str()
        .expect("fixture load_id")
        .to_owned()
}

fn fixture_load_id() -> String {
    load_id_of(&fixture_dir())
}

/// Every committed row's `_rdlt_load_id`, deduplicated.
fn committed_load_ids(dest: &MemoryDestination) -> Vec<String> {
    let mut ids: Vec<String> = dest
        .snapshot()
        .values()
        .flatten()
        .filter_map(|row| row.get(system_columns::LOAD_ID))
        .filter_map(|v| v.as_str().map(str::to_owned))
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

/// How a staged header is written back: a CURRENT-format line carries a fresh
/// checksum trailer; an older format's header never had one.
enum HeaderForm {
    Trailered,
    Bare,
}

/// Stage a fixture into a fresh workdir's `wal/` directory, optionally
/// rewriting the manifest's `Run` header first.
fn stage_fixture(
    fixture: &Path,
    workdir: &Path,
    form: HeaderForm,
    rewrite_header: impl FnOnce(&mut serde_json::Value),
) {
    let wal_dir = workdir.join("wal");
    copy_wal(fixture, &wal_dir);
    // Recovery refuses a manifest without its rules sidecar. The
    // sidecar is a workdir file, not part of the frozen WAL stream this
    // fixture pins, so it is staged here rather than committed with
    // the fixture; a future RDLT_REPIN capture carries its own.
    std::fs::write(
        wal_dir.join("rules.json"),
        serde_json::to_vec(&rdlt_core::naming::IdentRules::default()).expect("rules json"),
    )
    .expect("write rules sidecar");
    let manifest_path = wal_dir.join("manifest.jsonl");
    let manifest = std::fs::read_to_string(&manifest_path).expect("read fixture manifest");
    let mut lines: Vec<String> = manifest.lines().map(str::to_owned).collect();
    let mut header: serde_json::Value =
        serde_json::from_str(json_of(&lines[0])).expect("fixture Run header parses");
    rewrite_header(&mut header);
    lines[0] = match form {
        // Rewriting the JSON invalidates the recorded digest, so the header
        // is re-trailered — the property under test is the VERSION gate, not
        // the checksum gate.
        HeaderForm::Trailered => v3_line(&header.to_string()),
        HeaderForm::Bare => header.to_string(),
    };
    std::fs::write(&manifest_path, lines.join("\n") + "\n").expect("write manifest");
}

/// The pin. Under `RDLT_REPIN=1` the fixture and its expected rendering are
/// regenerated from THIS build; otherwise the committed fixture must replay to
/// the committed rendering, byte for byte.
#[tokio::test]
async fn a_v3_wal_replays_to_identical_rows_and_ids() {
    let expected_path = fixture_dir().join("expected_rows.txt");

    if std::env::var_os("RDLT_REPIN").is_some() {
        // Capture: run the corpus into a crash-at-commit destination so the
        // whole span stays uncommitted, then keep its WAL as the fixture.
        let dir = tempfile::tempdir().expect("tempdir");
        let workdir = dir.path().join("work");
        let dest = CrashDestination::new(MemoryDestination::new(), FaultPoint::BeforeCommit(1));
        Engine::new(config(&workdir), corpus_source(), dest)
            .run()
            .await
            .expect_err("the capture run must fail at commit, leaving its WAL");
        copy_wal(&workdir.join("wal"), &fixture_dir());
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let workdir = dir.path().join("work");
    stage_fixture(&fixture_dir(), &workdir, HeaderForm::Trailered, |_| {});
    let dest = recover_into_destination(&workdir).await;
    let actual = render_committed(&dest);

    assert!(
        actual.contains(system_columns::ID),
        "replayed rows must carry their persisted identity columns:\n{actual}"
    );
    assert_eq!(
        committed_load_ids(&dest),
        vec![fixture_load_id()],
        "every committed row must come from the REPLAY (the fixture's load id), \
         not from re-extraction under a fresh id"
    );
    assert!(
        !workdir.join("wal").exists(),
        "a fully recovered WAL is cleared"
    );

    if std::env::var_os("RDLT_REPIN").is_some() {
        std::fs::write(&expected_path, &actual).expect("write expected rendering");
        return;
    }
    let expected = std::fs::read_to_string(&expected_path).unwrap_or_else(|e| {
        panic!(
            "missing expected rendering {expected_path:?}: {e}; \
             regenerate with RDLT_REPIN=1 and review every line"
        )
    });
    assert_eq!(
        actual, expected,
        "the committed v3 fixture no longer replays to its committed rows — \
         WAL residue from shipped builds would be misread; if this change is \
         deliberate it needs a format-version bump, not a re-pin"
    );
}

/// The fixture itself must BE a v3 manifest — versioned header, a checksum
/// trailer on EVERY line, Arrow IPC segments — otherwise the replay pin above
/// is testing something else entirely.
#[test]
fn the_fixture_is_a_v3_manifest_with_checksummed_lines_and_arrow_segments() {
    let manifest =
        std::fs::read_to_string(fixture_dir().join("manifest.jsonl")).expect("fixture manifest");
    for line in manifest.lines() {
        let json = json_of(line);
        assert_ne!(
            json, line,
            "every v3 line carries a checksum trailer: {line}"
        );
        assert_eq!(
            v3_line(json),
            line,
            "the trailer is blake3 over exactly the JSON bytes"
        );
    }
    let header: serde_json::Value =
        serde_json::from_str(json_of(manifest.lines().next().expect("header line")))
            .expect("header json");
    assert_eq!(header["rec"], "run");
    assert_eq!(
        header["format_version"], 3,
        "the fixture pins WAL format v3; a bump regenerates the fixture DELIBERATELY"
    );
    let segment = manifest
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(json_of(l)).ok())
        .find(|r| r["rec"] == "segment")
        .expect("the fixture holds at least one segment record");
    let file = segment["file"].as_str().expect("segment file name");
    assert!(
        file.ends_with(".arrow"),
        "v3 segments are Arrow IPC files: {file}"
    );
    assert!(
        fixture_dir().join(file).exists(),
        "the named segment is committed alongside the manifest: {file}"
    );
}

/// A v1 manifest — one whose `Run` header predates the version field — names
/// parquet segments this build cannot read. It must be refused by VERSION and
/// recovery must degrade to re-extraction: none of its rows may surface.
#[tokio::test]
async fn a_v1_headerless_manifest_is_refused_never_replayed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let workdir = dir.path().join("work");
    // A v1 header is BARE: the version field did not exist, and neither did
    // the checksum trailer.
    stage_fixture(&fixture_dir(), &workdir, HeaderForm::Bare, |header| {
        header
            .as_object_mut()
            .expect("run header is an object")
            .remove("format_version");
    });

    assert_refused_and_reextracted(&workdir, &fixture_load_id()).await;
}

/// THE v2→v3 DEGRADE PIN (047 M4): the REAL committed v2 fixture — trailer-
/// less lines from the shipping v2 build — must refuse by VERSION and degrade
/// to re-extraction. This is the acceptable cost of the bump, and the same
/// degradation the v1→v2 transition took: slower, never wrong, and never a
/// replay of lines this build cannot verify.
#[tokio::test]
async fn a_v2_wal_is_refused_never_replayed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let workdir = dir.path().join("work");
    stage_fixture(&v2_fixture_dir(), &workdir, HeaderForm::Bare, |header| {
        assert_eq!(
            header["format_version"], 2,
            "the retired fixture must still be the v2 capture"
        );
    });

    assert_refused_and_reextracted(&workdir, &load_id_of(&v2_fixture_dir())).await;
}

/// A v4 manifest was written by a FUTURE engine; its records cannot be trusted
/// to mean what this build thinks they mean. Same refusal, same degradation.
#[tokio::test]
async fn a_v4_manifest_is_refused_never_replayed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let workdir = dir.path().join("work");
    stage_fixture(&fixture_dir(), &workdir, HeaderForm::Trailered, |header| {
        header["format_version"] = json!(4);
    });

    assert_refused_and_reextracted(&workdir, &fixture_load_id()).await;
}

/// The refusal contract, observed through the public surface: the run still
/// SUCCEEDS (degradation to re-extraction is slower, never wrong), the rows
/// arrive under the NEW run's load id — proof no fixture segment replayed —
/// and the refused WAL is cleared.
async fn assert_refused_and_reextracted(workdir: &Path, refused_load_id: &str) {
    let dest = recover_into_destination(workdir).await;
    let ids = committed_load_ids(&dest);
    assert!(
        !dest.snapshot().is_empty(),
        "degradation must re-extract, not lose the data"
    );
    assert!(
        !ids.iter().any(|id| id == refused_load_id),
        "no row may carry the refused fixture's load id — that would mean a \
         segment in an unsupported format was replayed: {ids:?}"
    );
    assert!(
        !workdir.join("wal").exists(),
        "a refused WAL is cleared so it cannot re-trip every later run"
    );
}
