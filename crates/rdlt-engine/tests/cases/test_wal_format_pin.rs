//! Pins of the WAL v1 on-disk format, every input synthesized AT TEST TIME
//! (owner rule, 047 round 1: no committed fixtures, no fixture-dir helpers,
//! no RDLT_REPIN machinery — a canonical-bytes pin builds its input from the
//! writer's own API in the test).
//!
//! The format version is 1 until the public release; in the unpublished
//! window format changes land IN PLACE. What that retires is the cross-build
//! committed-fixture oracle — pre-release, byte stability across builds is
//! NOT a commitment, and the property that replaces it is pinned here through
//! the PUBLIC surface: residue this build cannot verify (a versionless
//! manifest, a bare pre-checksum manifest claiming another version number, a
//! checksummed manifest claiming a future version) is refused and recovery
//! degrades to re-extraction — always safe, because the WAL is a replayable
//! buffer, never the source of truth. The envelope itself is pinned by
//! re-deriving it independently of the engine's encoder over a WAL the real
//! writer just produced.

use std::{
    fmt::Write as _,
    path::{Path, PathBuf},
};

use rdlt_connector::source::StreamSpec;
use rdlt_core::commit::CommitPolicy;
use rdlt_core::schema;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::memory;
use serde_json::json;

use super::common::stream_with_batches;
use super::support::crash::{CrashDestination, FaultPoint};
use super::support::scripted;

/// A manifest line, re-derived INDEPENDENTLY of the engine's encoder:
/// `{json}|{blake3-hex-of-the-json-bytes}`. A test computing the envelope by
/// hand is the pin that the shipped encoder cannot drift.
fn manifest_line(json: &str) -> String {
    format!("{json}|{}", blake3::hash(json.as_bytes()).to_hex())
}

/// Split one manifest line into its JSON half, tolerating bare lines.
fn json_of(line: &str) -> &str {
    match line.char_indices().rev().nth(64) {
        Some((split, '|')) => &line[..split],
        _ => line,
    }
}

/// The corpus behind every pin here: nested rows so the segments carry child
/// tables and the full lineage column set, across two checkpointed batches in
/// ONE commit span.
fn corpus_source() -> scripted::Source {
    let batches = vec![
        memory::Batch::new(vec![
            json!({"id": 1, "name": "alpha", "tags": ["x", "y"]}),
            json!({"id": 2, "name": "beta", "tags": []}),
        ])
        .with_checkpoint(json!({"batch": 0})),
        memory::Batch::new(vec![
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
        // Both checkpoints accumulate into one span, so the pins cover a
        // multi-batch, multi-checkpoint replay rather than the smallest one.
        .with_commit_policy(CommitPolicy::every_checkpoints(2))
}

/// Produce a REAL crashed WAL in `workdir` from the writer's own API: run the
/// corpus into a crash-at-commit destination so the whole span stays
/// uncommitted and its WAL survives.
async fn capture_crashed_wal(workdir: &Path) -> PathBuf {
    let dest = CrashDestination::new(memory::Destination::new(), FaultPoint::BeforeCommit(1));
    Engine::new(config(workdir), corpus_source(), dest)
        .run()
        .await
        .expect_err("the capture run must fail at commit, leaving its WAL");
    let wal_dir = workdir.join("wal");
    assert!(
        wal_dir.join("manifest.jsonl").exists(),
        "the crashed run leaves its manifest"
    );
    wal_dir
}

/// Render the destination's committed contents as stable text: one line per
/// row, `table<TAB>row-json`, tables in name order, rows in committed order.
/// `serde_json::Map` renders keys sorted, so the line is deterministic.
fn render_committed(dest: &memory::Destination) -> String {
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
/// REPLAYS a staged WAL, its final checkpoint resumes the source past its last
/// batch, so the destination holds exactly the replayed rows — stamped with
/// the crashed run's `_rdlt_load_id`. When recovery REFUSES it, the source
/// re-extracts from scratch and every row carries the new run's load id
/// instead. The load id is the witness for which path ran.
async fn recover_into_destination(workdir: &Path) -> memory::Destination {
    let dest = memory::Destination::new();
    Engine::new(config(workdir), corpus_source(), dest.clone())
        .run()
        .await
        .expect("recovery run");
    dest
}

/// The `load_id` a staged WAL's `Run` header names — the id replayed rows keep.
fn load_id_of(wal_dir: &Path) -> String {
    let manifest =
        std::fs::read_to_string(wal_dir.join("manifest.jsonl")).expect("staged manifest");
    let header: serde_json::Value =
        serde_json::from_str(json_of(manifest.lines().next().expect("header line")))
            .expect("header json");
    header["load_id"]
        .as_str()
        .expect("staged load_id")
        .to_owned()
}

/// Every committed row's `_rdlt_load_id`, deduplicated.
fn committed_load_ids(dest: &memory::Destination) -> Vec<String> {
    let mut ids: Vec<String> = dest
        .snapshot()
        .values()
        .flatten()
        .filter_map(|row| row.get(schema::system::LOAD_ID))
        .filter_map(|v| v.as_str().map(str::to_owned))
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

/// How a rewritten header is written back: a CURRENT-format line carries a
/// fresh checksum trailer; a bare header models a line no current writer
/// produced.
enum HeaderForm {
    Trailered,
    Bare,
}

/// Rewrite a staged WAL's `Run` header in place.
fn rewrite_header(wal_dir: &Path, form: HeaderForm, rewrite: impl FnOnce(&mut serde_json::Value)) {
    let manifest_path = wal_dir.join("manifest.jsonl");
    let manifest = std::fs::read_to_string(&manifest_path).expect("read staged manifest");
    let mut lines: Vec<String> = manifest.lines().map(str::to_owned).collect();
    let mut header: serde_json::Value =
        serde_json::from_str(json_of(&lines[0])).expect("staged Run header parses");
    rewrite(&mut header);
    lines[0] = match form {
        // Rewriting the JSON invalidates the recorded digest, so the header
        // is re-trailered — the property under test is the VERSION gate, not
        // the checksum gate.
        HeaderForm::Trailered => manifest_line(&header.to_string()),
        HeaderForm::Bare => header.to_string(),
    };
    std::fs::write(&manifest_path, lines.join("\n") + "\n").expect("write staged manifest");
}

/// THE CANONICAL-BYTES PIN, input built from the writer's own API at test
/// time: every line of a freshly written manifest must equal the
/// independently re-derived envelope, the header must stamp `format_version`
/// 1, and segments must be `.arrow` files present beside the manifest.
#[tokio::test]
async fn the_writer_emits_the_v1_envelope_on_every_line() {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal_dir = capture_crashed_wal(&dir.path().join("work")).await;

    let manifest =
        std::fs::read_to_string(wal_dir.join("manifest.jsonl")).expect("captured manifest");
    for line in manifest.lines() {
        let json = json_of(line);
        assert_ne!(json, line, "every line carries a checksum trailer: {line}");
        assert_eq!(
            manifest_line(json),
            line,
            "the trailer is blake3 over exactly the JSON bytes"
        );
    }
    let header: serde_json::Value =
        serde_json::from_str(json_of(manifest.lines().next().expect("header line")))
            .expect("header json");
    assert_eq!(header["rec"], "run");
    assert_eq!(
        header["format_version"], 1,
        "the format version is 1 until the public release; pre-release format \
         changes land in place — this assertion moves WITH a deliberate change, \
         never silently"
    );
    let segment = manifest
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(json_of(l)).ok())
        .find(|r| r["rec"] == "segment")
        .expect("the captured span holds at least one segment record");
    let file = segment["file"].as_str().expect("segment file name");
    assert!(
        file.ends_with(".arrow"),
        "segments are Arrow IPC files: {file}"
    );
    assert!(
        wal_dir.join(file).exists(),
        "the named segment sits beside the manifest: {file}"
    );
}

/// The replay pin: recovering a real crashed WAL commits exactly the rows a
/// clean run of the same corpus commits — identical content and identity
/// columns, with only the load id differing (replay keeps the CRASHED run's
/// id; that continuity is asserted too). Same-build by construction: the
/// cross-build committed-fixture oracle is retired with the pre-release rule.
#[tokio::test]
async fn a_crashed_wal_replays_to_exactly_a_clean_runs_rows() {
    // The clean control: same corpus, no crash.
    let clean_dir = tempfile::tempdir().expect("tempdir");
    let clean_dest = memory::Destination::new();
    Engine::new(
        config(&clean_dir.path().join("work")),
        corpus_source(),
        clean_dest.clone(),
    )
    .run()
    .await
    .expect("clean run");

    // The recovery arm: crash at commit, then recover over the residue.
    let dir = tempfile::tempdir().expect("tempdir");
    let workdir = dir.path().join("work");
    let crashed_load_id = load_id_of(&capture_crashed_wal(&workdir).await);
    let dest = recover_into_destination(&workdir).await;

    assert_eq!(
        committed_load_ids(&dest),
        vec![crashed_load_id],
        "every committed row must come from the REPLAY (the crashed run's load \
         id), not from re-extraction under a fresh id"
    );
    assert!(
        !workdir.join("wal").exists(),
        "a fully recovered WAL is cleared"
    );

    // Content equality modulo the load id: normalize each rendering's single
    // load id to a placeholder and require byte equality — identity columns
    // (`_rdlt_id`, lineage) are content-derived and must survive verbatim.
    let normalize = |dest: &memory::Destination| -> String {
        let ids = committed_load_ids(dest);
        assert_eq!(ids.len(), 1, "one load id per arm: {ids:?}");
        render_committed(dest).replace(&ids[0], "<load-id>")
    };
    let replayed = normalize(&dest);
    assert!(
        replayed.contains(schema::system::ID),
        "replayed rows must carry their persisted identity columns:\n{replayed}"
    );
    assert_eq!(
        replayed,
        normalize(&clean_dest),
        "replaying a crashed span must commit exactly what a clean run commits"
    );
}

/// A manifest whose Run header carries NO version field is not a recognized
/// manifest (every writer stamps the field): it lands on the corruption arms
/// and recovery degrades to re-extraction — none of its rows may surface.
#[tokio::test]
async fn a_versionless_manifest_is_unrecognized_never_replayed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let workdir = dir.path().join("work");
    let wal_dir = capture_crashed_wal(&workdir).await;
    let crashed_load_id = load_id_of(&wal_dir);
    rewrite_header(&wal_dir, HeaderForm::Bare, |header| {
        header
            .as_object_mut()
            .expect("run header is an object")
            .remove("format_version");
    });

    assert_refused_and_reextracted(&workdir, &crashed_load_id).await;
}

/// THE DEV-WINDOW DEGRADE PIN, synthesized INLINE: a manifest of bare
/// pre-checksum lines whose header claims another version number refuses by
/// VERSION and degrades to re-extraction — slower, never wrong, and never a
/// replay of lines this build cannot verify. The WAL is a replayable buffer,
/// never the source of truth, so refusing it loses nothing.
#[tokio::test]
async fn a_bare_manifest_claiming_another_version_is_refused_never_replayed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let workdir = dir.path().join("work");
    let wal_dir = workdir.join("wal");
    std::fs::create_dir_all(&wal_dir).expect("wal dir");
    std::fs::write(wal_dir.join(".rdlt-wal"), b"rdlt-wal-v1\n").expect("ownership marker");
    // Hand-built bare lines: the shape a pre-checksum dev-window writer left.
    std::fs::write(
        wal_dir.join("manifest.jsonl"),
        concat!(
            "{\"rec\":\"run\",\"format_version\":2,\"load_id\":\"stale-dev\",",
            "\"pipeline\":\"wal-format-pin\"}\n",
            "{\"rec\":\"checkpoint\",\"stream\":\"items\",\"cursor\":{\"batch\":0}}\n",
        ),
    )
    .expect("write bare manifest");
    std::fs::write(
        wal_dir.join("rules.json"),
        serde_json::to_vec(&rdlt_core::schema::IdentRules::default()).expect("rules json"),
    )
    .expect("write rules sidecar");

    assert_refused_and_reextracted(&workdir, "stale-dev").await;
}

/// A checksummed manifest claiming a FUTURE version was written by an engine
/// whose records cannot be trusted to mean what this build thinks they mean.
/// Same refusal, same degradation — exact-match in both directions.
#[tokio::test]
async fn a_future_version_manifest_is_refused_never_replayed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let workdir = dir.path().join("work");
    let wal_dir = capture_crashed_wal(&workdir).await;
    let crashed_load_id = load_id_of(&wal_dir);
    rewrite_header(&wal_dir, HeaderForm::Trailered, |header| {
        header["format_version"] = json!(2);
    });

    assert_refused_and_reextracted(&workdir, &crashed_load_id).await;
}

/// The refusal contract, observed through the public surface: the run still
/// SUCCEEDS (degradation to re-extraction is slower, never wrong), the rows
/// arrive under the NEW run's load id — proof no refused segment replayed —
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
        "no row may carry the refused manifest's load id — that would mean an \
         unverifiable span was replayed: {ids:?}"
    );
    assert!(
        !workdir.join("wal").exists(),
        "a refused WAL is cleared so it cannot re-trip every later run"
    );
}
