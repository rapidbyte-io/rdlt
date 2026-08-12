//! The reference connector answers the same kits every shipping
//! connector answers, plus its own exactly-once pins: the byte cursor's
//! resume law over an unchanged, grown, and shrunk file, and the
//! receipt-driven replay that keeps a crashed load from duplicating.

use rdlt_connector_reference::{destination, source};
use rdlt_connector_sdk::spi::core::{CommitReceipt, LoadId, PipelineId, TableName, WriteMode};
use rdlt_connector_sdk::spi::{Destination, OpenContext, Source};
use rdlt_testkit::{
    TableProbe, assert_conformant, batch_of, commit_meta_for, schema_for, verify_destination,
    verify_source,
};
use serde_json::json;

use super::common::{DirProbe, read_stream};

/// Three seed rows, 8 bytes per line (`{"n":1}` + newline) — the byte
/// offsets the cursor pins below are derived from this shape.
const SEED: &str = "{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n";

/// A tempdir holding `events.jsonl` seeded with [`SEED`], plus a source
/// shell over it.
fn seeded_source() -> (tempfile::TempDir, std::path::PathBuf, source::Shell) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("events.jsonl");
    std::fs::write(&path, SEED).expect("seed file");
    let shell = source::Shell::from_value(json!({"path": path})).expect("valid config");
    (dir, path, shell)
}

/// The source kit: deterministic reads, the resume law over every
/// checkpoint, cancellation — certified against the same clauses every
/// shipping source answers, with no skips tolerated.
#[tokio::test]
async fn the_source_kit_certifies_the_shell() {
    let (_dir, _path, shell) = seeded_source();
    assert_eq!(shell.spec().name, "io.rapidbyte.reference");
    assert_conformant(verify_source(&shell).await.expecting_no_skips());
}

/// The destination kit: staging invisibility, atomic state, idempotent
/// re-commit, crashed-session teardown — certified with no skips (D8
/// is never asserted: this destination declares `merge = false`).
#[tokio::test]
async fn the_destination_kit_certifies_the_shell() {
    let dir = tempfile::tempdir().expect("tempdir");
    let shell = destination::Shell::from_value(json!({"path": dir.path()})).expect("valid config");
    assert_eq!(shell.spec().name, "io.rapidbyte.reference");
    let probe = DirProbe(dir.path().to_path_buf());
    assert_conformant(
        verify_destination(&shell, &probe)
            .await
            .expecting_no_skips(),
    );
}

/// The exactly-once pin: a committed cursor at EOF means a re-run of an
/// unchanged file reads NOTHING again. Also pins the persisted v1 wire
/// shape — `{"v":1,"bytes_read":<u64>}` — as data, not just behavior.
#[tokio::test]
async fn a_second_read_of_an_unchanged_file_yields_zero_rows() {
    let (_dir, _path, shell) = seeded_source();
    let stream = shell.streams().await.expect("streams").remove(0);
    assert_eq!(
        stream.name.as_str(),
        "events",
        "the stream is named by the file stem"
    );

    let (rows, checkpoint) = read_stream(&shell, &stream, None).await.expect("full read");
    assert_eq!(rows.len(), 3);
    let cursor = checkpoint.expect("the read checkpoints");
    assert_eq!(cursor.as_value(), &json!({"v": 1, "bytes_read": 24}));

    let (rows, _) = read_stream(&shell, &stream, Some(cursor))
        .await
        .expect("resumed read");
    assert!(
        rows.is_empty(),
        "an unchanged file re-read from its committed cursor must yield zero rows, got {rows:?}"
    );
}

/// A file that grew since the committed cursor yields ONLY the tail —
/// the appended rows, nothing re-read.
#[tokio::test]
async fn a_grown_file_yields_only_the_tail() {
    let (_dir, path, shell) = seeded_source();
    let stream = shell.streams().await.expect("streams").remove(0);
    let (_, checkpoint) = read_stream(&shell, &stream, None).await.expect("full read");
    let cursor = checkpoint.expect("the read checkpoints");

    let mut grown = std::fs::read_to_string(&path).expect("read back");
    grown.push_str("{\"n\":4}\n");
    std::fs::write(&path, grown).expect("grow file");

    let (rows, checkpoint) = read_stream(&shell, &stream, Some(cursor))
        .await
        .expect("tail read");
    assert_eq!(rows, vec![json!({"n": 4})], "only the appended tail");
    assert_eq!(
        checkpoint.expect("the tail read checkpoints").as_value(),
        &json!({"v": 1, "bytes_read": 32})
    );
}

/// A file that SHRANK below the committed cursor is a typed refusal
/// with the frozen spelling — never a silent re-read or a guess.
#[tokio::test]
async fn a_shrunk_file_refuses_with_the_frozen_spelling() {
    let (_dir, path, shell) = seeded_source();
    let stream = shell.streams().await.expect("streams").remove(0);
    let (_, checkpoint) = read_stream(&shell, &stream, None).await.expect("full read");
    let cursor = checkpoint.expect("the read checkpoints");

    std::fs::write(&path, "{\"n\":1}\n").expect("shrink file");

    let refused = read_stream(&shell, &stream, Some(cursor))
        .await
        .expect_err("a shrunk file must refuse");
    assert_eq!(
        refused.to_string(),
        format!(
            "fatal source error: reference source: {} shrank below the cursor (24 > 8): \
             refusing to guess",
            path.display()
        )
    );
}

/// The receipt log drives the sdk choreography: re-committing a load's
/// already-published unit returns the prior receipt, republishes
/// nothing, and clears the redelivered staging so no LATER commit
/// publishes it either.
#[tokio::test]
async fn a_replayed_load_id_does_not_duplicate_rows() {
    let dir = tempfile::tempdir().expect("tempdir");
    let shell = destination::Shell::from_value(json!({"path": dir.path()})).expect("valid config");
    let probe = DirProbe(dir.path().to_path_buf());
    let pipeline = PipelineId::new("ref-replay");
    let load = LoadId::new("ref-load-1");
    let table = TableName::new("events");
    let schema = schema_for("events");

    let mut session = shell
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    session
        .ensure_table(&schema, &WriteMode::Append)
        .await
        .expect("ensure");
    session
        .write(&table, batch_of(&[1, 2]))
        .await
        .expect("write");
    let receipt = session
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");
    assert_eq!(probe.count(&table).await.expect("count"), 2);
    // The crash: the session dies uncommitted-of-nothing, and the SAME
    // load re-attempts — the engine redelivers the unit it never saw
    // acknowledged.
    drop(session);

    let mut session = shell
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("re-open");
    session
        .ensure_table(&schema, &WriteMode::Append)
        .await
        .expect("re-ensure");
    session
        .write(&table, batch_of(&[1, 2]))
        .await
        .expect("redelivered write");
    let replayed = session
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("replayed commit");
    assert_eq!(replayed, receipt, "the replay returns the PRIOR receipt");
    assert_eq!(
        probe.count(&table).await.expect("count"),
        2,
        "a replayed load id must not duplicate rows"
    );

    // The replay hook's other half: the redelivered staging was cleared,
    // so the next genuine commit publishes nothing it shouldn't.
    session
        .commit(commit_meta_for(&pipeline, &load, 2))
        .await
        .expect("next commit");
    assert_eq!(
        probe.count(&table).await.expect("count"),
        2,
        "redelivered staging must not leak into a later commit"
    );
}

/// The config gate's refusals, full-string: the two documents share the
/// one-field shape, reject unknown keys, and refuse an empty path with
/// their own frozen wording.
#[test]
fn the_config_gate_refuses_with_frozen_spellings() {
    let refused = source::Shell::from_value(json!({"path": ""})).unwrap_err();
    assert_eq!(
        refused.to_string(),
        "invalid reference source config: `path` is empty — one jsonl file is required"
    );
    let refused = source::Shell::from_value(json!({"path": "/"})).unwrap_err();
    assert_eq!(
        refused.to_string(),
        "invalid reference source config: `/` has no file stem to name the stream"
    );
    let refused = source::Shell::from_value(json!({"path": "a.jsonl", "glob": "*"})).unwrap_err();
    assert_eq!(
        refused.to_string(),
        "invalid reference source JSON: unknown field `glob`, expected `path`"
    );
    let refused = destination::Shell::from_value(json!({"path": ""})).unwrap_err();
    assert_eq!(
        refused.to_string(),
        "invalid reference destination config: `path` is empty — one output directory is required"
    );
}

/// Replace is typed-unsupported, recorded never silent: accepting it
/// would append where the pipeline asked for the table's contents to
/// be replaced, quietly forever.
#[tokio::test]
async fn a_replace_write_mode_refuses_with_the_frozen_spelling() {
    let dir = tempfile::tempdir().expect("tempdir");
    let shell = destination::Shell::from_value(json!({"path": dir.path()})).expect("valid config");
    let mut session = shell
        .open(OpenContext::new(
            PipelineId::new("ref-replace"),
            LoadId::new("ref-load-r"),
        ))
        .await
        .expect("open");
    let refused = session
        .ensure_table(&schema_for("events"), &WriteMode::Replace)
        .await
        .expect_err("replace must refuse");
    assert_eq!(
        refused.to_string(),
        "fatal destination error: reference destination: table `events`: write mode `replace` \
         is not supported — jsonl parts are append-only"
    );
}

/// A receipt whose append TORE mid-write (its line never got the
/// terminating newline) never became durable: it reads as ABSENT, the
/// retried commit republishes over its deterministic part names
/// without duplicating, and the repaired log carries exactly the
/// durable receipt line — no glued garbage for a later read to refuse.
#[tokio::test]
async fn a_torn_receipt_tail_reads_as_absent_and_republishes_without_duplication() {
    let dir = tempfile::tempdir().expect("tempdir");
    let shell = destination::Shell::from_value(json!({"path": dir.path()})).expect("valid config");
    let probe = DirProbe(dir.path().to_path_buf());
    let pipeline = PipelineId::new("ref-torn");
    let load = LoadId::new("ref-load-t");
    let table = TableName::new("events");
    let schema = schema_for("events");

    let mut session = shell
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    session
        .ensure_table(&schema, &WriteMode::Append)
        .await
        .expect("ensure");
    session
        .write(&table, batch_of(&[1, 2]))
        .await
        .expect("write");
    session
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");
    drop(session);

    // The tear: the append died mid-line, before its newline landed.
    let log_path = dir.path().join("_reference_receipts.json");
    let log = std::fs::read_to_string(&log_path).expect("read log");
    std::fs::write(&log_path, &log[..log.len() - 5]).expect("tear the tail");

    let mut session = shell
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("re-open");
    session
        .ensure_table(&schema, &WriteMode::Append)
        .await
        .expect("re-ensure");
    session
        .write(&table, batch_of(&[1, 2]))
        .await
        .expect("redelivered write");
    let receipt = session
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("the torn receipt reads as absent, so this commit republishes");
    assert_eq!(
        probe.count(&table).await.expect("count"),
        2,
        "the republish overwrites its own deterministic part — never duplicates"
    );
    let repaired = std::fs::read_to_string(&log_path).expect("read repaired log");
    assert_eq!(
        repaired,
        format!("{}\n", serde_json::to_string(&receipt).expect("encode")),
        "the torn bytes are gone and exactly the durable receipt line remains"
    );

    // The republished receipt IS durable: a further re-commit replays.
    session
        .write(&table, batch_of(&[1, 2]))
        .await
        .expect("second redelivery");
    let replayed = session
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("replayed commit");
    assert_eq!(replayed, receipt);
    assert_eq!(probe.count(&table).await.expect("count"), 2);
}

/// Mid-log corruption is NOT a torn append: an unparseable line that
/// IS newline-terminated refuses typed, full-string — including the
/// parser's own framing, reproduced rather than transcribed.
#[tokio::test]
async fn a_corrupt_interior_receipt_line_still_refuses() {
    let dir = tempfile::tempdir().expect("tempdir");
    let shell = destination::Shell::from_value(json!({"path": dir.path()})).expect("valid config");
    let log_path = dir.path().join("_reference_receipts.json");
    std::fs::write(&log_path, "not a receipt\n").expect("seed corruption");

    let mut session = shell
        .open(OpenContext::new(
            PipelineId::new("ref-corrupt"),
            LoadId::new("ref-load-c"),
        ))
        .await
        .expect("open");
    session
        .ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    session
        .write(&TableName::new("events"), batch_of(&[1]))
        .await
        .expect("write");
    let refused = session
        .commit(commit_meta_for(
            &PipelineId::new("ref-corrupt"),
            &LoadId::new("ref-load-c"),
            1,
        ))
        .await
        .expect_err("a corrupt interior line must refuse");
    let parse_error =
        serde_json::from_str::<CommitReceipt>("not a receipt").expect_err("not a receipt json");
    assert_eq!(
        refused.to_string(),
        format!(
            "fatal destination error: reference destination: {} carries a corrupt receipt line \
             `not a receipt`: {parse_error}",
            log_path.display()
        )
    );
}
