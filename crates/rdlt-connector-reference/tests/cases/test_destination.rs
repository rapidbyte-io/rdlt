//! The destination's own exactly-once pins: the receipt-driven replay
//! that keeps a crashed load from duplicating, the retried publish
//! over intact staging, the one-pipeline state slot, the write-mode
//! refusals, the session lease, torn and corrupt receipt logs, and the
//! part-name gate.

use rdlt_connector_reference::destination::config::Config;
use rdlt_connector_reference::destination::connector::Reference;
use rdlt_connector_sdk::config::Document;
use rdlt_connector_sdk::destination::Shell;
use rdlt_connector_sdk::spi::core::commit::{CommitReceipt, WriteMode};
use rdlt_connector_sdk::spi::core::id::{LoadId, PipelineId, TableName};
use rdlt_connector_sdk::spi::destination::{Destination, OpenContext};
use rdlt_testkit::conformance::destination::TableProbe;
use rdlt_testkit::fixtures::{batch_of, commit_meta_for, schema_for};
use serde_json::json;

use super::support::DirProbe;

/// The sdk shell over `dir` — the SPI face this crate's tests drive
/// in-process.
fn shell_over(dir: &std::path::Path) -> Shell<Reference> {
    Shell::<Reference>::from_value(json!({"path": dir})).expect("valid config")
}

/// The receipt log drives the sdk choreography: re-committing a load's
/// already-published unit returns the prior receipt, republishes
/// nothing, and clears the redelivered staging so no LATER commit
/// publishes it either.
#[tokio::test]
async fn a_replayed_load_id_does_not_duplicate_rows() {
    let dir = tempfile::tempdir().expect("tempdir");
    let shell = shell_over(dir.path());
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

/// A transiently failed publish leaves staging INTACT, so retrying the
/// SAME commit on the same session re-persists every row. The
/// drain-before-persist shape this pins against was silent loss: the
/// failed attempt emptied staging, and the retry then published ZERO
/// parts yet still wrote state and appended the receipt — after which
/// `existing_receipt` vouched for a commit whose rows were partially
/// absent, forever.
#[tokio::test]
async fn a_retried_publish_after_a_transient_failure_re_persists_the_rows() {
    let dir = tempfile::tempdir().expect("tempdir");
    let shell = shell_over(dir.path());
    let probe = DirProbe(dir.path().to_path_buf());
    let pipeline = PipelineId::new("ref-retry");
    let load = LoadId::new("ref-load-retry");
    // Two tables, and publish walks them in name order — so a blocker
    // under the SECOND name lands the failure MID-publish: after
    // `aa_events` persisted, before `zz_events` could.
    let first = TableName::new("aa_events");
    let second = TableName::new("zz_events");

    let mut session = shell
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    session
        .ensure_table(&schema_for("aa_events"), &WriteMode::Append)
        .await
        .expect("ensure first");
    session
        .ensure_table(&schema_for("zz_events"), &WriteMode::Append)
        .await
        .expect("ensure second");
    session
        .write(&first, batch_of(&[1, 2]))
        .await
        .expect("write first");
    session
        .write(&second, batch_of(&[3, 4, 5]))
        .await
        .expect("write second");

    // The injected IO failure: a directory squatting the part's staging
    // path makes its file creation fail — a transient refusal, exactly
    // the disk-full class mid-publish failures classify as. The staged
    // part name carries the injective tuple digest — recomputed here so
    // this pin also freezes the naming algorithm.
    let digest = {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"rdlt-reference:part:v1\0");
        for field in ["zz_events".as_bytes(), "ref-load-retry".as_bytes()] {
            hasher.update(&(field.len() as u64).to_le_bytes());
            hasher.update(field);
        }
        hasher.update(&1u64.to_le_bytes());
        hasher.finalize().to_hex()[..8].to_owned()
    };
    let blocker = dir
        .path()
        .join(format!("_staged-zz_events-ref-load-retry-1-{digest}.jsonl"));
    std::fs::create_dir(&blocker).expect("blocker dir");

    let refused = session
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect_err("the blocked part write must fail the commit");
    assert!(
        refused
            .to_string()
            .starts_with("transient destination error: reference destination: write "),
        "expected the transient write refusal, got: {refused}"
    );
    assert!(
        !dir.path().join("_reference_receipts.json").exists(),
        "a failed publish must never leave a receipt behind"
    );

    // The client retries the SAME commit WITHOUT re-writing — the shape
    // a transient classification invites.
    std::fs::remove_dir(&blocker).expect("unblock");
    session
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("the retried commit re-persists from intact staging");
    assert_eq!(probe.count(&first).await.expect("count"), 2);
    assert_eq!(
        probe.count(&second).await.expect("count"),
        3,
        "the retry must re-persist the rows the failed attempt staged"
    );

    // Staging cleared on the SUCCESSFUL publish only: the next commit
    // has nothing left to double-publish.
    session
        .commit(commit_meta_for(&pipeline, &load, 2))
        .await
        .expect("next commit");
    assert_eq!(probe.count(&first).await.expect("count"), 2);
    assert_eq!(
        probe.count(&second).await.expect("count"),
        3,
        "successfully published staging must not leak into a later commit"
    );
}

/// ONE state slot means ONE pipeline per directory: a second pipeline
/// reading the slot must refuse typed — answering `None` would read as
/// "never committed", so the engine would re-extract from scratch,
/// append every already-loaded row a second time, and the next publish
/// would destroy the first pipeline's cursors.
#[tokio::test]
async fn another_pipelines_state_refuses_rather_than_reading_fresh() {
    let dir = tempfile::tempdir().expect("tempdir");
    let shell = shell_over(dir.path());
    let pipeline_a = PipelineId::new("orders");
    let load = LoadId::new("ref-load-o");
    let mut session = shell
        .open(OpenContext::new(pipeline_a.clone(), load.clone()))
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
    session
        .commit(commit_meta_for(&pipeline_a, &load, 1))
        .await
        .expect("commit");
    drop(session);

    let mut session = shell
        .open(OpenContext::new(
            PipelineId::new("customers"),
            LoadId::new("ref-load-c"),
        ))
        .await
        .expect("open under the second pipeline");
    let refused = session
        .read_state(&PipelineId::new("customers"))
        .await
        .expect_err("a foreign state slot must refuse, never read fresh");
    assert_eq!(
        refused.to_string(),
        format!(
            "fatal destination error: reference destination: {} carries the state of \
             pipeline `orders` — this session is pipeline `customers`, and one directory \
             holds ONE pipeline's state: reading it as fresh would append every \
             already-loaded row again, and the next publish would destroy `orders`' \
             cursors; give each pipeline its own output directory",
            dir.path().join("_reference_state.json").display()
        )
    );
}

/// A trailing slash over an existing FILE must refuse at the probe:
/// `stat("…/file/")` reports NotADirectory, and a walk that steps to
/// `.parent()` skips the file (the path API normalizes the trailing
/// slash away, so the parent is the directory ABOVE it), passing a
/// probe whose `connect` then fails transient — retry bait against a
/// misconfiguration no retry fixes.
#[tokio::test]
async fn check_refuses_a_trailing_slash_over_a_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let occupied = dir.path().join("occupied");
    std::fs::write(&occupied, b"x").expect("seed the file");
    let trailing = format!("{}/", occupied.display());
    let shell = Shell::<Reference>::from_value(json!({"path": trailing})).expect("valid config");
    let refused = shell
        .check()
        .await
        .expect_err("a file behind a trailing slash must refuse, not pass while connect fails");
    let rendered = refused.to_string();
    assert!(
        rendered.starts_with("fatal destination error: ")
            && rendered.contains("is not a directory"),
        "the refusal is fatal and names the occupant: {rendered}"
    );
}

/// A commit whose meta names another load refuses — and the refusal
/// renders BOTH load ids through the bounded diagnostic render: the
/// meta's is wire-authored text a direct Backend driver hands in
/// unbounded, so a hostile one arrives spelled-out and truncated, never
/// raw in the error.
#[tokio::test]
async fn a_wrong_load_publish_refusal_renders_the_load_id_inert() {
    let dir = tempfile::tempdir().expect("tempdir");
    let shell = shell_over(dir.path());
    let pipeline = PipelineId::new("p");
    let mut session = shell
        .open(OpenContext::new(pipeline.clone(), LoadId::new("good")))
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
    let hostile = format!("evil\u{1b}]52;c;A\u{7}{}", "x".repeat(2000));
    let refused = session
        .commit(commit_meta_for(&pipeline, &LoadId::new(hostile), 1))
        .await
        .expect_err("a commit naming another load refuses");
    let rendered = refused.to_string();
    assert!(
        !rendered.contains('\u{1b}') && !rendered.contains('\u{7}'),
        "no raw control byte survives the refusal: {rendered:?}"
    );
    assert!(
        rendered.contains("\\u{1b}") && rendered.contains("truncated from"),
        "the hostile id arrives spelled out and bounded: {rendered}"
    );
}

/// The unsupported-mode refusal quotes the table name — wire-authored
/// for a served backend, unbounded for a direct driver — through the
/// bounded diagnostic render.
#[tokio::test]
async fn an_unsupported_mode_refusal_renders_the_table_name_inert() {
    let dir = tempfile::tempdir().expect("tempdir");
    let shell = shell_over(dir.path());
    let mut session = shell
        .open(OpenContext::new(PipelineId::new("p"), LoadId::new("l")))
        .await
        .expect("open");
    let refused = session
        .ensure_table(
            &schema_for("evil\u{1b}]52;c;A\u{7}table"),
            &WriteMode::Replace,
        )
        .await
        .expect_err("replace refuses");
    let rendered = refused.to_string();
    assert!(
        !rendered.contains('\u{1b}') && !rendered.contains('\u{7}'),
        "no raw control byte survives the refusal: {rendered:?}"
    );
    assert!(
        rendered.contains("\\u{1b}") && rendered.contains("append-only"),
        "the name arrives spelled out inside the refusal: {rendered}"
    );
}

/// The foreign-pipeline state refusal quotes the OCCUPANT's pipeline id
/// — content read off DISK, which nothing upstream ever gated — through
/// the bounded diagnostic render.
#[tokio::test]
async fn a_foreign_pipeline_state_refusal_renders_the_occupant_inert() {
    let dir = tempfile::tempdir().expect("tempdir");
    let shell = shell_over(dir.path());
    let occupant = PipelineId::new("evil\u{1b}]52;c;A\u{7}pipe");
    let load = LoadId::new("l");
    let mut session = shell
        .open(OpenContext::new(occupant.clone(), load.clone()))
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
    session
        .commit(commit_meta_for(&occupant, &load, 1))
        .await
        .expect("commit under the hostile pipeline id");
    drop(session);

    let mut session = shell
        .open(OpenContext::new(PipelineId::new("p"), LoadId::new("l2")))
        .await
        .expect("re-open");
    let refused = session
        .read_state(&PipelineId::new("p"))
        .await
        .expect_err("a foreign state slot refuses");
    let rendered = refused.to_string();
    assert!(
        !rendered.contains('\u{1b}') && !rendered.contains('\u{7}'),
        "no raw control byte survives the refusal: {rendered:?}"
    );
    assert!(
        rendered.contains("\\u{1b}"),
        "the occupant arrives spelled out: {rendered}"
    );
}

/// A corrupt receipt line carrying control bytes renders spelled-out
/// and bounded in the refusal — the line is DISK content, and quoting
/// it raw would hand a terminal-injection payload to whoever reads the
/// error.
#[tokio::test]
async fn a_corrupt_receipt_line_refusal_renders_the_line_inert() {
    let dir = tempfile::tempdir().expect("tempdir");
    let hostile_line = format!("evil\u{1b}]52;c;A\u{7}{}\n", "x".repeat(2000));
    std::fs::write(dir.path().join("_reference_receipts.json"), hostile_line)
        .expect("seed the corrupt log");
    let shell = shell_over(dir.path());
    let mut session = shell
        .open(OpenContext::new(PipelineId::new("p"), LoadId::new("l")))
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
        .commit(commit_meta_for(&PipelineId::new("p"), &LoadId::new("l"), 1))
        .await
        .expect_err("a corrupt interior line refuses");
    let rendered = refused.to_string();
    assert!(
        !rendered.contains('\u{1b}') && !rendered.contains('\u{7}'),
        "no raw control byte survives the refusal: {rendered:?}"
    );
    assert!(
        rendered.contains("\\u{1b}") && rendered.contains("truncated from"),
        "the corrupt line arrives spelled out and bounded: {rendered}"
    );
}

/// The config gate's refusal, full-string: the one-field document
/// refuses an empty path with its own frozen wording. The gate is the
/// `Document` trait, so it is tested through it — no shell in between.
#[test]
fn the_config_gate_refuses_an_empty_path_with_the_frozen_spelling() {
    let refused = Config::from_value(json!({"path": ""})).unwrap_err();
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
    let shell = shell_over(dir.path());
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

/// Merge refuses the same way — typed, never silent. The engine's
/// validate gate refuses Merge against the declared `merge = false`
/// capability, but a host driving the backend directly never passes
/// that gate: accepting Merge here would append where the caller asked
/// for upsert-by-key, duplicating every redelivery quietly forever.
#[tokio::test]
async fn a_merge_write_mode_refuses_with_the_frozen_spelling() {
    let dir = tempfile::tempdir().expect("tempdir");
    let shell = shell_over(dir.path());
    let mut session = shell
        .open(OpenContext::new(
            PipelineId::new("ref-merge"),
            LoadId::new("ref-load-m"),
        ))
        .await
        .expect("open");
    let refused = session
        .ensure_table(
            &schema_for("events"),
            &WriteMode::Merge {
                key: vec!["id".into()],
            },
        )
        .await
        .expect_err("merge must refuse");
    assert_eq!(
        refused.to_string(),
        "fatal destination error: reference destination: table `events`: write mode `merge` \
         is not supported — jsonl parts are append-only"
    );
}

/// The session lease: two concurrent sessions of one pipeline would
/// each read the same persisted cursor and publish the same rows under
/// their own load ids — so the second open refuses typed with the
/// frozen spelling, and the lease releases with the session (drop or
/// process death), never blocking a successor.
#[tokio::test]
async fn a_second_concurrent_session_refuses_and_the_lease_releases_on_drop() {
    let dir = tempfile::tempdir().expect("tempdir");
    let shell = shell_over(dir.path());
    let held = shell
        .open(OpenContext::new(
            PipelineId::new("ref-lease"),
            LoadId::new("ref-load-a"),
        ))
        .await
        .expect("first open");
    let refused = match shell
        .open(OpenContext::new(
            PipelineId::new("ref-lease"),
            LoadId::new("ref-load-b"),
        ))
        .await
    {
        Ok(_) => panic!("a second concurrent session must refuse"),
        Err(refused) => refused,
    };
    assert_eq!(
        refused.to_string(),
        format!(
            "fatal destination error: reference destination: another session holds the lease \
             at {} — one session per output directory",
            dir.path().join("_reference_lease.lock").display()
        )
    );
    drop(held);
    shell
        .open(OpenContext::new(
            PipelineId::new("ref-lease"),
            LoadId::new("ref-load-c"),
        ))
        .await
        .expect("the lease released with the dropped session");
}

/// A receipt whose append TORE mid-write (its line never got the
/// terminating newline) never became durable: it reads as ABSENT, the
/// retried commit republishes over its deterministic part names
/// without duplicating, and the repaired log carries exactly the
/// durable receipt line — no glued garbage for a later read to refuse.
#[tokio::test]
async fn a_torn_receipt_tail_reads_as_absent_and_republishes_without_duplication() {
    let dir = tempfile::tempdir().expect("tempdir");
    let shell = shell_over(dir.path());
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
    let shell = shell_over(dir.path());
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

/// A table name is the SOURCE's declaration — third-party input by the
/// time it reaches a destination — and this connector is the worked
/// example third parties copy, so the part-filename seat must refuse a
/// name that could steer the write outside the output directory, typed
/// and fatal (no retry changes a declared name). Without the gate,
/// `../../evil` died as a TRANSIENT filesystem error on the staging
/// name — a retry-forever misclassification that named no cause — and
/// nothing may ever land outside the directory.
#[tokio::test]
async fn a_table_name_carrying_path_punctuation_is_refused_at_publish() {
    let root = tempfile::tempdir().expect("tempdir");
    let dir = root.path().join("a").join("b");
    let shell = shell_over(&dir);
    let pipeline = PipelineId::new("ref-traversal");
    let load = LoadId::new("ref-load-evil");
    let table = TableName::new("../../evil");

    let mut session = shell
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    session
        .ensure_table(&schema_for("../../evil"), &WriteMode::Append)
        .await
        .expect("ensure runs no DDL and stages nothing");
    session
        .write(&table, batch_of(&[1]))
        .await
        .expect("staging is in-memory");
    let error = session
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect_err("a traversal-shaped table name must be refused");
    assert_eq!(
        error.to_string(),
        "fatal destination error: reference destination: table name `../../evil` cannot \
         become a part filename — names carrying path separators, `..`, or control \
         characters are refused, because a filename built from them could land outside \
         the output directory"
    );
    // Nothing escaped the output directory: the tempdir root and its
    // `a` level hold ONLY the directory chain, no part and no staging.
    for level in [root.path().to_path_buf(), root.path().join("a")] {
        let entries: Vec<_> = std::fs::read_dir(&level)
            .expect("the level lists")
            .map(|e| e.expect("entry").file_name())
            .collect();
        assert_eq!(
            entries.len(),
            1,
            "only the directory chain at {level:?}: {entries:?}"
        );
    }
}

/// The tear's nastiest shape — the torn tail is INVALID UTF-8 (an
/// append that died inside a multi-byte character of a non-ASCII load
/// id's JSON spelling). A whole-file string read fails as `InvalidData`
/// and wedges the choreography on a transient; the bytes-first read
/// keeps the tear where it belongs: in the tail, which reads as absent,
/// while the durable lines still answer.
#[tokio::test]
async fn a_torn_receipt_tail_of_invalid_utf8_reads_as_absent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let shell = shell_over(dir.path());
    let pipeline = PipelineId::new("ref-torn-u8");
    // A non-ASCII load id: its JSON spelling carries the multi-byte
    // characters the tear can split.
    let load = LoadId::new("ref-lōad-è");
    let receipt = CommitReceipt {
        load_id: load.clone(),
        commit_seq: 7,
    };
    let durable_line = format!("{}\n", serde_json::to_string(&receipt).expect("encode"));
    let mut torn: Vec<u8> = serde_json::to_string(&receipt)
        .expect("encode")
        .into_bytes();
    // Truncate until the prefix is invalid UTF-8 — inside a
    // multi-byte character of the id's spelling. (JSON escapes the
    // id's non-ASCII as \u sequences, so also plant a RAW multi-byte
    // tail: the point is a tail no string decoder accepts.)
    torn.extend_from_slice("ł".as_bytes()[..1].to_vec().as_slice());
    assert!(
        std::str::from_utf8(&torn).is_err(),
        "the fixture's tail must be invalid UTF-8 — that is the point"
    );
    let mut log = durable_line.into_bytes();
    log.extend_from_slice(&torn);
    std::fs::write(dir.path().join("_reference_receipts.json"), log).expect("plant the torn log");

    let mut session = shell
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    // Drive the choreography: with the durable line present and the
    // tail torn mid-character, the commit must answer with the STORED
    // receipt (the replay path) — a whole-file string read wedged here
    // as an endless transient instead.
    session
        .ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    let answered = session
        .commit(commit_meta_for(&pipeline, &load, 7))
        .await
        .expect("the torn tail is absent, never a transient wedge");
    assert_eq!(
        answered, receipt,
        "the complete durable line still resolves through an invalid-UTF-8 tail"
    );
}

/// Invalid UTF-8 BEFORE the last complete line is a different animal
/// from the torn tail above — the writer only ever appends valid UTF-8
/// and a torn append is newline-less, so an interior corruption is
/// permanent: FATAL, never the endless-transient wedge the torn-tail
/// read closed for the other cause.
#[tokio::test]
async fn interior_invalid_utf8_in_the_receipt_log_is_fatal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let shell = shell_over(dir.path());
    let log_path = dir.path().join("_reference_receipts.json");
    // A newline-TERMINATED line carrying a raw invalid byte: durable
    // by position, unreadable by content.
    std::fs::write(&log_path, b"{\"load_id\":\"x\xff\"}\n").expect("seed interior corruption");

    let pipeline = PipelineId::new("ref-interior-u8");
    let load = LoadId::new("ref-load-i");
    let mut session = shell
        .open(OpenContext::new(pipeline.clone(), load.clone()))
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
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect_err("interior invalid UTF-8 must refuse");
    let rendered = refused.to_string();
    assert!(
        rendered.starts_with("fatal destination error: reference destination: ")
            && rendered.contains("corrupt receipt log (invalid UTF-8"),
        "the refusal is fatal and names the corruption: {rendered}"
    );
}

/// The raw backend over `dir` — the seat a foreign wire client reaches
/// WITHOUT the sdk `Session` wrapper's choreography guard, so the
/// backend's own receipt guard is all that stands.
async fn raw_backend(
    dir: &std::path::Path,
    pipeline: &PipelineId,
    load: &LoadId,
) -> rdlt_connector_reference::destination::session::Session {
    use rdlt_connector_sdk::destination::DestinationConnector;
    let connector = Reference::assemble(Config::from_value(json!({"path": dir})).expect("config"))
        .expect("assemble");
    connector
        .connect(&OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("connect")
}

/// A replay must name a receipt this store actually issued: a raw wire
/// client handing `Replay` a fabricated receipt — one the receipt log
/// never held — is refused typed, and its staged rows survive to be
/// published. Before this was pinned the raw replay cleared staging
/// unconditionally and answered Ok, silently discarding the staged
/// rows against a receipt that vouched for nothing. (The sdk wrapper
/// never reaches the refusal: it only replays a receipt
/// `existing_receipt` just returned.)
#[tokio::test]
async fn a_replay_of_a_receipt_the_store_never_issued_refuses_and_keeps_staging() {
    use rdlt_connector_sdk::destination::Backend;
    let dir = tempfile::tempdir().expect("tempdir");
    let probe = DirProbe(dir.path().to_path_buf());
    let pipeline = PipelineId::new("ref-fabricated-replay");
    let load = LoadId::new("ref-load-fabricated");
    let table = TableName::new("events");
    let mut backend = raw_backend(dir.path(), &pipeline, &load).await;

    backend
        .write(&table, batch_of(&[1, 2, 3]))
        .await
        .expect("write");
    let fabricated = CommitReceipt {
        load_id: load.clone(),
        commit_seq: 7,
    };
    let meta = commit_meta_for(&pipeline, &load, 7);
    let error = backend
        .replay(&meta, &fabricated)
        .await
        .expect_err("a receipt the log never held must refuse");
    let rendered = error.to_string();
    assert!(
        rendered.contains("never issued")
            && rendered.contains("ref-load-fabricated")
            && rendered.contains('7'),
        "the refusal names the store's judgment and the (load, seq): {rendered}"
    );

    // The staged rows survived the refused replay: a genuine publish
    // still persists all three.
    backend
        .publish(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("publish");
    assert_eq!(
        probe.count(&table).await.expect("count"),
        3,
        "a refused replay must leave staging intact"
    );
}

/// The replay refusal renders its load id BOUNDED: a receipt's fields
/// are wire-authored at the raw seat, and a refusal quoting them must
/// stay a refusal — control bytes spelled out, the render capped —
/// never a multi-KiB echo or terminal-injection material.
#[tokio::test]
async fn a_replay_refusal_renders_a_hostile_load_id_bounded() {
    use rdlt_connector_sdk::destination::Backend;
    let dir = tempfile::tempdir().expect("tempdir");
    let pipeline = PipelineId::new("ref-hostile-replay");
    // OSC-52 shaped and multi-KiB: both the injection and the size
    // threat in one id. It never reaches a filename (the guards refuse
    // before any publish), only the refusal's render.
    let hostile = format!("\u{1b}]52;c;{}\u{7}", "A".repeat(8 * 1024));
    let load = LoadId::new(hostile.clone());
    let table = TableName::new("events");
    let mut backend = raw_backend(dir.path(), &pipeline, &load).await;

    backend.write(&table, batch_of(&[1])).await.expect("write");
    let fabricated = CommitReceipt {
        load_id: load.clone(),
        commit_seq: 1,
    };
    let meta = commit_meta_for(&pipeline, &load, 1);
    let error = backend
        .replay(&meta, &fabricated)
        .await
        .expect_err("a receipt the log never held must refuse");
    let rendered = error.to_string();
    assert!(
        rendered.len() < 700,
        "the wire-authored id renders capped, not echoed whole: {} bytes",
        rendered.len()
    );
    assert!(
        !rendered.contains('\u{1b}') && !rendered.contains('\u{7}'),
        "control bytes render spelled out, never raw: {rendered:?}"
    );
    assert!(
        rendered.contains("truncated from"),
        "the render names the true length: {rendered}"
    );
}

/// A replay's receipt must name THIS replay's commit: a receipt the
/// log holds for a DIFFERENT (load, seq) proves only its own commit —
/// clearing staging against it would discard rows the store never
/// published under this commit. Refused with staging intact.
#[tokio::test]
async fn a_replay_with_a_held_receipt_for_another_commit_refuses_and_keeps_staging() {
    use rdlt_connector_sdk::destination::Backend;
    let dir = tempfile::tempdir().expect("tempdir");
    let probe = DirProbe(dir.path().to_path_buf());
    let pipeline = PipelineId::new("ref-wrong-seq-replay");
    let load = LoadId::new("ref-load-wrong-seq");
    let table = TableName::new("events");
    let mut backend = raw_backend(dir.path(), &pipeline, &load).await;

    backend
        .write(&table, batch_of(&[1, 2]))
        .await
        .expect("write");
    let held = backend
        .publish(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("publish");

    // Commit 2's rows staged, then a replay handing commit 1's HELD
    // receipt — issued, but naming another commit.
    backend
        .write(&table, batch_of(&[3, 4, 5]))
        .await
        .expect("restaged write");
    let meta = commit_meta_for(&pipeline, &load, 2);
    let error = backend
        .replay(&meta, &held)
        .await
        .expect_err("a held receipt naming another commit must refuse");
    let rendered = error.to_string();
    assert!(
        rendered.contains("a receipt proves only its own commit")
            && rendered.contains("(ref-load-wrong-seq, 2)")
            && rendered.contains("(ref-load-wrong-seq, 1)"),
        "the refusal names both pairs: {rendered}"
    );

    // The staged rows survived: commit 2's genuine publish persists
    // all three on top of commit 1's two.
    backend.publish(meta).await.expect("publish");
    assert_eq!(
        probe.count(&table).await.expect("count"),
        5,
        "a refused replay must leave staging intact"
    );
}

/// The held-receipt half: a replay naming a receipt the log DOES hold
/// behaves exactly as before — Ok, staging cleared, so a later commit
/// publishes nothing redelivered.
#[tokio::test]
async fn a_replay_of_a_held_receipt_clears_staging_as_before() {
    use rdlt_connector_sdk::destination::Backend;
    let dir = tempfile::tempdir().expect("tempdir");
    let probe = DirProbe(dir.path().to_path_buf());
    let pipeline = PipelineId::new("ref-held-replay");
    let load = LoadId::new("ref-load-held");
    let table = TableName::new("events");
    let mut backend = raw_backend(dir.path(), &pipeline, &load).await;

    backend
        .write(&table, batch_of(&[1, 2]))
        .await
        .expect("write");
    let receipt = backend
        .publish(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("publish");

    // The redelivery: rows restaged, then the held receipt replayed.
    backend
        .write(&table, batch_of(&[1, 2]))
        .await
        .expect("redelivered write");
    let meta = commit_meta_for(&pipeline, &load, 1);
    backend
        .replay(&meta, &receipt)
        .await
        .expect("a held receipt replays");
    backend
        .publish(commit_meta_for(&pipeline, &load, 2))
        .await
        .expect("next publish");
    assert_eq!(
        probe.count(&table).await.expect("count"),
        2,
        "replayed staging must not leak into a later commit"
    );
}

/// A commit whose receipt is durable is FINAL: a client that publishes
/// the same `(load, seq)` again — restaged rows and all, never having
/// asked for the existing receipt — gets the prior receipt back, its
/// restaged rows are dropped, the published part's bytes stay as they
/// were, and the receipt log grows by nothing. Before this was pinned
/// the raw publish rewrote the part under its deterministic name and
/// appended a second receipt line — the committed rows silently
/// replaced while both receipts vouched.
#[tokio::test]
async fn a_republished_receipted_commit_replays_instead_of_rewriting_the_part() {
    use rdlt_connector_sdk::destination::Backend;
    let dir = tempfile::tempdir().expect("tempdir");
    let pipeline = PipelineId::new("ref-republish");
    let load = LoadId::new("ref-load-republish");
    let table = TableName::new("events");
    let mut backend = raw_backend(dir.path(), &pipeline, &load).await;

    backend
        .write(&table, batch_of(&[1, 2]))
        .await
        .expect("write");
    let first = backend
        .publish(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("first publish");
    let part_of = || {
        std::fs::read_dir(dir.path())
            .expect("read_dir")
            .map(|entry| entry.expect("entry").path())
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("events-") && name.ends_with(".jsonl"))
            })
            .expect("the published part exists")
    };
    let part = part_of();
    let bytes_after_first = std::fs::read(&part).expect("part bytes");
    let receipts_after_first =
        std::fs::read_to_string(dir.path().join("_reference_receipts.json")).expect("receipts");
    assert_eq!(receipts_after_first.lines().count(), 1);

    // The choreography-violating client: DIFFERENT rows restaged, the
    // SAME commit published again, no `existing_receipt` asked.
    backend
        .write(&table, batch_of(&[7, 8, 9]))
        .await
        .expect("restaged write");
    let again = backend
        .publish(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("a republish of a receipted commit answers, it does not fail");
    assert_eq!(again, first, "the republish returns the PRIOR receipt");
    assert_eq!(
        std::fs::read(part_of()).expect("part bytes"),
        bytes_after_first,
        "the committed part's bytes are untouched by the republish"
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("_reference_receipts.json"))
            .expect("receipts")
            .lines()
            .count(),
        1,
        "the receipt log holds exactly one line for the commit"
    );

    // The restaged rows were DROPPED, not deferred: the next commit
    // publishes nothing of them.
    backend
        .publish(commit_meta_for(&pipeline, &load, 2))
        .await
        .expect("next publish");
    assert_eq!(
        DirProbe(dir.path().to_path_buf())
            .count(&table)
            .await
            .expect("count"),
        2,
        "only the first commit's rows are visible"
    );
}

/// One session serves ONE load: a publish whose meta names another
/// load is refused fatal, naming both — its part names would key on the
/// session's load while its receipt keyed on the meta's, a receipt
/// vouching for another load's files.
#[tokio::test]
async fn a_publish_for_another_load_refuses_fatal() {
    use rdlt_connector_sdk::destination::Backend;
    use rdlt_connector_sdk::spi::error::DestinationError;
    let dir = tempfile::tempdir().expect("tempdir");
    let pipeline = PipelineId::new("ref-cross-keyed");
    let opened_for = LoadId::new("ref-load-opened");
    let other = LoadId::new("ref-load-other");
    let table = TableName::new("events");
    let mut backend = raw_backend(dir.path(), &pipeline, &opened_for).await;

    backend.write(&table, batch_of(&[1])).await.expect("write");
    let refused = backend
        .publish(commit_meta_for(&pipeline, &other, 1))
        .await
        .expect_err("a publish keyed on another load must refuse");
    assert!(
        matches!(refused, DestinationError::Fatal(_)),
        "fatal, not transient — a retry can never make the loads agree: {refused}"
    );
    let rendered = refused.to_string();
    assert!(
        rendered.contains("ref-load-other") && rendered.contains("ref-load-opened"),
        "the refusal names both loads: {rendered}"
    );
    assert!(
        !dir.path().join("_reference_receipts.json").exists(),
        "a refused publish leaves no receipt behind"
    );
    assert_eq!(
        DirProbe(dir.path().to_path_buf())
            .count(&table)
            .await
            .expect("count"),
        0,
        "and publishes no part"
    );
}

/// The reachability probe is READ-ONLY and honest: a clean directory
/// (or a not-yet-created path under one) passes without creating
/// anything; a path whose nearest existing ancestor is a FILE is the
/// misconfiguration connect would hit, refused fatal at check instead.
#[tokio::test]
async fn the_check_probe_is_read_only_and_refuses_a_file_in_the_way() {
    let dir = tempfile::tempdir().expect("tempdir");

    // Existing directory: reachable, and the probe creates nothing.
    let shell = shell_over(dir.path());
    shell.check().await.expect("an existing directory passes");

    // Absent target under an existing directory: still reachable —
    // connect would create it — and STILL nothing is created.
    let absent = dir.path().join("not").join("yet");
    let shell = Shell::<Reference>::from_value(json!({"path": absent})).expect("valid config");
    shell.check().await.expect("a creatable path passes");
    assert!(
        !dir.path().join("not").exists(),
        "the probe must not create anything"
    );

    // A FILE where a directory must be: fatal, typed, at check time.
    let file = dir.path().join("occupied");
    std::fs::write(&file, b"not a directory").expect("write");
    for path in [file.clone(), file.join("child")] {
        let shell = Shell::<Reference>::from_value(json!({"path": path})).expect("valid config");
        let error = shell.check().await.expect_err("a file in the way refuses");
        let text = error.to_string();
        assert!(
            text.contains("is not a directory"),
            "the refusal names the shape: {text}"
        );
        assert!(
            matches!(
                error,
                rdlt_connector_sdk::spi::error::DestinationError::Fatal(_)
            ),
            "a misconfigured path is fatal, not retryable: {error:?}"
        );
    }
}
