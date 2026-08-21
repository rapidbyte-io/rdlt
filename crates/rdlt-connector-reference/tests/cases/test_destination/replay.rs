//! The receipt log drives the sdk choreography: replay instead of rewrite, torn and corrupt tails judged honestly, and staging cleared by the same law.

use super::*;

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

/// A receipt log grown past 8 MiB — the document family's ceiling, and
/// this log's own read ceiling once — still answers replay: the log is
/// an append-only journal that grows for the store's whole life, so a
/// total bound here was a wedge every honest store eventually reached
/// (~250k short-id commits), turning every later publish FATAL. The
/// scan streams one line at a time: the grown log dedups a replayed
/// receipt AND accepts the next append.
#[tokio::test]
async fn a_receipt_log_past_the_old_ceiling_still_answers_replay() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut log = String::from("{\"load_id\":\"l\",\"commit_seq\":1}\n");
    for n in 0.. {
        log.push_str(&format!("{{\"load_id\":\"load-{n}\",\"commit_seq\":1}}\n"));
        if log.len() > 8 * 1024 * 1024 + 1024 {
            break;
        }
    }
    std::fs::write(dir.path().join("_reference_receipts.json"), &log).expect("seed the log");
    let shell = shell_over(dir.path());
    let probe = DirProbe(dir.path().to_path_buf());
    let table = TableName::new("events");

    // The receipted (load, seq) replays: the prior receipt comes back,
    // the redelivered staging is dropped, nothing publishes.
    let mut session = shell
        .open(OpenContext::new(PipelineId::new("p"), LoadId::new("l")))
        .await
        .expect("open");
    session
        .ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    session.write(&table, batch_of(&[1])).await.expect("write");
    let replayed = session
        .commit(commit_meta_for(&PipelineId::new("p"), &LoadId::new("l"), 1))
        .await
        .expect("a grown log answers replay instead of wedging");
    assert_eq!(
        replayed,
        CommitReceipt {
            load_id: LoadId::new("l"),
            commit_seq: 1
        }
    );
    assert_eq!(
        probe.count(&table).await.expect("count"),
        0,
        "the replayed unit republishes nothing"
    );
    drop(session);

    // An unseen (load, seq) still publishes: the append lands past the
    // old ceiling and the store keeps working.
    let mut session = shell
        .open(OpenContext::new(PipelineId::new("p"), LoadId::new("l2")))
        .await
        .expect("re-open");
    session
        .ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    session
        .write(&table, batch_of(&[1, 2]))
        .await
        .expect("write");
    session
        .commit(commit_meta_for(
            &PipelineId::new("p"),
            &LoadId::new("l2"),
            1,
        ))
        .await
        .expect("a fresh commit appends past the old ceiling");
    assert_eq!(probe.count(&table).await.expect("count"), 2);
}

/// A newline-terminated receipt line longer than any line the gated
/// append writes is corruption, refused typed with the line-bound
/// spelling — the streaming scan's memory bound is the per-LINE bound,
/// and this is the arm that keeps it honest.
#[tokio::test]
async fn an_oversized_receipt_line_refuses_the_log_as_corrupt() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut log = vec![b'x'; 9000];
    log.push(b'\n');
    std::fs::write(dir.path().join("_reference_receipts.json"), log).expect("seed the log");
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
        .expect_err("an over-long interior line refuses");
    let rendered = refused.to_string();
    assert!(
        rendered.contains("8192-byte line bound")
            && rendered.contains("refusing the log as corrupt"),
        "refused at the line bound, typed: {rendered}"
    );
}

/// The raw backend over `dir` — the seat a foreign wire client reaches
/// WITHOUT the sdk `Session` wrapper's choreography guard, so the
/// backend's own receipt guard is all that stands.
pub(super) async fn raw_backend(
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

/// The torn-tail cut's boundary, both sides: a newline-less log of
/// exactly one gated line (8192 bytes) is the writer's maximal possible
/// tear — cut to empty, and the store keeps working. ONE byte more is
/// a tail no gated writer can leave (the gate bounds every line before
/// its newline), so it refuses as corruption instead of being silently
/// repaired to empty — a repair would eat evidence of a foreign write.
///
/// WHICH SEAT ANSWERS, said plainly: on the commit path the receipt
/// scan reaches the over-long line first and refuses there, so this
/// pin's second arm proves the STORE refuses, not that the cut does.
/// The cut's own arm is pinned directly beside it in the store's unit
/// tests, where reverting the guard actually reds.
#[tokio::test]
async fn the_torn_tail_cut_admits_a_maximal_tear_and_refuses_one_byte_more() {
    // 8192 newline-less bytes: a maximal tear; the commit cuts it and
    // publishes.
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("_reference_receipts.json"),
        vec![b'x'; 8192],
    )
    .expect("seed the torn log");
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
    session
        .commit(commit_meta_for(&PipelineId::new("p"), &LoadId::new("l"), 1))
        .await
        .expect("a maximal tear is cut and the commit publishes");

    // 8193: no gated writer leaves this; refused as corrupt.
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("_reference_receipts.json"),
        vec![b'x'; 8193],
    )
    .expect("seed the over-long tail");
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
        .expect_err("a tail past the maximal tear refuses");
    let rendered = refused.to_string();
    assert!(
        rendered.contains("8192-byte line bound")
            && rendered.contains("refusing the log as corrupt"),
        "refused with the line-bound corruption spelling: {rendered}"
    );
}
