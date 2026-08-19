//! The source's own exactly-once pins: the byte cursor's resume law
//! over an unchanged, grown, shrunk, and rewritten file, the
//! newline-termination rule against a live appender, the config gate's
//! refusals, and the read-failure classification.

use rdlt_connector_reference::source::config::Config;
use rdlt_connector_reference::source::connector::Reference;
use rdlt_connector_sdk::config::Document;
use rdlt_connector_sdk::source::Shell;
use rdlt_connector_sdk::spi::source::{Source, StreamSpec};
use serde_json::json;

use super::support::read_stream;

/// Three seed rows, 8 bytes per line (`{"n":1}` + newline) — the byte
/// offsets the cursor pins below are derived from this shape.
const SEED: &str = "{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n";

/// The cursor's tail hash, re-derived independently of the crate: the
/// hex blake3 of the last `min(bytes_read, 4096)` consumed bytes.
fn tail_hash_of(consumed: &str) -> String {
    let tail = &consumed.as_bytes()[consumed.len().saturating_sub(4096)..];
    blake3::hash(tail).to_hex().to_string()
}

/// A tempdir holding `events.jsonl` seeded with [`SEED`], plus the sdk
/// shell over it — the SPI face this crate's tests drive in-process.
fn seeded_source() -> (tempfile::TempDir, std::path::PathBuf, Shell<Reference>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("events.jsonl");
    std::fs::write(&path, SEED).expect("seed file");
    let shell = Shell::<Reference>::from_value(json!({"path": path})).expect("valid config");
    (dir, path, shell)
}

/// The exactly-once pin: a committed cursor at EOF means a re-run of an
/// unchanged file reads NOTHING again. Also pins the persisted v1 wire
/// shape — `{"v":1,"bytes_read":<u64>,"tail_hash":<hex>}` — as data,
/// not just behavior, the hash re-derived independently.
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
    assert_eq!(
        cursor.as_value(),
        &json!({"v": 1, "bytes_read": 24, "tail_hash": tail_hash_of(SEED)})
    );

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
    let grown = std::fs::read_to_string(&path).expect("read back");
    assert_eq!(
        checkpoint.expect("the tail read checkpoints").as_value(),
        &json!({"v": 1, "bytes_read": 32, "tail_hash": tail_hash_of(&grown)})
    );
}

/// A file REWRITTEN IN PLACE to the same (or greater) length refuses
/// typed with the frozen spelling: a bare offset guard would silently
/// resume mid-way through unrelated new content and emit its tail as
/// appended rows. A same-content rewrite legitimately passes — the
/// guard answers "is this still the file I read", not "was the inode
/// untouched".
#[tokio::test]
async fn a_file_rewritten_in_place_refuses_with_the_frozen_spelling() {
    let (_dir, path, shell) = seeded_source();
    let stream = shell.streams().await.expect("streams").remove(0);
    let (_, checkpoint) = read_stream(&shell, &stream, None).await.expect("full read");
    let cursor = checkpoint.expect("the read checkpoints");

    // Same byte length (24), different content: the shrink guard alone
    // cannot see this.
    std::fs::write(&path, "{\"m\":7}\n{\"m\":8}\n{\"m\":9}\n").expect("rewrite file");
    let refused = read_stream(&shell, &stream, Some(cursor.clone()))
        .await
        .expect_err("a rewritten file must refuse");
    assert_eq!(
        refused.to_string(),
        format!(
            "fatal source error: reference source: {}: the 24 bytes before the cursor no \
             longer match its tail hash — the file was rewritten in place, refusing to resume",
            path.display()
        )
    );

    // The rewrite restored byte-for-byte: the resume is legal again and
    // reads nothing new.
    std::fs::write(&path, SEED).expect("restore file");
    let (rows, _) = read_stream(&shell, &stream, Some(cursor))
        .await
        .expect("a same-content rewrite resumes");
    assert!(rows.is_empty(), "nothing new to read, got {rows:?}");
}

/// The newline-termination rule against a live appender: a final line
/// missing its newline is a row still being written — it is NOT
/// emitted, the cursor stays at the last newline, and the read that
/// sees the line completed picks it up whole. No split rows, no
/// refusal, and a fresh full read agrees with what the incremental
/// sessions delivered.
#[tokio::test]
async fn a_newline_less_tail_is_left_for_the_read_that_sees_it_complete() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("events.jsonl");
    // The appender flushed mid-line: the tail parses as JSON but its
    // line is not terminated.
    std::fs::write(&path, "{\"n\":1}\n{\"n\":2}").expect("seed file");
    let shell = Shell::<Reference>::from_value(json!({"path": path})).expect("valid config");
    let stream = shell.streams().await.expect("streams").remove(0);

    let (rows, checkpoint) = read_stream(&shell, &stream, None).await.expect("read");
    assert_eq!(
        rows,
        vec![json!({"n": 1})],
        "only the newline-terminated row is emitted"
    );
    let cursor = checkpoint.expect("the read checkpoints");
    assert_eq!(
        cursor.as_value(),
        &json!({"v": 1, "bytes_read": 8, "tail_hash": tail_hash_of("{\"n\":1}\n")}),
        "the cursor stays at the last newline, never mid-line"
    );

    // The appender finishes the line: the resumed read yields exactly
    // the completed row.
    std::fs::write(&path, "{\"n\":1}\n{\"n\":2}\n").expect("complete the line");
    let (rows, _) = read_stream(&shell, &stream, Some(cursor))
        .await
        .expect("resumed read");
    assert_eq!(
        rows,
        vec![json!({"n": 2})],
        "the completed row arrives whole — no split, no refusal"
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

/// The config gate's refusals, full-string: the one-field document
/// rejects unknown keys and refuses an empty or stem-less path with its
/// own frozen wording. The gate is the `Document` trait, so it is
/// tested through it — no shell in between.
#[test]
fn the_config_gate_refuses_with_frozen_spellings() {
    let refused = Config::from_value(json!({"path": ""})).unwrap_err();
    assert_eq!(
        refused.to_string(),
        "invalid reference source config: `path` is empty — one jsonl file is required"
    );
    let refused = Config::from_value(json!({"path": "/"})).unwrap_err();
    assert_eq!(
        refused.to_string(),
        "invalid reference source config: `/` has no file stem to name the stream"
    );
    let refused = Config::from_value(json!({"path": "a.jsonl", "glob": "*"})).unwrap_err();
    assert_eq!(
        refused.to_string(),
        "invalid reference source JSON: unknown field `glob`, expected `path`"
    );
}

/// A configured path naming a DIRECTORY can never be read no matter how
/// often it is retried: the failure classifies FATAL with the same
/// not-a-regular-file spelling `check` refuses — the two answers come
/// from one shape gate, so they agree by construction.
#[tokio::test]
async fn a_path_naming_a_directory_reads_fatal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("events");
    std::fs::create_dir(&path).expect("a directory where the file should be");
    let shell = Shell::<Reference>::from_value(json!({"path": path})).expect("valid config");
    let stream = shell.streams().await.expect("streams").remove(0);

    let refused = read_stream(&shell, &stream, None)
        .await
        .expect_err("reading a directory must refuse");
    assert_eq!(
        refused.to_string(),
        format!(
            "fatal source error: reference source: {}: not a regular file — `path` must \
             name a jsonl file",
            path.display()
        )
    );
}

/// The probe answers for the exact configured path: a directory fails
/// every read fatally, so a check that passed it was optimism about a
/// misconfiguration no retry fixes — it must refuse the same shape the
/// read refuses.
#[tokio::test]
async fn check_refuses_a_directory_like_the_read_does() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("events");
    std::fs::create_dir(&path).expect("a directory where the file should be");
    let shell = Shell::<Reference>::from_value(json!({"path": path})).expect("valid config");
    let refused = shell
        .check()
        .await
        .expect_err("a directory must fail the probe the way it fails the read");
    let rendered = refused.to_string();
    assert!(
        rendered.starts_with("fatal source error: "),
        "the probe's refusal is fatal: {rendered}"
    );
}

/// The unknown-stream refusal quotes the requested name — wire-authored
/// text for a served source — through the bounded diagnostic render:
/// control bytes arrive spelled out, never raw.
#[tokio::test]
async fn an_unknown_stream_refusal_renders_the_name_inert() {
    let (_dir, _path, shell) = seeded_source();
    let hostile = StreamSpec::new("evil\u{1b}]52;c;A\u{7}stream");
    let refused = read_stream(&shell, &hostile, None)
        .await
        .expect_err("an unknown stream refuses");
    let rendered = refused.to_string();
    assert!(
        !rendered.contains('\u{1b}') && !rendered.contains('\u{7}'),
        "no raw control byte survives the refusal: {rendered:?}"
    );
    assert!(
        rendered.contains("\\u{1b}") && rendered.contains("unknown stream"),
        "the name arrives spelled out inside the refusal: {rendered}"
    );
}

/// An unrecognized resume cursor refuses WITHOUT echoing the cursor —
/// a served cursor can weigh megabytes and a direct driver's is
/// unbounded — and the serde error's own fragment renders bounded and
/// inert.
#[tokio::test]
async fn an_unrecognized_cursor_refusal_does_not_echo_the_cursor() {
    let (_dir, _path, shell) = seeded_source();
    let stream = shell.streams().await.expect("streams").remove(0);
    let hostile = format!("evil\u{1b}]52;c;A\u{7}{}", "x".repeat(4000));
    let cursor = rdlt_connector_sdk::spi::core::cursor::Cursor::new(
        json!({"v": 1, "bytes_read": hostile, "tail_hash": "h"}),
    );
    let refused = read_stream(&shell, &stream, Some(cursor))
        .await
        .expect_err("an unrecognized cursor shape refuses");
    let rendered = refused.to_string();
    assert!(
        rendered.contains("unrecognized resume cursor"),
        "the refusal names the seat: {rendered}"
    );
    assert!(
        !rendered.contains('\u{1b}') && !rendered.contains('\u{7}'),
        "no raw control byte survives: {rendered:?}"
    );
    assert!(
        rendered.len() < 700,
        "the render is bounded, never the cursor's own size: {} bytes",
        rendered.len()
    );
}

/// A configured path naming a char device refuses typed at the read —
/// the same not-a-regular-file shape `check` refuses — instead of
/// reading from it. `/dev/null` reads as instantly-empty, so a read
/// that OPENS it succeeds silently; only the shape gate refuses it.
/// (`/dev/zero`, the same shape, would grow a buffer without bound.)
#[tokio::test]
async fn a_char_device_path_refuses_typed_at_the_read() {
    let shell = Shell::<Reference>::from_value(json!({"path": "/dev/null"})).expect("valid config");
    let stream = shell.streams().await.expect("streams").remove(0);
    let refused = read_stream(&shell, &stream, None)
        .await
        .expect_err("a char device must refuse at the read, not read as empty");
    assert_eq!(
        refused.to_string(),
        "fatal source error: reference source: /dev/null: not a regular file — `path` must \
         name a jsonl file"
    );
}

/// A configured path naming a FIFO refuses typed instead of parking the
/// read: opening a FIFO with no writer blocks forever, so the shape is
/// judged from metadata BEFORE any open. The read is driven from a
/// detached thread with its own runtime and a receive deadline — a
/// tokio timeout cannot fire over a synchronous open, and a wedged
/// worker would block the whole binary's exit — so a regression fails
/// in two seconds instead of hanging the suite.
#[tokio::test]
async fn a_fifo_path_refuses_instead_of_hanging() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fifo = dir.path().join("events.jsonl");
    let made = std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .expect("mkfifo runs");
    assert!(made.success(), "the fixture FIFO exists");
    let shell = Shell::<Reference>::from_value(json!({"path": &fifo})).expect("valid config");
    let stream = shell.streams().await.expect("streams").remove(0);

    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let outcome = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a current-thread runtime builds")
            .block_on(read_stream(&shell, &stream, None));
        let _ = tx.send(outcome);
    });
    let outcome = rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("the FIFO read must answer instead of hanging");
    let refused = outcome.expect_err("a FIFO must refuse typed");
    assert_eq!(
        refused.to_string(),
        format!(
            "fatal source error: reference source: {}: not a regular file — `path` must \
             name a jsonl file",
            fifo.display()
        )
    );
}

/// A single line past the 8 MiB line ceiling refuses typed instead of
/// materializing: jsonl is read line-streaming, so the ceiling is the
/// read's whole memory bound and an over-long line is judged as it
/// accumulates, never after it loaded.
#[tokio::test]
async fn an_oversized_line_refuses_typed_at_its_offset() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("events.jsonl");
    let mut giant = String::with_capacity((8 << 20) + 64);
    giant.push_str("{\"n\":1}\n{\"s\":\"");
    giant.push_str(&"x".repeat(8 << 20));
    giant.push_str("\"}\n");
    std::fs::write(&path, giant).expect("seed file");
    let shell = Shell::<Reference>::from_value(json!({"path": &path})).expect("valid config");
    let stream = shell.streams().await.expect("streams").remove(0);
    // The refusal must land BEFORE the row is pushed — a materialized
    // 8 MiB row would out-weigh the drain harness's budget and park the
    // read — so the pin drives it under the same fail-fast deadline as
    // the FIFO shape: a regression answers in two seconds, never a hang.
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let outcome = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a current-thread runtime builds")
            .block_on(read_stream(&shell, &stream, None));
        let _ = tx.send(outcome);
    });
    let outcome = rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("the over-ceiling read must answer instead of materializing");
    let refused = outcome.expect_err("an over-ceiling line must refuse");
    assert_eq!(
        refused.to_string(),
        format!(
            "fatal source error: reference source: {}: the line at byte 8 is over the \
             8388608-byte line ceiling — one jsonl row rides the document family's 8 MiB bound",
            path.display()
        )
    );
}

/// A file larger than one batch streams complete: every row arrives
/// exactly once across the batch boundaries and the final cursor stands
/// at EOF, so a resume reads zero — the count-integrity belt over the
/// streaming read.
#[tokio::test]
async fn a_multi_batch_file_streams_every_row_exactly_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("events.jsonl");
    let mut seed = String::new();
    for n in 0..3000 {
        seed.push_str(&format!("{{\"n\":{n}}}\n"));
    }
    std::fs::write(&path, &seed).expect("seed file");
    let shell = Shell::<Reference>::from_value(json!({"path": &path})).expect("valid config");
    let stream = shell.streams().await.expect("streams").remove(0);

    let (rows, checkpoint) = read_stream(&shell, &stream, None).await.expect("full read");
    assert_eq!(rows.len(), 3000, "every row exactly once across batches");
    assert_eq!(
        rows[2999],
        json!({"n": 2999}),
        "order preserved to the last row"
    );
    let cursor = checkpoint.expect("the read checkpoints");
    assert_eq!(
        cursor.as_value(),
        &json!({"v": 1, "bytes_read": seed.len(), "tail_hash": tail_hash_of(&seed)})
    );
    let (rows, _) = read_stream(&shell, &stream, Some(cursor))
        .await
        .expect("resumed read");
    assert!(rows.is_empty(), "nothing left after a complete stream");
}
