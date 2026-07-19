//! US1 — JSONL file loading with per-file incremental resume (spec acceptance
//! scenarios 1–3 + edge cases). Drives the Source directly through the SPI channel.

use rdlt_connector::{Cursor, PushPayload, ReadRequest, Source, SourceError, records_channel};
use rdlt_source_file::FileSource;
use serde_json::Value;

fn write_file(dir: &std::path::Path, name: &str, lines: &[&str]) -> std::path::PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, format!("{}\n", lines.join("\n"))).expect("write file");
    path
}

fn source_for(pattern: &str) -> FileSource {
    FileSource::from_yaml(&format!(
        "streams:\n  - name: events\n    format: jsonl\n    path: \"{pattern}\"\n"
    ))
    .expect("valid config")
}

/// Read the single stream to completion; returns (rows, final checkpoint cursor).
async fn read_all(
    source: &FileSource,
    since: Option<Cursor>,
) -> Result<(Vec<Value>, Option<Cursor>), String> {
    let (out, mut input) = records_channel(16 << 20);
    let spec = source.streams().await.map_err(|e| e.to_string())?[0].clone();
    let read = {
        let request = ReadRequest::new(spec, since, out);
        async move { source.read(request).await }
    };
    tokio::pin!(read);

    let mut rows = Vec::new();
    let mut last_cursor = None;
    let mut read_result: Option<Result<(), SourceError>> = None;
    loop {
        tokio::select! {
            push = input.recv() => match push {
                Some(push) => match push.payload {
                    PushPayload::RawJson(bytes) => {
                        for doc in serde_json::Deserializer::from_slice(&bytes).into_iter::<Value>() {
                            rows.push(doc.map_err(|e| e.to_string())?);
                        }
                    }
                    PushPayload::Checkpoint(cursor) => last_cursor = Some(cursor),
                    other => return Err(format!("unexpected push {other:?}")),
                },
                None => break,
            },
            result = &mut read, if read_result.is_none() => read_result = Some(result),
        }
    }
    match read_result {
        Some(Err(e)) => Err(e.to_string()),
        _ => Ok((rows, last_cursor)),
    }
}

#[tokio::test]
async fn full_glob_load_in_path_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_file(dir.path(), "b.jsonl", &[r#"{"n":3}"#, r#"{"n":4}"#]);
    write_file(dir.path(), "a.jsonl", &[r#"{"n":1}"#, r#"{"n":2}"#]);
    write_file(dir.path(), "c.jsonl", &[r#"{"n":5}"#]);

    let source = source_for(&format!("{}/*.jsonl", dir.path().display()));
    let (rows, cursor) = read_all(&source, None).await.expect("read");
    let ns: Vec<i64> = rows.iter().map(|r| r["n"].as_i64().unwrap()).collect();
    assert_eq!(
        ns,
        vec![1, 2, 3, 4, 5],
        "lexicographic file order, line order within"
    );
    assert!(cursor.is_some(), "completion is checkpointed");
}

#[tokio::test]
async fn resume_reads_only_appended_tail_and_new_files() {
    let dir = tempfile::tempdir().expect("tempdir");
    let a = write_file(dir.path(), "a.jsonl", &[r#"{"n":1}"#]);
    write_file(dir.path(), "b.jsonl", &[r#"{"n":2}"#]);
    let pattern = format!("{}/*.jsonl", dir.path().display());

    let source = source_for(&pattern);
    let (rows, cursor) = read_all(&source, None).await.expect("first read");
    assert_eq!(rows.len(), 2);
    let cursor = cursor.expect("cursor");

    // Append to a.jsonl; add a brand-new c.jsonl.
    use std::io::Write;
    let mut fh = std::fs::OpenOptions::new()
        .append(true)
        .open(&a)
        .expect("open");
    writeln!(fh, r#"{{"n":10}}"#).expect("append");
    drop(fh);
    write_file(dir.path(), "c.jsonl", &[r#"{"n":20}"#]);

    let source = source_for(&pattern);
    let (rows, _) = read_all(&source, Some(cursor)).await.expect("resume read");
    let mut ns: Vec<i64> = rows.iter().map(|r| r["n"].as_i64().unwrap()).collect();
    ns.sort_unstable();
    assert_eq!(ns, vec![10, 20], "completed ranges are never re-read");
}

#[tokio::test]
async fn shrunk_file_fails_naming_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let a = write_file(dir.path(), "a.jsonl", &[r#"{"n":1}"#, r#"{"n":2}"#]);
    let pattern = format!("{}/*.jsonl", dir.path().display());

    let source = source_for(&pattern);
    let (_, cursor) = read_all(&source, None).await.expect("first read");

    // Rewrite the file SHORTER than the recorded progress.
    std::fs::write(&a, "{\"n\":9}\n").expect("truncate");

    let source = source_for(&pattern);
    let err = read_all(&source, cursor).await.expect_err("must fail");
    assert!(
        err.contains("a.jsonl"),
        "error must name the file, got: {err}"
    );
}

#[tokio::test]
async fn empty_glob_is_an_empty_success() {
    let dir = tempfile::tempdir().expect("tempdir");
    let source = source_for(&format!("{}/*.jsonl", dir.path().display()));
    let (rows, _) = read_all(&source, None).await.expect("empty glob is fine");
    assert!(rows.is_empty());
}

#[tokio::test]
async fn missing_named_file_is_an_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let source = source_for(&format!(
        "{}/definitely-missing.jsonl",
        dir.path().display()
    ));
    let err = read_all(&source, None)
        .await
        .expect_err("explicit missing file fails");
    assert!(
        err.contains("definitely-missing.jsonl"),
        "error names the file, got: {err}"
    );
}

#[tokio::test]
async fn malformed_line_fails_naming_file_and_offset() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_file(
        dir.path(),
        "bad.jsonl",
        &[r#"{"n":1}"#, "{not json", r#"{"n":3}"#],
    );
    let source = source_for(&format!("{}/*.jsonl", dir.path().display()));
    let err = read_all(&source, None)
        .await
        .expect_err("malformed line fails");
    assert!(
        err.contains("bad.jsonl"),
        "error names the file, got: {err}"
    );
    assert!(
        err.contains("offset"),
        "error carries a byte offset, got: {err}"
    );
}

#[tokio::test]
async fn unchanged_second_run_reads_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_file(dir.path(), "a.jsonl", &[r#"{"n":1}"#, r#"{"n":2}"#]);
    let pattern = format!("{}/*.jsonl", dir.path().display());

    let source = source_for(&pattern);
    let (_, cursor) = read_all(&source, None).await.expect("first read");
    let source = source_for(&pattern);
    let (rows, _) = read_all(&source, cursor).await.expect("second read");
    assert!(
        rows.is_empty(),
        "SC-002: zero rows on an unchanged file set"
    );
}
