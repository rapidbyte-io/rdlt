//! Every publish-path refusal renders its hostile input inert — bounded, never echoed raw.

use super::replay::raw_backend;
use super::*;


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
