//! T008: the file source passes the public conformance suite (spec FR-004/SC-001) —
//! "certified = passes conformance".

use rdlt_connector_file::FileSource;
use rdlt_testkit::assert_conformant;
use rdlt_testkit::conformance::source::verify_source;

#[tokio::test]
async fn file_source_is_conformant() {
    let dir = tempfile::tempdir().expect("tempdir");
    for (name, rows) in [
        ("a.jsonl", vec![r#"{"n":1}"#, r#"{"n":2}"#]),
        ("b.jsonl", vec![r#"{"n":3}"#]),
        ("c.jsonl", vec![r#"{"n":4}"#, r#"{"n":5}"#]),
    ] {
        std::fs::write(dir.path().join(name), format!("{}\n", rows.join("\n"))).expect("write");
    }
    let source = FileSource::from_yaml(&format!(
        "streams:\n  - name: events\n    format: jsonl\n    path: \"{}/*.jsonl\"\n",
        dir.path().display()
    ))
    .expect("config");
    assert_conformant(verify_source(&source).await);
}
