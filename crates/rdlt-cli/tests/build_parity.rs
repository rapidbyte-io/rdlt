//! Every parity-fixture document must not only PARSE but BUILD: the
//! construction arm for each destination kind runs end-to-end up to the
//! built Pipeline (no run — building touches no external service), so a
//! destination added to the spec language without a working construction
//! path fails here.

use rdlt::pipeline_spec::{Spec, SpecError, build_pipeline};

const PARITY: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../benches/parity_specs.yaml"
));

/// Rewrite the fixture's referenced config paths onto real temp files so
/// source construction can read them.
fn materialize(doc: &str, dir: &std::path::Path) -> String {
    std::fs::write(
        dir.join("source.yaml"),
        "base_url: \"http://127.0.0.1:1/api\"\nstreams:\n  - name: events\n    path: /events\n",
    )
    .expect("rest config");
    std::fs::write(
        dir.join("files.yaml"),
        format!(
            "streams:\n  - name: events\n    format: jsonl\n    path: \"{}/rows.jsonl\"\n",
            dir.display()
        ),
    )
    .expect("file config");
    std::fs::write(
        dir.join("pg-source.yaml"),
        "conn: host=127.0.0.1 port=5432 user=postgres dbname=raw\ntables:\n  - name: events\n",
    )
    .expect("pg config");
    std::fs::write(dir.join("rows.jsonl"), "{\"id\":1}\n").expect("rows");
    doc.replace(
        "config: source.yaml",
        &format!("config: {}/source.yaml", dir.display()),
    )
    .replace(
        "config: files.yaml",
        &format!("config: {}/files.yaml", dir.display()),
    )
    .replace(
        "config: pg-source.yaml",
        &format!("config: {}/pg-source.yaml", dir.display()),
    )
    .replace(
        "path: out.duckdb",
        &format!("path: {}/out.duckdb", dir.display()),
    )
    .replace("path: ./lake", &format!("path: {}/lake", dir.display()))
    .replace(
        "path: lake/out",
        &format!("path: {}/lake-out", dir.display()),
    )
}

#[tokio::test]
async fn every_parity_document_builds_a_pipeline() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut count = 0usize;
    for (index, raw_doc) in PARITY.split("\n---\n").enumerate() {
        let doc_dir = dir.path().join(format!("doc{index}"));
        std::fs::create_dir_all(&doc_dir).expect("doc dir");
        let rewritten = materialize(raw_doc, &doc_dir);
        let spec: Spec = serde_yaml::from_str(&rewritten).expect("rewritten doc parses");
        let pipeline = build_pipeline(&spec)
            .unwrap_or_else(|e| panic!("document #{index} failed to build: {e}"));
        drop(pipeline);
        count += 1;
    }
    assert_eq!(count, 6, "fixture covers every destination kind");
}

/// Construction failures surface as typed Resolve errors naming the
/// problem — a missing config file must not panic or misclassify.
#[tokio::test]
async fn missing_config_file_is_a_resolve_error() {
    let yaml = "pipeline: p\nsource:\n  rest:\n    config: /no/such/file.yaml\ndestination:\n  parquet:\n    path: ./out\n";
    let spec: Spec = serde_yaml::from_str(yaml).expect("parses");
    match build_pipeline(&spec) {
        Err(SpecError::Resolve(message)) => {
            assert!(message.contains("/no/such/file.yaml"), "{message}");
        }
        Err(other) => panic!("expected a Resolve error, got: {other}"),
        Ok(_) => panic!("a missing config file must not build"),
    }
}
