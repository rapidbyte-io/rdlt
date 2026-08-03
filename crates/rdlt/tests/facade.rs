//! T027: the US1 flow through the public facade, plus build-time validation (B1–B3).

use rdlt::prelude::*;
use rdlt_testkit::{MemoryDestination, MemorySource};
use serde_json::json;

#[tokio::test]
async fn full_sync_through_the_facade() {
    let source = MemorySource::single_stream(
        rdlt_connector::StreamSpec::new("users"),
        vec![
            json!({"id": 1, "name": "ada", "emails": [{"addr": "a@x"}]}),
            json!({"id": 2, "name": "grace", "emails": []}),
        ],
    );
    let dest = MemoryDestination::new();

    let pipeline = Pipeline::builder("facade-demo")
        .source(source)
        .destination(dest.clone())
        .write_mode(WriteMode::Append)
        .build()
        .expect("valid config");

    // A pipeline is one-shot: `run` consumes it, so a second run cannot compile
    // (proven by the `compile_fail` doctest on `Pipeline::run`). Resuming means
    // building a fresh pipeline, which continues from committed state.
    let report = pipeline.run().await.expect("run succeeds");
    assert_eq!(report.total_rows(), 3, "2 users + 1 email child row");
    assert_eq!(dest.committed_rows("users").len(), 2);
    assert_eq!(dest.committed_rows("users__emails").len(), 1);
}

#[test]
fn build_rejects_merge_against_non_merge_destination() {
    let dest = MemoryDestination::new()
        .with_capabilities(rdlt_connector::DestinationCapabilities::default().with_merge(false));
    let err = Pipeline::builder("bad")
        .source(MemorySource::default())
        .destination(dest)
        .write_mode(WriteMode::Merge {
            key: vec!["id".into()],
        })
        .build()
        .expect_err("must fail fast at build time, pre-I/O");
    assert!(matches!(err, RdltError::Config { .. }));
    assert!(err.to_string().contains("Merge"));
}

#[test]
fn build_rejects_empty_merge_key() {
    let err = Pipeline::builder("bad")
        .source(MemorySource::default())
        .destination(MemoryDestination::new())
        .write_mode(WriteMode::Merge { key: vec![] })
        .build()
        .expect_err("empty key is a config error");
    assert!(err.to_string().contains("key"));
}

/// EVERY connector accepts its config INLINE and by PATH, and the two
/// forms produce the same pipeline.
///
/// Before this, only the postgres source took an inline document —
/// rest, oracle and file were path-only, so a one-connector pipeline
/// still needed two files. The asymmetry was the defect: a reader had
/// to remember which connector spelled it which way.
#[cfg(all(feature = "rest", feature = "file"))]
#[test]
fn a_source_config_is_accepted_inline_and_by_path() {
    use rdlt::pipeline_spec::Spec;

    let inline = r#"
pipeline: p
workdir: /tmp/rdlt-spec-test
source:
  rest:
    base_url: https://example.invalid
    auth: none
    streams:
      - name: s
        path: /s
destination:
  file:
    path: /tmp/rdlt-spec-out
    format: jsonl
"#;
    let spec: Spec = serde_yaml::from_str(inline).expect("inline source parses");
    rdlt::pipeline_spec::build_pipeline(&spec).expect("inline source builds");

    // The SAME document behind `config:` — written to a real file so
    // the path arm is exercised, not just parsed.
    let dir = tempfile::tempdir().expect("tempdir");
    let doc = dir.path().join("rest.yaml");
    std::fs::write(
        &doc,
        "base_url: https://example.invalid\nauth: none\nstreams:\n  - name: s\n    path: /s\n",
    )
    .expect("write");
    let by_path = format!(
        "pipeline: p\nworkdir: /tmp/rdlt-spec-test\nsource:\n  rest:\n    config: {}\n\
         destination:\n  file:\n    path: /tmp/rdlt-spec-out\n    format: jsonl\n",
        doc.display()
    );
    let spec: Spec = serde_yaml::from_str(&by_path).expect("path source parses");
    rdlt::pipeline_spec::build_pipeline(&spec).expect("path source builds");
}

/// An INLINE document is VALIDATED, not merely deserialized.
///
/// Untagged deserialization would happily accept a structurally-valid
/// document that the connector's own `validate` rejects; routing both
/// forms through the `Document` gate is what stops an invalid inline
/// config reaching the runtime — and the message is the connector's
/// own, not a facade paraphrase.
#[cfg(feature = "rest")]
#[test]
fn an_inline_config_still_faces_the_connectors_own_validator() {
    use rdlt::pipeline_spec::Spec;

    // Structurally fine, semantically refused: REST requires at least
    // one stream, and `max_concurrency` at least 1. Neither is
    // something serde can catch — only the connector's validator.
    let text = r#"
pipeline: p
workdir: /tmp/rdlt-spec-test
source:
  rest:
    base_url: https://example.invalid
    auth: none
    max_concurrency: 0
    streams:
      - name: s
        path: /s
destination:
  file:
    path: /tmp/rdlt-spec-out
    format: jsonl
"#;
    let spec: Spec = serde_yaml::from_str(text).expect("parses");
    let err = rdlt::pipeline_spec::build_pipeline(&spec)
        .expect_err("an invalid inline document must not build")
        .to_string();
    assert!(
        err.contains("max_concurrency"),
        "the connector's own wording must survive: {err}"
    );
}

/// `config:` mixed with inline keys is a LOUD error.
///
/// The untagged choice hangs on `ConfigPath` denying unknown fields.
/// Without that, a document carrying both would match the path arm and
/// the inline keys would be silently ignored — the worst outcome,
/// because it looks like it worked.
#[cfg(feature = "rest")]
#[test]
fn a_path_mixed_with_inline_keys_is_refused() {
    use rdlt::pipeline_spec::Spec;

    let text = r#"
pipeline: p
workdir: /tmp/rdlt-spec-test
source:
  rest:
    config: some/rest.yaml
    base_url: https://example.invalid
destination:
  file:
    path: /tmp/rdlt-spec-out
    format: jsonl
"#;
    // At PARSE, not later: neither arm matches, because the path arm
    // denies unknown fields and the inline arm has no `config` field.
    // An earlier attempt made the inline arm a free-form value, which
    // let this through to be caught only by the connector — loud, but
    // one layer too late.
    let parsed: Result<Spec, _> = serde_yaml::from_str(text);
    assert!(
        parsed.is_err(),
        "`config:` mixed with inline keys must fail at parse"
    );
}

/// `commit_policy` reaches the engine from the pipeline document, and
/// a policy that can never fire is REFUSED there.
///
/// The commit cadence is what decides how many rows land in each
/// destination part, so without this the only way to change file size
/// was to change how the SOURCE pages — conflating two decisions that
/// are not the same one.
#[cfg(all(feature = "rest", feature = "file"))]
#[test]
fn commit_policy_is_read_from_the_document_and_checked() {
    use rdlt::pipeline_spec::Spec;

    let with_policy = r#"
pipeline: p
workdir: /tmp/rdlt-commit-policy
commit_policy:
  every_bytes: 104857600
  every_seconds: 900
source:
  rest:
    base_url: https://example.invalid
    auth: none
    streams:
      - name: s
        path: /s
destination:
  file:
    path: /tmp/rdlt-commit-policy-out
    format: jsonl
"#;
    let spec: Spec = serde_yaml::from_str(with_policy).expect("parses");
    let policy = spec.commit_policy.expect("present");
    assert_eq!(policy.every_bytes, Some(104_857_600));
    assert_eq!(policy.every_seconds, Some(900));
    // 100 MB OR 15 minutes — whichever first.
    assert!(policy.triggers(0, 104_857_600, 0));
    assert!(policy.triggers(0, 0, 900));
    rdlt::pipeline_spec::build_pipeline(&spec).expect("builds");

    // A policy naming no threshold would never commit until the run
    // ended, so it is refused rather than honoured.
    let empty = with_policy.replace(
        "commit_policy:\n  every_bytes: 104857600\n  every_seconds: 900",
        "commit_policy: {}",
    );
    let spec: Spec = serde_yaml::from_str(&empty).expect("parses");
    let err = rdlt::pipeline_spec::build_pipeline(&spec)
        .expect_err("a policy with no threshold must not build")
        .to_string();
    assert!(err.contains("no threshold"), "{err}");

    // Absent is the safe default, not an error.
    let none = with_policy.replace(
        "commit_policy:\n  every_bytes: 104857600\n  every_seconds: 900\n",
        "",
    );
    let spec: Spec = serde_yaml::from_str(&none).expect("parses");
    assert!(spec.commit_policy.is_none());
    rdlt::pipeline_spec::build_pipeline(&spec).expect("builds with the default");
}
