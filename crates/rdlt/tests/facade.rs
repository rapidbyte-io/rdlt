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

/// `commit_policy` reaches the engine from the pipeline document, and
/// a policy that can never fire is REFUSED there — BEFORE any
/// connector is resolved, so the refusal costs no spawn.
///
/// The commit cadence is what decides how many rows land in each
/// destination part, so without this the only way to change file size
/// was to change how the SOURCE pages — conflating two decisions that
/// are not the same one.
#[tokio::test]
async fn commit_policy_is_read_from_the_document_and_checked() {
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
    let spec: Spec = serde_yaml_ng::from_str(with_policy).expect("parses");
    let policy = spec.commit_policy.expect("present");
    assert_eq!(policy.every_bytes, Some(104_857_600));
    assert_eq!(policy.every_seconds, Some(900));
    // 100 MB OR 15 minutes — whichever first.
    assert!(policy.triggers(0, 104_857_600, 0));
    assert!(policy.triggers(0, 0, 900));

    // A policy naming no threshold would never commit until the run
    // ended, so it is refused rather than honoured — and the check
    // sits before connector resolution, so no binary is looked for.
    let empty = with_policy.replace(
        "commit_policy:\n  every_bytes: 104857600\n  every_seconds: 900",
        "commit_policy: {}",
    );
    let spec: Spec = serde_yaml_ng::from_str(&empty).expect("parses");
    let err = rdlt::pipeline_spec::build_pipeline(&spec, std::path::Path::new(""))
        .await
        .expect_err("a policy with no threshold must not build")
        .to_string();
    assert!(err.contains("no threshold"), "{err}");

    // Absent is the safe default, not an error.
    let none = with_policy.replace(
        "commit_policy:\n  every_bytes: 104857600\n  every_seconds: 900\n",
        "",
    );
    let spec: Spec = serde_yaml_ng::from_str(&none).expect("parses");
    assert!(spec.commit_policy.is_none());
}

/// 6L6: the engine's escape-hatch knobs are reachable from the facade —
/// a cell budget tightened through the builder refuses a batch the
/// default would pass, with the refusal naming the knob the builder
/// just set (the honest remedy for wide-and-large tables is raising it
/// HERE, not dropping to raw `EngineConfig`).
#[tokio::test]
async fn the_builder_plumbs_the_engine_knobs() {
    let source = MemorySource::single_stream(
        rdlt_connector::StreamSpec::new("users"),
        vec![
            json!({"id": 1, "name": "ada"}),
            json!({"id": 2, "name": "grace"}),
        ],
    );
    let dest = MemoryDestination::new();

    let error = Pipeline::builder("knob-demo")
        .source(source)
        .destination(dest)
        .max_batch_cells(1)
        .build()
        .expect("valid config")
        .run()
        .await
        .expect_err("a one-cell budget refuses any real batch");
    let rendered = error.to_string();
    assert!(
        rendered.contains("with_max_batch_cells") || rendered.contains("cell"),
        "the refusal names the knob or its axis: {rendered}"
    );
}
