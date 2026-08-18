//! The engine's boundary through the public facade: a full sync over
//! in-memory doubles, and every `build()` rejection arm producing a
//! typed config error naming the offender — none may slip through to
//! run time.

use rdlt::document;
use rdlt::error::Error;
use rdlt::prelude::*;
use rdlt::sdk::spi::destination::Capabilities;
use rdlt::sdk::spi::source::StreamSpec;
use rdlt_testkit::memory;
use serde_json::json;

use super::support::{empty_source, merge_less_destination};

#[tokio::test]
async fn full_sync_through_the_facade() {
    let source = memory::Source::single_stream(
        StreamSpec::new("users"),
        vec![
            json!({"id": 1, "name": "ada", "emails": [{"addr": "a@x"}]}),
            json!({"id": 2, "name": "grace", "emails": []}),
        ],
    );
    let dest = memory::Destination::new();

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

/// A destination without merge capability rejects Merge at build — both as
/// the default write mode and per-stream, each naming what requested it.
#[test]
fn merge_against_a_merge_less_destination_is_rejected_at_build() {
    let dest = merge_less_destination();
    let err = Pipeline::builder("p")
        .source(empty_source())
        .destination(dest.clone())
        .write_mode(WriteMode::Merge {
            key: vec!["id".into()],
        })
        .build()
        .expect_err("default merge must be rejected");
    assert!(
        err.to_string().contains("default write mode"),
        "names the default request: {err}"
    );

    let err = Pipeline::builder("p")
        .source(empty_source())
        .destination(dest)
        .write_mode_for(
            "orders",
            WriteMode::Merge {
                key: vec!["id".into()],
            },
        )
        .build()
        .expect_err("per-stream merge must be rejected");
    assert!(
        err.to_string().contains("orders"),
        "names the stream: {err}"
    );
}

#[test]
fn build_rejects_merge_against_non_merge_destination() {
    let dest =
        memory::Destination::new().with_capabilities(Capabilities::default().with_merge(false));
    let err = Pipeline::builder("bad")
        .source(memory::Source::default())
        .destination(dest)
        .write_mode(WriteMode::Merge {
            key: vec!["id".into()],
        })
        .build()
        .expect_err("must fail fast at build time, pre-I/O");
    assert!(matches!(err, Error::Config { .. }));
    assert!(err.to_string().contains("Merge"));
}

/// Merge with an EMPTY key is rejected in both spellings.
#[test]
fn empty_merge_key_is_rejected_at_build() {
    let err = Pipeline::builder("p")
        .source(empty_source())
        .destination(memory::Destination::new())
        .write_mode(WriteMode::Merge { key: vec![] })
        .build()
        .expect_err("empty default key");
    assert!(err.to_string().contains("at least one key column"), "{err}");

    let err = Pipeline::builder("p")
        .source(empty_source())
        .destination(memory::Destination::new())
        .write_mode_for("orders", WriteMode::Merge { key: vec![] })
        .build()
        .expect_err("empty per-stream key");
    assert!(
        err.to_string().contains("`orders`"),
        "names the stream: {err}"
    );
}

#[test]
fn build_rejects_empty_merge_key() {
    let err = Pipeline::builder("bad")
        .source(memory::Source::default())
        .destination(memory::Destination::new())
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
    let doc: document::Document = serde_yaml_ng::from_str(with_policy).expect("parses");
    let policy = doc.commit_policy.expect("present");
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
    let doc: document::Document = serde_yaml_ng::from_str(&empty).expect("parses");
    let err = document::build(&doc, std::path::Path::new(""))
        .await
        .expect_err("a policy with no threshold must not build")
        .to_string();
    assert!(err.contains("no threshold"), "{err}");

    // Absent is the safe default, not an error.
    let none = with_policy.replace(
        "commit_policy:\n  every_bytes: 104857600\n  every_seconds: 900\n",
        "",
    );
    let doc: document::Document = serde_yaml_ng::from_str(&none).expect("parses");
    assert!(doc.commit_policy.is_none());
}

/// The engine's escape-hatch knobs are reachable from the facade — a
/// cell budget tightened through the builder refuses a batch the
/// default would pass, with the refusal naming the knob the builder
/// just set (the honest remedy for wide-and-large tables is raising it
/// HERE, not dropping to raw `EngineConfig`).
#[tokio::test]
async fn the_builder_plumbs_the_engine_knobs() {
    let source = memory::Source::single_stream(
        StreamSpec::new("users"),
        vec![
            json!({"id": 1, "name": "ada"}),
            json!({"id": 2, "name": "grace"}),
        ],
    );
    let dest = memory::Destination::new();

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

/// The builder refuses a threshold-less commit policy — the same
/// refusal the YAML parse makes. Such a policy would hold the whole run
/// in one crash window, the exact shape the type's `check` exists to
/// refuse.
#[test]
fn build_rejects_a_threshold_less_commit_policy() {
    let threshold_less = CommitPolicy {
        every_checkpoints: None,
        every_bytes: None,
        every_seconds: None,
    };
    let err = Pipeline::builder("bad-policy")
        .source(memory::Source::default())
        .destination(memory::Destination::new())
        .commit_policy(threshold_less)
        .build()
        .expect_err("a policy with no threshold must not build");
    assert!(
        err.to_string().contains("every_checkpoints"),
        "the refusal names the remedy: {err}"
    );
}
