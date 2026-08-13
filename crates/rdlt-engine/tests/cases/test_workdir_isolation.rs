//! The workdir is ONE pipeline's territory: a WAL span left by a
//! crashed pipeline must never be replayed, attributed, or cleared by
//! a DIFFERENT pipeline that happens to scan the same directory. The
//! scan refuses the occupied workdir typed — the one outcome recovery
//! resolves WITHOUT clearing, because clearing is exactly the
//! destruction the refusal exists to prevent.

use rdlt_core::{CommitPolicy, RdltError};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{
    CrashDestination, FaultPoint, MemoryBatch, MemoryDestination, MemorySource, MemoryStream,
};
use serde_json::json;

use super::common::without_load_id;

fn batches() -> Vec<MemoryBatch> {
    vec![
        MemoryBatch::new(vec![json!({"seq": 1}), json!({"seq": 2})]).with_checkpoint(1),
        MemoryBatch::new(vec![json!({"seq": 3}), json!({"seq": 4})]).with_checkpoint(2),
        MemoryBatch::new(vec![json!({"seq": 5})]).with_checkpoint(3),
    ]
}

fn source() -> MemorySource {
    MemorySource::new(vec![MemoryStream::new(
        rdlt_connector::StreamSpec::new("events"),
        batches(),
    )])
}

fn config(pipeline: &str, workdir: &std::path::Path) -> EngineConfig {
    EngineConfig::new(pipeline)
        .with_commit_policy(CommitPolicy::every_checkpoints(1))
        .with_workdir(workdir.to_path_buf())
}

/// THE CROSS-PIPELINE REFUSAL: `orders` crashes with a replayable WAL
/// span; `customers` then runs over the SAME workdir. The scan must
/// refuse typed — naming the occupant — and must NOT clear the WAL
/// (replaying would commit `orders`' rows and cursors under
/// `customers`; clearing would destroy `orders`' recovery material).
/// `orders` itself then recovers exactly as if `customers` had never
/// run.
#[tokio::test]
async fn a_foreign_pipelines_wal_refuses_and_survives() {
    let dir = tempfile::tempdir().expect("tempdir");

    // `orders` crashes during commit 2: a replayable span is on disk.
    let orders_dest = MemoryDestination::new();
    let crash = CrashDestination::new(orders_dest.clone(), FaultPoint::BeforeCommit(2));
    Engine::new(config("orders", dir.path()), source(), crash)
        .run()
        .await
        .expect_err("the fixture must crash");
    let manifest = dir.path().join("wal").join("manifest.jsonl");
    assert!(manifest.exists(), "the crash leaves a manifest");

    // `customers` over the same workdir: typed refusal, frozen spelling.
    let customers_dest = MemoryDestination::new();
    let err = Engine::new(
        config("customers", dir.path()),
        source(),
        customers_dest.clone(),
    )
    .run()
    .await
    .expect_err("a foreign WAL must refuse the run");
    let RdltError::Config { message } = &err else {
        panic!("the refusal is configuration-class (give each pipeline its own workdir): {err:?}");
    };
    assert_eq!(
        message.as_str(),
        format!(
            "the WAL at `{}` belongs to pipeline `orders` — this run is pipeline \
             `customers`; replaying another pipeline's crash residue would commit its rows \
             and cursors under the wrong pipeline, and clearing it would destroy its \
             recovery material, so the run refuses: give each pipeline its own workdir, \
             or move or delete that directory if `orders` is retired",
            dir.path().join("wal").display()
        )
    );
    assert!(
        customers_dest.snapshot().is_empty(),
        "nothing of `orders` may reach `customers`' destination"
    );
    assert!(
        manifest.exists(),
        "the refusal must not clear the foreign WAL"
    );

    // `orders` recovers as if nothing intervened.
    let report = Engine::new(config("orders", dir.path()), source(), orders_dest.clone())
        .run()
        .await
        .expect("orders' own recovery run");
    let _ = report;

    // Ground truth: an uninterrupted `orders` run in a fresh workdir.
    let clean_dir = tempfile::tempdir().expect("tempdir");
    let clean_dest = MemoryDestination::new();
    Engine::new(
        config("orders", clean_dir.path()),
        source(),
        clean_dest.clone(),
    )
    .run()
    .await
    .expect("clean run");
    assert_eq!(without_load_id(&orders_dest), without_load_id(&clean_dest));
}
