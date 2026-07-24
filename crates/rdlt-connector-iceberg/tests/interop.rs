//! Interop cells: pyiceberg — a second, independent
//! Iceberg implementation — reads rdlt-written tables back through the
//! SAME REST catalog and confirms counts, schema, partition spec, and
//! the rdlt.* snapshot receipts. Skip-not-fail: no container runtime OR
//! no interop venv (build one with:
//! `python3 -m venv tools/interop/.venv && tools/interop/.venv/bin/pip
//!  install -r tools/interop/requirements.txt`).

mod common;

use common::CatalogFixture;
use rdlt_connector::StreamSpec;
use rdlt_connector_iceberg::{IcebergDest, PartitionField, PartitionTransform, TableOptions};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream};
use serde_json::json;

fn interop_python() -> Option<std::path::PathBuf> {
    if let Ok(explicit) = std::env::var("RDLT_INTEROP_PYTHON") {
        return Some(std::path::PathBuf::from(explicit));
    }
    let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)?
        .to_path_buf();
    let venv = repo.join("tools/interop/.venv/bin/python");
    venv.exists().then_some(venv)
}

fn readback_script() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("repo root")
        .join("tools/interop/pyiceberg_readback.py")
}

async fn read_back(
    python: &std::path::Path,
    fixture: &CatalogFixture,
    namespace: &str,
    table: &str,
) -> serde_json::Value {
    let output = std::process::Command::new(python)
        .arg(readback_script())
        .args(["--uri", &fixture.catalog_uri])
        .args(["--warehouse", common::WAREHOUSE])
        .args([
            "--credential",
            &format!("{}:{}", common::CLIENT_ID, common::CLIENT_SECRET),
        ])
        .args(["--namespace", namespace])
        .args(["--table", table])
        .output()
        .expect("spawn pyiceberg readback");
    assert!(
        output.status.success(),
        "pyiceberg readback failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("readback json")
}

fn skip_no_venv() -> Option<std::path::PathBuf> {
    let python = interop_python();
    if python.is_none() {
        eprintln!("SKIP: no interop venv (tools/interop/.venv) — pyiceberg cell not run");
    }
    python
}

/// Shape 1: plain append across two commits — pyiceberg sees the exact
/// count, the columns, both snapshots, and the rdlt.* receipts.
#[tokio::test(flavor = "multi_thread")]
async fn pyiceberg_reads_plain_append() {
    let Some(python) = skip_no_venv() else {
        return;
    };
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let source = MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![
            MemoryBatch::new(vec![
                json!({"id": 1, "name": "a"}),
                json!({"id": 2, "name": "b"}),
            ])
            .with_checkpoint(2),
            MemoryBatch::new(vec![json!({"id": 3, "name": "c"})]).with_checkpoint(3),
        ],
    )]);
    let dest = IcebergDest::from_config(fixture.config("interop_plain")).expect("dest");
    Engine::new(EngineConfig::new("ice-interop"), source, dest)
        .run()
        .await
        .expect("run");

    let doc = read_back(&python, &fixture, "interop_plain", "events").await;
    assert_eq!(doc["count"], 3);
    assert_eq!(doc["snapshots"], 2, "one snapshot per commit");
    let columns: Vec<&str> = doc["columns"]
        .as_array()
        .expect("columns")
        .iter()
        .map(|c| c["name"].as_str().expect("name"))
        .collect();
    assert!(
        columns.contains(&"id") && columns.contains(&"name"),
        "{columns:?}"
    );
    for props in doc["snapshot_props"].as_array().expect("props") {
        assert!(
            props["rdlt.load-id"].is_string(),
            "receipt visible: {props}"
        );
        assert!(props["rdlt.commit-seq"].is_string());
    }
}

/// Shape 2: partitioned table — pyiceberg sees the partition spec and
/// the same exact count.
#[tokio::test(flavor = "multi_thread")]
async fn pyiceberg_reads_partitioned() {
    let Some(python) = skip_no_venv() else {
        return;
    };
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let source = MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![
            MemoryBatch::new(vec![
                json!({"id": 1, "region": "eu"}),
                json!({"id": 2, "region": "us"}),
                json!({"id": 3, "region": "eu"}),
            ])
            .with_checkpoint(3),
        ],
    )]);
    let config = fixture.config("interop_part").with_table(
        "events",
        TableOptions::new()
            .with_partition(PartitionField::new("region", PartitionTransform::Identity)),
    );
    let dest = IcebergDest::from_config(config).expect("dest");
    Engine::new(EngineConfig::new("ice-interop-p"), source, dest)
        .run()
        .await
        .expect("run");

    let doc = read_back(&python, &fixture, "interop_part", "events").await;
    assert_eq!(doc["count"], 3);
    assert_eq!(
        doc["partition_fields"],
        json!(["region"]),
        "identity partition spec visible to pyiceberg"
    );
}

/// Shape 3: post-additive-drift — a second run adds a nullable column;
/// pyiceberg reads the evolved schema and the full row count.
#[tokio::test(flavor = "multi_thread")]
async fn pyiceberg_reads_after_additive_drift() {
    let Some(python) = skip_no_venv() else {
        return;
    };
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let dest = IcebergDest::from_config(fixture.config("interop_drift")).expect("dest");
    let run1 = MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![MemoryBatch::new(vec![json!({"id": 1})]).with_checkpoint(1)],
    )]);
    Engine::new(EngineConfig::new("ice-drift"), run1, dest.clone())
        .run()
        .await
        .expect("run 1");
    // Run 2 drifts: a new column appears mid-stream.
    let run2 = MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![
            MemoryBatch::new(vec![json!({"id": 1})]).with_checkpoint(1),
            MemoryBatch::new(vec![json!({"id": 2, "note": "drifted"})]).with_checkpoint(2),
        ],
    )]);
    Engine::new(EngineConfig::new("ice-drift"), run2, dest)
        .run()
        .await
        .expect("run 2");

    let doc = read_back(&python, &fixture, "interop_drift", "events").await;
    assert_eq!(doc["count"], 2);
    let columns: Vec<&str> = doc["columns"]
        .as_array()
        .expect("columns")
        .iter()
        .map(|c| c["name"].as_str().expect("name"))
        .collect();
    assert!(
        columns.contains(&"id") && columns.contains(&"note"),
        "evolved schema visible: {columns:?}"
    );
}
