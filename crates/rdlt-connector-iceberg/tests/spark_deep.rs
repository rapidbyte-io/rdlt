//! T014 Spark read-back (contract ID8 — DEEP tier only, `make test
//! TARGET=deep`). The reference JVM implementation reads rdlt-written
//! tables through the SAME REST catalog. Gated on RDLT_DEEP=1 (Spark
//! image + Maven-resolved Iceberg runtime are heavyweight); skips
//! visibly otherwise, and without a container runtime.

mod common;

use common::CatalogFixture;
use rdlt_connector::StreamSpec;
use rdlt_connector_iceberg::{IcebergDest, PartitionField, PartitionTransform, TableOptions};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream};
use serde_json::json;

fn spark_readback(
    fixture: &CatalogFixture,
    namespace: &str,
    table: &str,
) -> (u64, Vec<(String, String)>) {
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("repo root")
        .join("tools/interop/spark_readback.sh");
    let output = std::process::Command::new(&script)
        .args([
            fixture.catalog_uri.as_str(),
            common::WAREHOUSE,
            &format!("{}:{}", common::CLIENT_ID, common::CLIENT_SECRET),
            namespace,
            table,
            fixture.s3_endpoint.as_str(),
            common::S3_KEY,
            common::S3_SECRET,
        ])
        .output()
        .expect("spawn spark readback");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "spark readback failed:\n{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut count = None;
    let mut columns = Vec::new();
    for line in stdout.lines() {
        if let Some(value) = line.trim().strip_prefix("COUNT=") {
            count = Some(value.parse::<u64>().expect("count"));
        }
        // DESCRIBE rows print as name<TAB>type[<TAB>comment]; the
        // partition-info section starts with `#` markers — skip those.
        if !line.starts_with('#')
            && !line.starts_with("COUNT=")
            && let Some((name, rest)) = line.split_once('\t')
        {
            let ty = rest.split('\t').next().unwrap_or_default();
            if !name.trim().is_empty() && !ty.trim().is_empty() {
                columns.push((name.trim().to_owned(), ty.trim().to_owned()));
            }
        }
    }
    (count.expect("COUNT line in spark output"), columns)
}

/// The three shapes of the pyiceberg cell, read by Spark: plain append
/// ×2 commits, identity-partitioned, and post-additive-drift.
#[tokio::test(flavor = "multi_thread")]
async fn spark_reads_all_three_shapes() {
    if std::env::var("RDLT_DEEP").is_err() {
        eprintln!("SKIP: RDLT_DEEP not set — Spark deep-tier cell not run");
        return;
    }
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };

    // Shape 1: plain append across two commits.
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
    let dest = IcebergDest::from_config(fixture.config("spark_plain")).expect("dest");
    Engine::new(EngineConfig::new("ice-spark"), source, dest)
        .run()
        .await
        .expect("plain run");

    // Shape 2: identity-partitioned.
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
    let config = fixture.config("spark_part").with_table(
        "events",
        TableOptions::new()
            .with_partition(PartitionField::new("region", PartitionTransform::Identity)),
    );
    let dest = IcebergDest::from_config(config).expect("dest");
    Engine::new(EngineConfig::new("ice-spark-p"), source, dest)
        .run()
        .await
        .expect("partitioned run");

    // Shape 3: additive drift across two runs.
    let dest = IcebergDest::from_config(fixture.config("spark_drift")).expect("dest");
    let run1 = MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![MemoryBatch::new(vec![json!({"id": 1})]).with_checkpoint(1)],
    )]);
    Engine::new(EngineConfig::new("ice-spark-d"), run1, dest.clone())
        .run()
        .await
        .expect("drift run 1");
    let run2 = MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![
            MemoryBatch::new(vec![json!({"id": 1})]).with_checkpoint(1),
            MemoryBatch::new(vec![json!({"id": 2, "note": "drifted"})]).with_checkpoint(2),
        ],
    )]);
    Engine::new(EngineConfig::new("ice-spark-d"), run2, dest)
        .run()
        .await
        .expect("drift run 2");

    // Spark reads all three back.
    let (count, columns) = spark_readback(&fixture, "spark_plain", "events");
    assert_eq!(count, 3);
    assert!(
        columns.iter().any(|(n, t)| n == "id" && t == "bigint")
            && columns.iter().any(|(n, t)| n == "name" && t == "string"),
        "plain columns: {columns:?}"
    );

    let (count, columns) = spark_readback(&fixture, "spark_part", "events");
    assert_eq!(count, 3);
    assert!(
        columns.iter().any(|(n, _)| n == "region"),
        "partitioned columns: {columns:?}"
    );

    let (count, columns) = spark_readback(&fixture, "spark_drift", "events");
    assert_eq!(count, 2);
    assert!(
        columns.iter().any(|(n, _)| n == "note"),
        "drifted columns: {columns:?}"
    );
}
