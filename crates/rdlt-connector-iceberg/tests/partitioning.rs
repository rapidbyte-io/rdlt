//! T012 partitioning cells: partition_by → an Iceberg partition spec at
//! table create; partitioned writes fan out one data file per partition
//! value; the spec is visible in raw table metadata; a config that
//! disagrees with the live spec is typed. Live against Polaris + RUSTFS,
//! skip-not-fail.

mod common;

use common::CatalogFixture;
use rdlt_connector::StreamSpec;
use rdlt_connector_iceberg::{IcebergDest, PartitionField, PartitionTransform, TableOptions};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream};
use serde_json::json;

fn regional_source() -> MemorySource {
    MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![
            MemoryBatch::new(vec![
                json!({"id": 1, "region": "eu"}),
                json!({"id": 2, "region": "us"}),
                json!({"id": 3, "region": "eu"}),
                json!({"id": 4, "region": "us"}),
                json!({"id": 5, "region": "ap"}),
            ])
            .with_checkpoint(5),
        ],
    )])
}

/// Identity-partitioned writes land with one data file PER partition
/// value in the commit (the observable fanout layout) and the spec is
/// in the table metadata.
#[tokio::test(flavor = "multi_thread")]
async fn partitioned_writes_fan_out_and_spec_is_visible() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let config = fixture.config("part").with_table(
        "events",
        TableOptions::new()
            .with_partition(PartitionField::new("region", PartitionTransform::Identity)),
    );
    let dest = IcebergDest::from_config(config).expect("dest");
    let report = Engine::new(EngineConfig::new("ice-part"), regional_source(), dest)
        .run()
        .await
        .expect("partitioned run");
    assert_eq!(report.total_rows(), 5);

    let snapshots = fixture.snapshot_summaries("part", "events").await;
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0]["added-records"], "5");
    assert_eq!(
        snapshots[0]["added-data-files"], "3",
        "fanout: one data file per distinct region (eu/us/ap)"
    );

    let metadata = fixture.table_metadata("part", "events").await;
    let specs = metadata["metadata"]["partition-specs"]
        .as_array()
        .expect("partition-specs");
    let fields = specs
        .iter()
        .flat_map(|s| s["fields"].as_array().cloned().unwrap_or_default())
        .collect::<Vec<_>>();
    assert!(
        fields
            .iter()
            .any(|f| f["name"] == "region" && f["transform"] == "identity"),
        "identity spec visible in metadata: {specs:?}"
    );
}

/// A config whose partition_by disagrees with the LIVE table's spec is
/// a typed error — specs are fixed at creation, never silently re-specced.
#[tokio::test(flavor = "multi_thread")]
async fn partition_spec_mismatch_is_typed() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    // Create unpartitioned…
    let dest = IcebergDest::from_config(fixture.config("part_mismatch")).expect("dest");
    Engine::new(EngineConfig::new("ice-pm"), regional_source(), dest)
        .run()
        .await
        .expect("first run");
    // …then demand a partitioned spec for the same table. The source
    // must offer NEW rows past the committed cursor — the engine only
    // ensures tables for streams it will load.
    let config = fixture.config("part_mismatch").with_table(
        "events",
        TableOptions::new()
            .with_partition(PartitionField::new("region", PartitionTransform::Identity)),
    );
    let dest = IcebergDest::from_config(config).expect("dest");
    let more = MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![
            MemoryBatch::new(vec![
                json!({"id": 1, "region": "eu"}),
                json!({"id": 2, "region": "us"}),
                json!({"id": 3, "region": "eu"}),
                json!({"id": 4, "region": "us"}),
                json!({"id": 5, "region": "ap"}),
            ])
            .with_checkpoint(5),
            MemoryBatch::new(vec![json!({"id": 6, "region": "eu"})]).with_checkpoint(6),
        ],
    )]);
    let err = Engine::new(EngineConfig::new("ice-pm"), more, dest)
        .run()
        .await
        .expect_err("mismatch must be typed");
    let text = format!("{err}");
    assert!(
        text.contains("partition") && text.contains("fixed at creation"),
        "names the mismatch: {text}"
    );
}

/// Parameterized transforms live (parity D2 closed): truncate(1) fans
/// out by string prefix, bucket(4) hashes into at most 4 shards; both
/// specs land at create and are visible in raw metadata.
#[tokio::test(flavor = "multi_thread")]
async fn bucket_and_truncate_partition_live() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let config = fixture
        .config("part_param")
        .with_table(
            "events",
            TableOptions::new().with_partition(PartitionField::new(
                "region",
                PartitionTransform::Truncate(1),
            )),
        )
        .with_table(
            "clicks",
            TableOptions::new()
                .with_partition(PartitionField::new("id", PartitionTransform::Bucket(4))),
        );
    let source = MemorySource::new(vec![
        MemoryStream::new(
            StreamSpec::new("events"),
            vec![
                MemoryBatch::new(vec![
                    json!({"id": 1, "region": "eu-west"}),
                    json!({"id": 2, "region": "eu-north"}),
                    json!({"id": 3, "region": "us-east"}),
                    json!({"id": 4, "region": "ap-south"}),
                ])
                .with_checkpoint(4),
            ],
        ),
        MemoryStream::new(
            StreamSpec::new("clicks"),
            vec![
                MemoryBatch::new((1..=32).map(|i| json!({"id": i})).collect()).with_checkpoint(32),
            ],
        ),
    ]);
    let dest = IcebergDest::from_config(config).expect("dest");
    let report = Engine::new(EngineConfig::new("ice-part-param"), source, dest)
        .run()
        .await
        .expect("run");
    assert_eq!(report.total_rows(), 36);

    // truncate(1): eu/eu/us/ap → exactly 3 prefix partitions.
    let snapshots = fixture.snapshot_summaries("part_param", "events").await;
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0]["added-records"], "4");
    assert_eq!(
        snapshots[0]["added-data-files"], "3",
        "one data file per truncated prefix (eu/us/ap)"
    );

    // bucket(4): 32 ids hash into AT MOST 4 shards (>1 in practice).
    let snapshots = fixture.snapshot_summaries("part_param", "clicks").await;
    assert_eq!(snapshots[0]["added-records"], "32");
    let files: u64 = snapshots[0]["added-data-files"].parse().expect("count");
    assert!(
        (1..=4).contains(&files),
        "bucket(4) bounds the fanout: {files}"
    );

    // Both specs visible in raw metadata with their parameters.
    for (table, name, transform) in [
        ("events", "region_trunc", "truncate[1]"),
        ("clicks", "id_bucket", "bucket[4]"),
    ] {
        let metadata = fixture.table_metadata("part_param", table).await;
        let fields: Vec<serde_json::Value> = metadata["metadata"]["partition-specs"]
            .as_array()
            .expect("specs")
            .iter()
            .flat_map(|s| s["fields"].as_array().cloned().unwrap_or_default())
            .collect();
        assert!(
            fields
                .iter()
                .any(|f| f["name"] == name && f["transform"] == transform),
            "{table}: {name}/{transform} visible: {fields:?}"
        );
    }
}
