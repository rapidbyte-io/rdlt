//! Partitioned output: bare-value directories, `__null__`, sanitized
//! slugs, and final names independent of cross-table arrival order.

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use rdlt_connector_file_v2::destination;
use rdlt_connector_sdk::spi::core::types::LogicalType;
use rdlt_connector_sdk::spi::core::{
    ColumnDef, ColumnType, LoadId, PipelineId, Provenance, TableName, TableSchema, WriteMode,
};
use rdlt_connector_sdk::spi::{Destination, OpenContext, RecordBatch};
use rdlt_testkit::commit_meta_for;

use super::common::local_dest;

fn partitioned_schema(table: &str) -> TableSchema {
    let col = |name: &str, ty| ColumnDef {
        name: name.to_owned(),
        column_type: ColumnType::scalar(ty),
        nullable: true,
        provenance: Provenance::Inferred,
    };
    TableSchema {
        table: TableName::new(table),
        parent: None,
        columns: vec![
            col("id", LogicalType::Int64),
            col("region", LogicalType::Utf8),
        ],
    }
}

fn regional_batch(values: &[Option<&str>]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("region", DataType::Utf8, true),
    ]));
    let ids: Vec<i64> = (0..values.len() as i64).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(values.to_vec())),
        ],
    )
    .expect("batch")
}

/// Bare-value partition directories with `__null__` and sanitized
/// slugs — never Hive `col=value`.
#[tokio::test]
async fn partition_directories_are_bare_sanitized_values() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = local_dest(dir.path()).with_partition_by("region");
    let dest = destination::Shell::new(config.clone()).expect("valid");
    let pipeline = PipelineId::new("partitions");
    let load = LoadId::new("load-a");
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    s.ensure_table(&partitioned_schema("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    s.write(
        &TableName::new("events"),
        regional_batch(&[Some("eu"), None, Some("us/east"), Some("eu")]),
    )
    .await
    .expect("write");
    s.commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");

    for part in [
        "events/eu/part-load-a-1-0.parquet",
        "events/__null__/part-load-a-1-0.parquet",
        "events/us_east/part-load-a-1-0.parquet",
    ] {
        assert!(dir.path().join(part).is_file(), "{part} must exist");
    }
    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        4,
        "partitioned rows all count"
    );
}

/// A missing partition column is refused at write time with the
/// frozen spelling.
#[tokio::test]
async fn a_missing_partition_column_refuses_at_write() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = local_dest(dir.path()).with_partition_by("zone");
    let dest = destination::Shell::new(config).expect("valid");
    let mut s = dest
        .open(OpenContext::new(
            PipelineId::new("missing-col"),
            LoadId::new("load-a"),
        ))
        .await
        .expect("open");
    s.ensure_table(&partitioned_schema("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    let err = s
        .write(&TableName::new("events"), regional_batch(&[Some("eu")]))
        .await
        .expect_err("refused")
        .to_string();
    assert!(
        err.contains("partition_by column `zone` does not exist in stream `events`"),
        "{err}"
    );
}

/// Final names count per TABLE+PARTITION: interleaving a second
/// table's writes cannot change the first table's final names.
#[tokio::test]
async fn final_names_independent_of_cross_table_arrival_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = local_dest(dir.path());
    let dest = destination::Shell::new(config).expect("valid");
    let pipeline = PipelineId::new("arrival-order");
    let load = LoadId::new("load-a");
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    for table in ["alpha", "beta"] {
        s.ensure_table(&partitioned_schema(table), &WriteMode::Append)
            .await
            .expect("ensure");
    }
    // Interleaved arrival: alpha, beta, alpha.
    s.write(&TableName::new("alpha"), regional_batch(&[Some("x")]))
        .await
        .expect("write");
    s.write(&TableName::new("beta"), regional_batch(&[Some("x")]))
        .await
        .expect("write");
    s.write(&TableName::new("alpha"), regional_batch(&[Some("x")]))
        .await
        .expect("write");
    s.commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");

    for part in [
        "alpha/part-load-a-1-0.parquet",
        "alpha/part-load-a-1-1.parquet",
        "beta/part-load-a-1-0.parquet",
    ] {
        assert!(
            dir.path().join(part).is_file(),
            "{part}: the index counts per table, not per arrival"
        );
    }
}
