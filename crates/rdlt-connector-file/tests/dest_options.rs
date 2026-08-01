//! Feature 015 US3 (T011/T012): the file destination's new vocabulary on
//! LOCAL storage — jsonl format, partition_by, config validation. (The
//! object-store legs ride s3_live.rs.)

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rdlt_connector::core::{
    ColumnDef, ColumnType, LoadId, LogicalType, PipelineId, Provenance, TableName, TableSchema,
    WriteMode,
};
use rdlt_connector::{Destination, OpenContext};
use rdlt_connector_file::dest::{DestFormat, FileDest, FileDestConfig};
use rdlt_testkit::commit_meta_for;

fn schema_for(table: &str) -> TableSchema {
    TableSchema {
        table: TableName::new(table),
        parent: None,
        columns: vec![
            ColumnDef {
                name: "id".into(),
                column_type: ColumnType::scalar(LogicalType::Int64),
                nullable: false,
                provenance: Provenance::Inferred,
            },
            ColumnDef {
                name: "day".into(),
                column_type: ColumnType::scalar(LogicalType::Utf8),
                nullable: true,
                provenance: Provenance::Inferred,
            },
        ],
    }
}

fn batch(ids: &[i64], days: &[Option<&str>]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("day", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(days.to_vec())),
        ],
    )
    .expect("batch")
}

async fn run_load(dest: &FileDest, rows: RecordBatch) {
    let pipeline = PipelineId::new("p");
    let load = LoadId::new("load-a");
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    s.ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    s.write(&TableName::new("events"), rows)
        .await
        .expect("write");
    s.commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");
}

/// jsonl output: same staging/rename protocol, line-count totals, valid
/// NDJSON content.
#[tokio::test]
async fn jsonl_format_writes_ndjson_parts() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dest = FileDest::from_config(
        FileDestConfig::new(dir.path().to_string_lossy()).with_format(DestFormat::Jsonl),
    )
    .expect("open");
    run_load(&dest, batch(&[1, 2], &[Some("d1"), Some("d2")])).await;
    assert_eq!(dest.count_rows("events").expect("count"), 2);
    let part = dir.path().join("events/part-load-a-1-0.jsonl");
    let body = std::fs::read_to_string(&part).expect("part exists");
    let first: serde_json::Value =
        serde_json::from_str(body.lines().next().expect("line")).expect("valid NDJSON");
    assert_eq!(first["id"], 1);
}

/// partition_by: one prefix per value, rows in exactly one partition set,
/// NULLs under __null__.
#[tokio::test]
async fn partition_by_splits_rows() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dest = FileDest::from_config(
        FileDestConfig::new(dir.path().to_string_lossy()).with_partition_by("day"),
    )
    .expect("open");
    run_load(
        &dest,
        batch(&[1, 2, 3, 4], &[Some("d1"), Some("d2"), Some("d1"), None]),
    )
    .await;
    assert_eq!(dest.count_rows("events").expect("count"), 4);
    let count = |sub: &str| {
        std::fs::read_dir(dir.path().join("events").join(sub))
            .map(|entries| entries.count())
            .unwrap_or(0)
    };
    assert_eq!(count("d1"), 1, "one part file for d1 (2 rows)");
    assert_eq!(count("d2"), 1);
    assert_eq!(count("__null__"), 1, "NULLs land under __null__");
}

/// A partition column absent from the schema is typed, naming the column.
#[tokio::test]
async fn missing_partition_column_is_typed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dest = FileDest::from_config(
        FileDestConfig::new(dir.path().to_string_lossy()).with_partition_by("ghost"),
    )
    .expect("open");
    let pipeline = PipelineId::new("p");
    let load = LoadId::new("load-a");
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    s.ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    let err = s
        .write(&TableName::new("events"), batch(&[1], &[Some("d1")]))
        .await
        .expect_err("missing column")
        .to_string();
    assert!(
        err.contains("ghost") && err.contains("partition_by"),
        "{err}"
    );
}

/// Config validation: empty path / empty partition column typed.
#[test]
fn dest_config_validation_is_typed() {
    let err = FileDest::from_config(FileDestConfig::new("")).expect_err("empty path");
    assert!(err.to_string().contains("path"), "{err}");
    let err = FileDest::from_config(FileDestConfig::new("out").with_partition_by(""))
        .expect_err("empty partition");
    assert!(err.to_string().contains("partition_by"), "{err}");
}

/// 015 review finding 1: Replace truncation never deletes files this
/// destination does not own. The FROZEN local-parquet config keeps the
/// exact pre-015 rule (top-level *.parquet, any name); new configs delete
/// only their own `part-*.<ext>` files.
#[tokio::test]
async fn replace_truncation_spares_user_files() {
    use rdlt_connector::core::WriteMode;
    use rdlt_connector::{Destination, OpenContext};

    // Frozen config: top-level *.parquet goes (old rule), user jsonl and
    // nested dirs SURVIVE.
    let dir = tempfile::tempdir().expect("tempdir");
    let table_dir = dir.path().join("events");
    std::fs::create_dir_all(table_dir.join("user-subdir")).expect("mkdir");
    std::fs::write(table_dir.join("stray.parquet"), b"not-ours-but-old-rule").expect("seed");
    std::fs::write(table_dir.join("user.jsonl"), b"{\"keep\":true}\n").expect("seed");
    std::fs::write(
        table_dir.join("user-subdir/data.parquet"),
        b"nested-user-file",
    )
    .expect("seed");
    let dest =
        FileDest::from_config(FileDestConfig::new(dir.path().to_string_lossy())).expect("open");
    let pipeline = PipelineId::new("p");
    let load = LoadId::new("load-r");
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    s.ensure_table(&schema_for("events"), &WriteMode::Replace)
        .await
        .expect("ensure");
    s.write(&TableName::new("events"), batch(&[1], &[Some("d1")]))
        .await
        .expect("write");
    s.commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");
    assert!(
        !table_dir.join("stray.parquet").exists(),
        "frozen rule: top-level *.parquet is truncated"
    );
    assert!(table_dir.join("user.jsonl").exists(), "user jsonl survives");
    assert!(
        table_dir.join("user-subdir/data.parquet").exists(),
        "nested user files survive"
    );

    // New config (jsonl format): ONLY part-*.jsonl is ours.
    let dir = tempfile::tempdir().expect("tempdir");
    let table_dir = dir.path().join("events");
    std::fs::create_dir_all(&table_dir).expect("mkdir");
    std::fs::write(table_dir.join("user.jsonl"), b"{\"keep\":true}\n").expect("seed");
    std::fs::write(table_dir.join("part-old-1-0.jsonl"), b"{\"old\":true}\n").expect("seed");
    let dest = FileDest::from_config(
        FileDestConfig::new(dir.path().to_string_lossy()).with_format(DestFormat::Jsonl),
    )
    .expect("open");
    let load = LoadId::new("load-r2");
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    s.ensure_table(&schema_for("events"), &WriteMode::Replace)
        .await
        .expect("ensure");
    s.write(&TableName::new("events"), batch(&[2], &[Some("d1")]))
        .await
        .expect("write");
    s.commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");
    assert!(
        !table_dir.join("part-old-1-0.jsonl").exists(),
        "our part files are truncated"
    );
    assert!(table_dir.join("user.jsonl").exists(), "user jsonl survives");
}

/// A Replace load must clear what THIS destination wrote for the table, even
/// when the format or partitioning changed since. Exercised through the real
/// commit path so the frozen-rule selector is in play: unit-testing the
/// ownership predicate alone passes while this fails, because the default local
/// parquet config routes through the frozen rule.
#[tokio::test]
async fn replace_clears_earlier_loads_written_in_another_shape() {
    let dir = tempfile::tempdir().expect("tempdir");
    let table_dir = dir.path().join("events");
    std::fs::create_dir_all(table_dir.join("eu")).expect("mkdir");

    // What earlier loads wrote: partitioned jsonl, and unpartitioned jsonl.
    std::fs::write(table_dir.join("eu/part-old-1-0.jsonl"), b"{\"old\":1}\n").expect("seed");
    std::fs::write(table_dir.join("part-old-1-1.jsonl"), b"{\"old\":2}\n").expect("seed");
    // And an earlier parquet load, partitioned.
    std::fs::write(table_dir.join("eu/part-old-2-0.parquet"), b"PAR1").expect("seed");

    // Now reconfigured to the DEFAULT: local, parquet, unpartitioned.
    let pipeline = PipelineId::new("p");
    let load = LoadId::new("new");
    let config = FileDestConfig::new(dir.path().to_string_lossy().to_string());
    let mut s = FileDest::from_config(config)
        .expect("dest")
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    s.ensure_table(&schema_for("events"), &WriteMode::Replace)
        .await
        .expect("ensure");
    s.write(&TableName::new("events"), batch(&[1], &[Some("d1")]))
        .await
        .expect("write");
    s.commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");

    for stale in [
        "eu/part-old-1-0.jsonl",
        "part-old-1-1.jsonl",
        "eu/part-old-2-0.parquet",
    ] {
        assert!(
            !table_dir.join(stale).exists(),
            "`{stale}` was written by this destination and must not survive a Replace"
        );
    }
}

/// The ownership rule must never claim a dataset this destination did not write.
/// `part-0.parquet` is pyarrow's and Spark's DEFAULT output basename, so a bare
/// `part-` prefix test deletes a user's own export sitting under the table.
#[tokio::test]
async fn replace_never_deletes_a_foreign_dataset() {
    let dir = tempfile::tempdir().expect("tempdir");
    let table_dir = dir.path().join("events");
    std::fs::create_dir_all(table_dir.join("spark-export")).expect("mkdir");
    std::fs::write(table_dir.join("spark-export/part-0.parquet"), b"PAR1").expect("seed");
    std::fs::write(
        table_dir.join("spark-export/part-00000-8f3a-c000.snappy.parquet"),
        b"PAR1",
    )
    .expect("seed");

    let pipeline = PipelineId::new("p");
    let load = LoadId::new("new");
    let config = FileDestConfig::new(dir.path().to_string_lossy().to_string())
        .with_format(DestFormat::Jsonl);
    let mut s = FileDest::from_config(config)
        .expect("dest")
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    s.ensure_table(&schema_for("events"), &WriteMode::Replace)
        .await
        .expect("ensure");
    s.write(&TableName::new("events"), batch(&[1], &[Some("d1")]))
        .await
        .expect("write");
    s.commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");

    for foreign in [
        "spark-export/part-0.parquet",
        "spark-export/part-00000-8f3a-c000.snappy.parquet",
    ] {
        assert!(
            table_dir.join(foreign).exists(),
            "`{foreign}` was not written by this destination and must survive"
        );
    }
}
