//! The 031 round-1 session guards, driven straight through the SPI:
//! the append-only re-ensure rule and the positional write check. Both
//! refuse TYPED where the old behavior would have landed or dropped
//! values silently.

use rdlt_connector_sdk::spi::core::{
    ColumnDef, ColumnType, LoadId, LogicalType, PipelineId, Provenance, TableName, TableSchema,
    WriteMode,
};
use rdlt_connector_sdk::spi::{Destination, OpenContext};
use serde_json::json;

use super::common::{dest_with, ints, texts, unit};

/// A schema of nullable scalar columns, in the given order.
fn table_of(table: &str, columns: &[(&str, LogicalType)]) -> TableSchema {
    TableSchema {
        table: TableName::new(table),
        parent: None,
        columns: columns
            .iter()
            .map(|(name, scalar)| ColumnDef {
                name: (*name).to_owned(),
                column_type: ColumnType::scalar(*scalar),
                nullable: true,
                provenance: Provenance::Inferred,
            })
            .collect(),
    }
}

/// S1: within a session, re-ensure may only ADD columns. A re-ensure
/// whose schema DROPS a previously ensured column is refused typed,
/// naming the dropped column — while a purely additive re-ensure of
/// the same table stays legal.
#[tokio::test]
async fn a_re_ensure_that_drops_an_ensured_column_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("regress.duckdb");
    let dest = dest_with(&file, json!({}));
    let mut session = dest
        .open(OpenContext::new(
            PipelineId::new("guards"),
            LoadId::new("load-1"),
        ))
        .await
        .unwrap();

    let base = &[("id", LogicalType::Int64), ("name", LogicalType::Utf8)];
    session
        .ensure_table(&table_of("events", base), &WriteMode::Append)
        .await
        .expect("first ensure");
    // Additive drift stays legal — the widen/add path is untouched.
    session
        .ensure_table(
            &table_of(
                "events",
                &[
                    ("id", LogicalType::Int64),
                    ("name", LogicalType::Utf8),
                    ("flag", LogicalType::Bool),
                ],
            ),
            &WriteMode::Append,
        )
        .await
        .expect("additive re-ensure");

    // A schema that DROPS previously ensured columns is not evolution.
    let err = session
        .ensure_table(
            &table_of("events", &[("id", LogicalType::Int64)]),
            &WriteMode::Append,
        )
        .await
        .expect_err("a column-dropping re-ensure must refuse")
        .to_string();
    assert!(
        err.contains("re-ensure drops previously ensured columns (flag, name)")
            || err.contains("re-ensure drops previously ensured columns (name, flag)"),
        "names every dropped column: {err}"
    );
    assert!(
        err.contains("two streams colliding on one table"),
        "states the diagnosis: {err}"
    );
}

/// S4: the stage append is positional, so a batch whose columns are
/// reordered against the ensured schema is refused typed at the first
/// divergent position — values must never land in the wrong columns.
#[tokio::test]
async fn a_reordered_batch_is_refused_before_the_positional_append() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("reorder.duckdb");
    let dest = dest_with(&file, json!({}));
    let mut session = dest
        .open(OpenContext::new(
            PipelineId::new("guards"),
            LoadId::new("load-1"),
        ))
        .await
        .unwrap();
    session
        .ensure_table(
            &table_of(
                "kv",
                &[("id", LogicalType::Int64), ("v", LogicalType::Utf8)],
            ),
            &WriteMode::Append,
        )
        .await
        .expect("ensure");

    let table = TableName::new("kv");
    // The ensured order lands fine.
    session
        .write(
            &table,
            unit(&[("id", ints(&[Some(1)])), ("v", texts(&[Some("ok")]))]),
        )
        .await
        .expect("the ensured order appends");

    // The SAME columns, reordered: refused at position 0.
    let err = session
        .write(
            &table,
            unit(&[("v", texts(&[Some("swapped")])), ("id", ints(&[Some(2)]))]),
        )
        .await
        .expect_err("a reordered batch must refuse")
        .to_string();
    assert!(
        err.contains("batch column 0 is `v` but the ensured schema has `id`"),
        "names the first divergent position: {err}"
    );
    assert!(
        err.contains("the stage append is positional"),
        "states why: {err}"
    );
}
