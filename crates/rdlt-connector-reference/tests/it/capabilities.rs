//! What the reference destinations declare they store.

use std::collections::BTreeSet;

use rdlt_connector::{ConnectContext, Destination, TypeKind as K, destination_factory};
use rdlt_connector_reference::{FilesDestination, SqliteDestination};
use serde_json::json;

async fn files(format: &str) -> BTreeSet<K> {
    let root = tempfile::tempdir().expect("a temporary directory");
    let destination: Box<dyn Destination> = destination_factory::<FilesDestination>()
        .connect(
            json!({ "root": root.path(), "format": format }),
            ConnectContext::new(),
        )
        .await
        .expect("the files destination connects");
    destination.capabilities().types.clone()
}

#[tokio::test]
async fn sqlite_widens_each_integer_to_every_wider_one_and_float32_to_float64() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let destination: Box<dyn Destination> = destination_factory::<SqliteDestination>()
        .connect(
            json!({ "path": directory.path().join("widen.db") }),
            ConnectContext::new(),
        )
        .await
        .expect("the sqlite destination connects");
    let expected = BTreeSet::from([
        (K::Int8, K::Int16),
        (K::Int8, K::Int32),
        (K::Int8, K::Int64),
        (K::Int16, K::Int32),
        (K::Int16, K::Int64),
        (K::Int32, K::Int64),
        (K::Float32, K::Float64),
    ]);
    assert_eq!(
        destination.capabilities().schema_changes.widenings,
        expected
    );
}

#[tokio::test]
async fn json_lines_keep_the_types_json_has_a_form_for_and_arrow_keeps_every_type() {
    let json_lines = BTreeSet::from([
        K::Bool,
        K::Int8,
        K::Int16,
        K::Int32,
        K::Int64,
        K::Float32,
        K::Float64,
        K::Utf8,
        K::Struct,
        K::List,
    ]);
    assert_eq!(files("jsonl").await, json_lines);
    let mut arrow = json_lines;
    arrow.extend([
        K::Decimal,
        K::Binary,
        K::Date,
        K::Time,
        K::Timestamp,
        K::Duration,
        K::Uuid,
        K::Json,
    ]);
    assert_eq!(files("arrow").await, arrow);
}
