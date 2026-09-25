use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Int32Array, RecordBatch,
    StringArray,
};
use arrow_schema::DataType;
use rdlt_connector::{
    CommitMeta, CommitSeq, ConnectContext, ConnectorErrorKind, Destination, Field, LoadId,
    LogicalType, OpenContext, OpenedSession, PipelineId, SchemaVersion, SegmentId, SegmentSet,
    TableChange, TablePath, TableRef, TableSchema, destination_factory,
};
use rdlt_connector_reference::{SqliteDestination, sqlite};
use serde_json::json;

async fn connect(path: &std::path::Path) -> Box<dyn Destination> {
    destination_factory::<SqliteDestination>()
        .connect(json!({ "path": path }), ConnectContext::new())
        .await
        .expect("the sqlite destination connects")
}

fn context() -> OpenContext {
    OpenContext {
        pipeline: PipelineId::parse("values").expect("valid pipeline id"),
        load_id: LoadId::from_parts(UNIX_EPOCH, 1),
    }
}

async fn open(destination: &dyn Destination) -> OpenedSession {
    destination
        .open(&context())
        .await
        .expect("the database opens")
}

fn table() -> TableRef {
    TableRef {
        path: TablePath::new(["values"]).expect("valid table path"),
        name: "values".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    }
}

fn commit(opened: &OpenedSession) -> CommitMeta {
    CommitMeta {
        load_id: LoadId::from_parts(UNIX_EPOCH, 1),
        commit_seq: CommitSeq::FIRST,
        epoch: opened.epoch,
        segments: [SegmentId(1)].into_iter().collect::<SegmentSet>(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
    }
}

/// A batch holding each column type SQLite stores, with a null in each but the integers.
fn storable() -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "flag",
            Arc::new(BooleanArray::from(vec![Some(true), None])) as ArrayRef,
        ),
        (
            "small",
            Arc::new(Int32Array::from(vec![Some(7), Some(-1)])) as _,
        ),
        (
            "ratio",
            Arc::new(Float32Array::from(vec![Some(0.5), None])) as _,
        ),
        (
            "name",
            Arc::new(StringArray::from(vec![Some("ann"), None])) as _,
        ),
        (
            "bytes",
            Arc::new(BinaryArray::from(vec![Some(&b"\x00\x01"[..]), None])) as _,
        ),
    ])
    .expect("the batch is valid")
}

/// Commits one batch of every column type SQLite stores to a new database; returns its path.
async fn committed(directory: &tempfile::TempDir) -> std::path::PathBuf {
    let path = directory.path().join("values.db");
    let destination = connect(&path).await;
    let mut opened = open(destination.as_ref()).await;
    let schema = TableSchema::new(vec![
        Field::new("flag", LogicalType::Bool, true),
        Field::new("small", LogicalType::Int32, true),
        Field::new("ratio", LogicalType::Float32, true),
        Field::new("name", LogicalType::Utf8, true),
        Field::new("bytes", LogicalType::Binary, true),
    ])
    .expect("the schema is valid");
    let create = TableChange::Create {
        table: table(),
        schema,
    };
    opened
        .session
        .apply_schema(&create)
        .await
        .expect("the table is created");
    let batch = storable();
    let mut writer = opened
        .session
        .writer(&table())
        .await
        .expect("a writer opens");
    writer
        .write(SegmentId(1), batch)
        .await
        .expect("the write stages");
    let stats = writer.flush().await.expect("the flush stages");
    assert_eq!(stats.rows, 2);
    let receipt = opened
        .session
        .commit(&commit(&opened))
        .await
        .expect("the commit lands");
    assert_eq!((receipt.rows, receipt.bytes), (stats.rows, stats.bytes));
    path
}

#[tokio::test]
async fn values_read_back_as_their_storage_class() {
    let directory = tempfile::tempdir().unwrap();
    let path = committed(&directory).await;
    let [published] = &sqlite::published(&path, "values").unwrap()[..] else {
        panic!("one batch reads back")
    };
    let types: Vec<&DataType> = published
        .schema_ref()
        .fields()
        .iter()
        .map(|field| field.data_type())
        .collect();
    assert_eq!(
        types,
        [
            &DataType::Boolean,
            &DataType::Int64,
            &DataType::Float64,
            &DataType::Utf8,
            &DataType::Binary
        ]
    );
    let column = |name: &str| published.column_by_name(name).unwrap();
    assert_eq!(
        column("flag").as_boolean().iter().collect::<Vec<_>>(),
        [Some(true), None]
    );
    assert_eq!(
        column("small")
            .as_primitive::<Int64Type>()
            .iter()
            .collect::<Vec<_>>(),
        [Some(7), Some(-1)]
    );
    assert_eq!(
        column("ratio")
            .as_primitive::<Float64Type>()
            .iter()
            .collect::<Vec<_>>(),
        [Some(0.5), None]
    );
    assert_eq!(
        column("bytes")
            .as_binary::<i32>()
            .iter()
            .collect::<Vec<_>>(),
        [Some(&b"\x00\x01"[..]), None]
    );
    assert!(sqlite::published(&path, "missing").unwrap().is_empty());
}

#[tokio::test]
async fn a_column_of_a_type_sqlite_does_not_store_fails_the_flush() {
    let directory = tempfile::tempdir().unwrap();
    let destination = connect(&directory.path().join("dates.db")).await;
    let mut opened = open(destination.as_ref()).await;
    let create = TableChange::Create {
        table: table(),
        schema: TableSchema::new(vec![Field::new("day", LogicalType::Utf8, true)]).unwrap(),
    };
    opened.session.apply_schema(&create).await.unwrap();
    let batch =
        RecordBatch::try_from_iter([("day", Arc::new(Date32Array::from(vec![1])) as ArrayRef)])
            .unwrap();
    let mut writer = opened.session.writer(&table()).await.unwrap();
    writer.write(SegmentId(1), batch).await.unwrap();
    let error = writer.flush().await.unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
}

#[tokio::test]
async fn a_database_that_cannot_be_opened_is_a_configuration_error() {
    let directory = tempfile::tempdir().unwrap();
    let destination = connect(&directory.path().join("missing").join("x.db")).await;
    let error = destination.check().await.unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Config);
    let error = destination.open(&context()).await.unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Config);
}
