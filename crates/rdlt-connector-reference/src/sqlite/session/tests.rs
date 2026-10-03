//! A session's writers index the tables a commit finds rows in by key, so a commit changes no
//! table or index.

use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field as ArrowField, Schema};
use rdlt_connector::{
    ChildTable, CommitMeta, CommitSeq, ConnectContext, Destination, Field, LoadId, LogicalType,
    MergeKey, OpenContext, PipelineId, RootKey, SEQ_COLUMN, SchemaVersion, SegmentId, TableChange,
    TablePath, TableRef, TableSchema, destination_factory,
};
use serde_json::json;

use super::super::{SqliteDestination, database};

fn table(name: &str, merge: MergeKey) -> TableRef {
    TableRef {
        path: TablePath::new([name]).expect("a valid table path"),
        name: name.into(),
        version: SchemaVersion(1),
        generation: None,
        merge: Some(merge),
    }
}

/// The table `roots`, merging by `id`, and its child table `items`, whose rows name their root
/// in `root`.
fn roots_and_items() -> (TableRef, TableRef) {
    let roots = MergeKey {
        columns: vec!["id".into()],
        seq: SEQ_COLUMN.into(),
        root: None,
        changes: None,
        history: None,
    };
    let items = MergeKey {
        columns: vec!["root".into()],
        seq: SEQ_COLUMN.into(),
        root: Some(RootKey {
            table: "roots".into(),
            id: "id".into(),
            seq: SEQ_COLUMN.into(),
        }),
        changes: None,
        history: None,
    };
    (table("roots", roots), table("items", items))
}

/// A batch of one row whose `key` column holds 1.
fn batch(key: &str) -> RecordBatch {
    let schema = Schema::new(vec![
        ArrowField::new(key, DataType::Int64, false),
        ArrowField::new(SEQ_COLUMN, DataType::Binary, false),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1])),
        Arc::new(BinaryArray::from_iter_values([[0_u8; 16]])),
    ];
    RecordBatch::try_new(Arc::new(schema), columns).expect("a valid batch")
}

/// The name and table of every table and index in the database at `path`, in order.
fn schema_objects(path: &std::path::Path) -> Vec<(String, String)> {
    let connection = database::connect(path).expect("the database opens");
    let mut statement = connection
        .prepare("SELECT name, tbl_name FROM sqlite_master ORDER BY name")
        .expect("the listing prepares");
    statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .expect("the listing runs")
        .collect::<Result<_, _>>()
        .expect("the listing reads")
}

/// The creation of `table`, keyed by `key`.
fn create(table: &TableRef, key: &str) -> TableChange {
    let fields = vec![
        Field::new(key, LogicalType::Int64, false),
        Field::new(SEQ_COLUMN, LogicalType::Binary, false),
    ];
    TableChange::Create {
        table: table.clone(),
        schema: TableSchema::new(fields).expect("the schema is valid"),
    }
}

#[tokio::test]
async fn writers_index_what_a_commit_finds_by_key_and_the_commit_changes_no_schema() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("indexes.db");
    let destination: Box<dyn Destination> = destination_factory::<SqliteDestination>()
        .connect(json!({ "path": path }), ConnectContext::new())
        .await
        .expect("the destination connects");
    let context = OpenContext {
        pipeline: PipelineId::parse("indexes").expect("a valid pipeline id"),
        load_id: LoadId::from_parts(UNIX_EPOCH, 1),
    };
    let mut opened = destination.open(&context).await.expect("a session opens");
    let (roots, items) = roots_and_items();
    for (table, key) in [(&roots, "id"), (&items, "root")] {
        let create = create(table, key);
        opened
            .session
            .apply_schema(&create)
            .await
            .expect("the table is created");
        let mut writer = opened.session.writer(table).await.expect("a writer opens");
        writer
            .write(SegmentId(1), batch(key))
            .await
            .expect("the write buffers");
        writer.flush().await.expect("the flush stages");
    }
    let staged = schema_objects(&path);
    for (index, table) in [
        ("_rdlt_key__roots", "roots"),
        ("_rdlt_root__items", "items"),
    ] {
        assert!(
            staged.contains(&(index.to_owned(), table.to_owned())),
            "{index} on {table}: {staged:?}"
        );
    }
    let meta = CommitMeta {
        load_id: context.load_id,
        commit_seq: CommitSeq::FIRST,
        epoch: opened.epoch,
        segments: [SegmentId(1)].into_iter().collect(),
        abandoned: rdlt_connector::SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: vec![ChildTable {
            table: "items".into(),
            merge: items.merge.clone().expect("a merge key"),
        }],
        drop_tables: Vec::new(),
        horizon: None,
    };
    let receipt = opened
        .session
        .commit(&meta)
        .await
        .expect("the commit lands");
    assert_eq!(receipt.rows, 2);
    assert_eq!(schema_objects(&path), staged);
}

#[test]
fn a_catalog_that_cannot_be_read_is_an_error_where_a_table_is_read_back() {
    use rdlt_connector::sqlgen::{SqlDialect as _, SqlPlanner, Sqlite};
    let directory = crate::scratch::tempdir().expect("a temporary directory");
    let path = directory.path().join("orders.db");
    let connection = database::connect(&path).expect("the database opens");
    let planner = SqlPlanner::try_new(Sqlite).expect("a planner");
    database::run_all(&connection, &planner.bootstrap()).expect("the catalog is made");
    // Statements as long as the query of a table's columns fail to prepare; that of what the
    // database takes a name for is shorter, and runs.
    let listing = Sqlite.columns("_rdlt_owners").sql.len();
    assert!(Sqlite.resolves("orders").sql.len() < listing - 1);
    let limit = i32::try_from(listing - 1).expect("a length");
    let length = rusqlite::limits::Limit::SQLITE_LIMIT_SQL_LENGTH;
    connection.set_limit(length, limit).expect("a limit");
    let read = super::owners::published(&connection, &planner, "orders");
    let failed = read.expect_err("the catalog cannot be read");
    assert_eq!(failed.code(), None, "{failed}");
    // Without the limit, a table no pipeline owns reads as none.
    connection.set_limit(length, 1_000_000).expect("a limit");
    let read = super::owners::published(&connection, &planner, "orders");
    assert_eq!(read.expect("the catalog reads"), None);
}
