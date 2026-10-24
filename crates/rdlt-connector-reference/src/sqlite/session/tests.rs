//! A session's writers index what a commit finds rows in, so a commit changes no table or
//! index: an appending writer the staging by who staged its rows, which a keyed writer drops, and
//! a keyed writer its tables by key.
//!
//! A commit runs a fixed count of statements for each table it publishes.

use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field as ArrowField, Schema};
use parking_lot::Mutex;
use rdlt_connector::sqlgen::{SqlPlanner, Sqlite};
use rdlt_connector::{
    ChildTable, CommitMeta, CommitSeq, ConnectContext, Destination, Epoch, Field, LoadId,
    LogicalType, MergeKey, OpenContext, OpenedSession, PipelineId, RootKey, SEQ_COLUMN,
    SchemaVersion, SegmentId, Session as _, TableChange, TablePath, TableRef, TableSchema,
    TableWriter as _, destination_factory,
};
use rusqlite::trace::TraceEvent;
use serde_json::json;

use super::super::database::Database;
use super::super::{SqliteDestination, database};
use super::SqliteSession;

fn table(name: &str, merge: Option<MergeKey>) -> TableRef {
    TableRef {
        path: TablePath::new([name]).expect("a valid table path"),
        name: name.into(),
        version: SchemaVersion(1),
        generation: None,
        merge,
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
    (table("roots", Some(roots)), table("items", Some(items)))
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

/// Checks that `staged`, the tables and indexes once `events`, `roots` and `items` are staged,
/// index the append table's staging by who staged its rows, and the keyed tables by key alone.
fn indexed_as_a_commit_finds_rows(staged: &[(String, String)]) {
    for (index, table) in [
        by_stager("events"),
        ("_rdlt_key__roots".to_owned(), "roots".to_owned()),
        ("_rdlt_root__items".to_owned(), "items".to_owned()),
    ] {
        assert!(
            staged.contains(&(index.clone(), table.clone())),
            "{index} on {table}: {staged:?}"
        );
    }
    for name in ["roots", "items"] {
        assert!(!staged.contains(&by_stager(name)), "{name}: {staged:?}");
    }
}

/// The index of the staging of the table `name` by who staged its rows, and that staging.
fn by_stager(name: &str) -> (String, String) {
    let planner = SqlPlanner::try_new(Sqlite).expect("a planner");
    let staging = planner.staging_table(name);
    (planner.staged_index_name(&staging), staging)
}

/// The first commit of `load` at `epoch`: segment 1, whose rows `items`, a child table, follows.
fn first_commit(load: LoadId, epoch: Epoch, items: &TableRef) -> CommitMeta {
    CommitMeta {
        load_id: load,
        commit_seq: CommitSeq::FIRST,
        epoch,
        segments: [SegmentId(1)].into_iter().collect(),
        abandoned: rdlt_connector::SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: vec![ChildTable {
            table: items.name.clone(),
            merge: items.merge.clone().expect("a merge key"),
        }],
        drop_tables: Vec::new(),
        horizon: None,
    }
}

/// A session of the pipeline `indexes` on the database at `path`, and the context it opened in.
async fn open_session(path: &std::path::Path) -> (OpenedSession, OpenContext) {
    let destination: Box<dyn Destination> = destination_factory::<SqliteDestination>()
        .connect(json!({ "path": path }), ConnectContext::new())
        .await
        .expect("the destination connects");
    let context = OpenContext {
        pipeline: PipelineId::parse("indexes").expect("a valid pipeline id"),
        load_id: LoadId::from_parts(UNIX_EPOCH, 1),
    };
    let opened = destination.open(&context).await.expect("a session opens");
    (opened, context)
}

#[tokio::test]
async fn a_staging_index_stands_while_its_table_s_writer_appends() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("appends.db");
    let (mut opened, _) = open_session(&path).await;
    let events = table("events", None);
    opened
        .session
        .apply_schema(&create(&events, "id"))
        .await
        .expect("the table is created");
    let key = MergeKey {
        columns: vec!["id".into()],
        seq: SEQ_COLUMN.into(),
        root: None,
        changes: None,
        history: None,
    };
    let keyed = table("events", Some(key));
    for (written, indexed) in [(&events, true), (&keyed, false), (&events, true)] {
        drop(
            opened
                .session
                .writer(written)
                .await
                .expect("a writer opens"),
        );
        let objects = schema_objects(&path);
        assert_eq!(
            objects.contains(&by_stager("events")),
            indexed,
            "{objects:?}"
        );
    }
}

#[tokio::test]
async fn writers_index_what_a_commit_finds_and_the_commit_changes_no_schema() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("indexes.db");
    let (mut opened, context) = open_session(&path).await;
    let (roots, items) = roots_and_items();
    let events = table("events", None);
    for (table, key) in [(&events, "id"), (&roots, "id"), (&items, "root")] {
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
    indexed_as_a_commit_finds_rows(&staged);
    let meta = first_commit(context.load_id, opened.epoch, &items);
    let receipt = opened
        .session
        .commit(&meta)
        .await
        .expect("the commit lands");
    assert_eq!(receipt.rows, 3);
    assert_eq!(schema_objects(&path), staged);
}

#[test]
fn a_catalog_that_cannot_be_read_is_an_error_where_a_table_is_read_back() {
    use rdlt_connector::sqlgen::SqlDialect as _;
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

/// The text of each statement started on a connection whose trace is [`started`].
static STARTED: Mutex<Vec<String>> = Mutex::new(Vec::new());

#[expect(
    clippy::needless_pass_by_value,
    reason = "a trace is a function of this signature"
)]
fn started(event: TraceEvent<'_>) {
    if let TraceEvent::Stmt(_, sql) = event {
        STARTED.lock().push(sql.to_owned());
    }
}

#[tokio::test]
async fn a_commit_of_an_append_a_merge_and_a_child_table_runs_24_statements() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let database = Database::open(directory.path().join("counted.db"))
        .await
        .expect("the database opens");
    let planner = Arc::new(SqlPlanner::try_new(Sqlite).expect("a planner"));
    let pipeline = PipelineId::parse("counted").expect("a valid pipeline id");
    let opened = SqliteSession::open(database.clone(), planner, pipeline)
        .await
        .expect("a session opens");
    let mut session = opened.session;
    let (roots, items) = roots_and_items();
    let events = table("events", None);
    for (table, key) in [(&events, "id"), (&roots, "id"), (&items, "root")] {
        session
            .apply_schema(&create(table, key))
            .await
            .expect("the table is created");
        let mut writer = session.writer(table).await.expect("a writer opens");
        writer
            .write(SegmentId(1), batch(key))
            .await
            .expect("the write buffers");
        writer.flush().await.expect("the flush stages");
    }
    let meta = first_commit(LoadId::from_parts(UNIX_EPOCH, 1), opened.epoch, &items);
    database.trace(Some(started));
    let receipt = session.commit(&meta).await.expect("the commit lands");
    database.trace(None);
    assert_eq!(receipt.rows, 3);
    // SQLite shows a program it runs within a statement, as a table-valued pragma's, as a
    // comment; the statements the destination runs are the rest.
    let started = STARTED.lock().clone();
    let run: Vec<&String> = started
        .iter()
        .filter(|sql| !sql.starts_with("--"))
        .collect();
    assert_eq!(run.len(), 24, "{run:#?}");
}
