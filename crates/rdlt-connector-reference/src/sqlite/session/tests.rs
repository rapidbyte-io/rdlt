//! A session's writers index what a commit finds rows in, so a commit changes no table or
//! index: an appending writer the staging by who staged its rows, which a keyed writer drops, and
//! a keyed writer its tables by key.
//!
//! A commit runs a fixed count of statements for each table it publishes.

use std::sync::Arc;

use arrow_array::{ArrayRef, BinaryArray, Int8Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field as ArrowField, Schema};
use parking_lot::Mutex;
use rdlt_connector::sqlgen::{SqlPlanner, Sqlite};
use rdlt_connector::{
    COLUMN_MISSING, ChangeColumns, ChangeOp, ChildTable, CommitKey, CommitMeta, ConnectContext,
    ConnectorErrorKind, Deletion, Destination, Epoch, Field, GenerationId, LogicalType, MergeKey,
    OP_COLUMN, OpenContext, OpenedSession, PipelineId, RootKey, SEQ_COLUMN, SchemaVersion,
    SegmentId, Session as _, TableChange, TablePath, TableRef, TableSchema, TableWriter as _,
    UNCHANGED_COLUMN, destination_factory,
};
use rusqlite::trace::TraceEvent;
use serde_json::json;

use super::super::database::Database;
use super::super::{SqliteDestination, database};
use super::{Columns, SqliteSession};

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

/// The first commit of a session at `epoch` of a pipeline nothing landed in: segment 1, whose
/// rows `children`, child tables, follow.
fn first_commit(epoch: Epoch, children: &[&TableRef]) -> CommitMeta {
    CommitMeta {
        key: CommitKey { epoch, seq: 1 },
        after: None,
        segments: [SegmentId(1)].into_iter().collect(),
        abandoned: rdlt_connector::SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: children
            .iter()
            .map(|child| ChildTable {
                table: child.name.clone(),
                merge: child.merge.clone().expect("a merge key"),
            })
            .collect(),
        drop_tables: Vec::new(),
    }
}

/// A session of the pipeline `indexes` on the database at `path`.
async fn open_session(path: &std::path::Path) -> OpenedSession {
    let destination: Box<dyn Destination> = destination_factory::<SqliteDestination>()
        .connect(json!({ "path": path }), ConnectContext::new())
        .await
        .expect("the destination connects");
    let context = OpenContext {
        pipeline: PipelineId::parse("indexes").expect("a valid pipeline id"),
    };
    destination.open(&context).await.expect("a session opens")
}

#[tokio::test]
async fn a_staging_index_stands_while_its_table_s_writer_appends() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("appends.db");
    let mut opened = open_session(&path).await;
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
    let mut opened = open_session(&path).await;
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
    let meta = first_commit(opened.epoch, &[&items]);
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
async fn a_commit_of_an_append_a_merge_and_a_child_table_runs_23_statements_two_of_its_fence() {
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
    let meta = first_commit(opened.epoch, &[&items]);
    let noted = session.written.of(&meta.segments);
    let tables: Vec<&str> = noted.keys().map(|(table, _)| table.as_str()).collect();
    assert_eq!(tables, ["events", "items", "roots"]);
    database.trace(Some(started));
    let receipt = session.commit(&meta).await.expect("the commit lands");
    database.trace(None);
    assert_eq!(receipt.rows, 3);
    // Nothing is kept of the columns of what the commit published.
    assert_eq!(session.written.of(&meta.segments), Columns::new());
    // SQLite shows a program it runs within a statement, as a table-valued pragma's, as a
    // comment; the statements the destination runs are the rest.
    let started = STARTED.lock().clone();
    let run: Vec<&String> = started
        .iter()
        .filter(|sql| !sql.starts_with("--"))
        .collect();
    assert_eq!(run.len(), 23, "{run:#?}");
    // The fence row is read under the write lock, then given the commit's key: no other statement
    // of the commit reads or writes it.
    let fenced: Vec<&&String> = run
        .iter()
        .filter(|sql| sql.contains("_rdlt_epochs"))
        .collect();
    assert_eq!(fenced.len(), 2, "{fenced:#?}");
    assert!(fenced[0].starts_with("SELECT") && fenced[1].starts_with("UPDATE"));
}

/// Creates `table` of an `id` and a `name` in `opened`'s session and stages two rows of it in
/// each of `segments`.
async fn staged_names(opened: &mut OpenedSession, table: &TableRef, segments: &[u64]) {
    let fields = vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("name", LogicalType::Utf8, true),
    ];
    let create = TableChange::Create {
        table: table.clone(),
        schema: TableSchema::new(fields).expect("the schema is valid"),
    };
    opened
        .session
        .apply_schema(&create)
        .await
        .expect("the table is created");
    let written = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
        ("name", Arc::new(StringArray::from(vec!["a", "b"])) as _),
    ])
    .expect("a valid batch");
    let mut writer = opened.session.writer(table).await.expect("a writer opens");
    for segment in segments {
        writer
            .write(SegmentId(*segment), written.clone())
            .await
            .expect("the write buffers");
    }
    writer.flush().await.expect("the flush stages");
}

#[tokio::test]
async fn a_commit_whose_table_lost_a_column_it_staged_fails_and_publishes_nothing() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("dropped.db");
    let mut opened = open_session(&path).await;
    // The commit publishes `audit` before `events`, the table that loses a column.
    for name in ["audit", "events"] {
        staged_names(&mut opened, &table(name, None), &[1]).await;
    }
    let outside = rusqlite::Connection::open(&path).expect("the database opens");
    outside
        .execute("ALTER TABLE \"events\" DROP COLUMN \"name\"", [])
        .expect("the column is dropped");
    let meta = first_commit(opened.epoch, &[]);
    let error = opened
        .session
        .commit(&meta)
        .await
        .expect_err("the commit is refused");
    assert_eq!(
        (error.kind(), error.code()),
        (ConnectorErrorKind::Data, Some(COLUMN_MISSING)),
        "{error}"
    );
    let count = |table: &str| -> i64 {
        outside
            .query_row(&format!("SELECT COUNT(*) FROM \"{table}\""), [], |row| {
                row.get(0)
            })
            .expect("the table counts")
    };
    let planner = SqlPlanner::try_new(Sqlite).expect("a planner");
    for table in ["audit", "events"] {
        assert_eq!(count(table), 0, "{table} published");
        assert_eq!(count(&planner.staging_table(table)), 2, "{table} staged");
    }
}

/// The rows `table` publishes in the database at `path`.
fn published(path: &std::path::Path, table: &str) -> i64 {
    let connection = rusqlite::Connection::open(path).expect("the database opens");
    connection
        .query_row(&format!("SELECT COUNT(*) FROM \"{table}\""), [], |row| {
            row.get(0)
        })
        .expect("the table counts")
}

/// Commit `seq` of a session at `epoch`, of segment `segment` alone, after the session's commit
/// before it.
fn commit_of(epoch: Epoch, seq: u64, segment: u64) -> CommitMeta {
    CommitMeta {
        key: CommitKey { epoch, seq },
        after: (seq > 1).then_some(CommitKey {
            epoch,
            seq: seq - 1,
        }),
        segments: [SegmentId(segment)].into_iter().collect(),
        ..first_commit(epoch, &[])
    }
}

/// The table `events` merging a change stream by `id`, and a change inserting a row of it.
fn changed_events() -> (TableRef, RecordBatch) {
    let key = MergeKey {
        columns: vec!["id".into()],
        seq: SEQ_COLUMN.into(),
        root: None,
        changes: Some(ChangeColumns {
            op: OP_COLUMN.into(),
            unchanged: Some(UNCHANGED_COLUMN.into()),
            deletion: Deletion::Hard,
        }),
        history: None,
    };
    let change = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        (
            SEQ_COLUMN,
            Arc::new(BinaryArray::from_iter_values([[0_u8; 16]])) as _,
        ),
        (
            OP_COLUMN,
            Arc::new(Int8Array::from(vec![ChangeOp::Insert.code()])) as _,
        ),
        (
            UNCHANGED_COLUMN,
            Arc::new(BinaryArray::from(vec![None::<&[u8]>])) as _,
        ),
    ])
    .expect("a valid batch");
    (table("events", Some(key)), change)
}

#[tokio::test]
async fn an_append_after_a_change_merge_publishes_though_its_staging_keeps_the_merge_s_flags() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("switched.db");
    let mut opened = open_session(&path).await;
    let (merged, change) = changed_events();
    opened
        .session
        .apply_schema(&create(&merged, "id"))
        .await
        .expect("the table is created");
    let appended = table("events", None);
    for (segment, (written, batch)) in [(&merged, change), (&appended, batch("id"))]
        .into_iter()
        .enumerate()
    {
        let segment = u64::try_from(segment).expect("a segment") + 1;
        let mut writer = opened
            .session
            .writer(written)
            .await
            .expect("a writer opens");
        writer
            .write(SegmentId(segment), batch)
            .await
            .expect("the write buffers");
        writer.flush().await.expect("the flush stages");
        // Each segment is its own commit, its place among the session's.
        let meta = commit_of(opened.epoch, segment, segment);
        opened
            .session
            .commit(&meta)
            .await
            .expect("the commit lands");
    }
    assert_eq!(published(&path, "events"), 2);
}

#[tokio::test]
async fn an_append_after_an_abandoned_generation_publishes_though_its_staging_keeps_a_column() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("abandoned.db");
    let mut opened = open_session(&path).await;
    let events = table("events", None);
    let generation = TableRef {
        generation: Some(GenerationId(7)),
        ..events.clone()
    };
    opened
        .session
        .apply_schema(&create(&events, "id"))
        .await
        .expect("the table is created");
    // The generation gains a column, which its table's staging gains with it, and is never
    // committed.
    drop(
        opened
            .session
            .writer(&generation)
            .await
            .expect("a writer opens"),
    );
    let add = TableChange::AddColumn {
        table: generation,
        field: Field::new("extra", LogicalType::Int64, true),
    };
    opened
        .session
        .apply_schema(&add)
        .await
        .expect("the column is added");
    drop(opened);
    let mut opened = open_session(&path).await;
    let mut writer = opened
        .session
        .writer(&events)
        .await
        .expect("a writer opens");
    writer
        .write(SegmentId(1), batch("id"))
        .await
        .expect("the write buffers");
    writer.flush().await.expect("the flush stages");
    let meta = first_commit(opened.epoch, &[]);
    opened
        .session
        .commit(&meta)
        .await
        .expect("the commit lands");
    assert_eq!(published(&path, "events"), 1);
}

#[tokio::test]
async fn a_commit_into_a_generation_that_lost_a_column_it_staged_fails_and_publishes_nothing() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("generation.db");
    let mut opened = open_session(&path).await;
    let generation = TableRef {
        generation: Some(GenerationId(7)),
        ..table("events", None)
    };
    opened
        .session
        .apply_schema(&create(&table("events", None), "id"))
        .await
        .expect("the table is created");
    let mut writer = opened
        .session
        .writer(&generation)
        .await
        .expect("a writer opens");
    writer
        .write(SegmentId(1), batch("id"))
        .await
        .expect("the write buffers");
    writer.flush().await.expect("the flush stages");
    let planner = SqlPlanner::try_new(Sqlite).expect("a planner");
    let target = planner.target(&generation);
    let outside = rusqlite::Connection::open(&path).expect("the database opens");
    let drop = format!("ALTER TABLE \"{target}\" DROP COLUMN \"{SEQ_COLUMN}\"");
    outside.execute(&drop, []).expect("the column is dropped");
    let meta = first_commit(opened.epoch, &[]);
    let error = opened
        .session
        .commit(&meta)
        .await
        .expect_err("the commit is refused");
    assert_eq!(
        (error.kind(), error.code()),
        (ConnectorErrorKind::Data, Some(COLUMN_MISSING)),
        "{error}"
    );
    assert_eq!(published(&path, &target), 0);
}

#[tokio::test]
async fn a_column_dropped_between_two_commits_of_a_session_fails_the_second() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("between.db");
    let mut opened = open_session(&path).await;
    staged_names(&mut opened, &table("events", None), &[1, 2]).await;
    let first = commit_of(opened.epoch, 1, 1);
    opened
        .session
        .commit(&first)
        .await
        .expect("the commit lands");
    let outside = rusqlite::Connection::open(&path).expect("the database opens");
    outside
        .execute("ALTER TABLE \"events\" DROP COLUMN \"name\"", [])
        .expect("the column is dropped");
    let second = commit_of(opened.epoch, 2, 2);
    let error = opened
        .session
        .commit(&second)
        .await
        .expect_err("the commit is refused");
    assert_eq!(error.code(), Some(COLUMN_MISSING), "{error}");
    assert_eq!(published(&path, "events"), 2);
}

#[tokio::test]
async fn a_column_dropped_after_a_commit_is_answered_as_landed_fails_the_next_commit() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("answered.db");
    let mut opened = open_session(&path).await;
    staged_names(&mut opened, &table("events", None), &[1, 2]).await;
    let first = commit_of(opened.epoch, 1, 1);
    let receipt = opened
        .session
        .commit(&first)
        .await
        .expect("the commit lands");
    // Sent again naming segment 2, it is answered as landed and publishes nothing.
    let again = CommitMeta {
        segments: [SegmentId(2)].into_iter().collect(),
        ..first
    };
    let answered = opened.session.commit(&again).await;
    assert_eq!(answered.expect("answered as landed"), receipt);
    let outside = rusqlite::Connection::open(&path).expect("the database opens");
    outside
        .execute("ALTER TABLE \"events\" DROP COLUMN \"name\"", [])
        .expect("the column is dropped");
    let second = commit_of(opened.epoch, 2, 2);
    let error = opened
        .session
        .commit(&second)
        .await
        .expect_err("the commit is refused");
    assert_eq!(error.code(), Some(COLUMN_MISSING), "{error}");
    assert_eq!(published(&path, "events"), 2);
}

#[tokio::test]
async fn a_commit_removes_what_its_abandoned_segments_staged_and_publishes_none_of_it() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("abandoning.db");
    let mut opened = open_session(&path).await;
    staged_names(&mut opened, &table("events", None), &[1, 2]).await;
    let meta = CommitMeta {
        abandoned: [SegmentId(2)].into_iter().collect(),
        ..commit_of(opened.epoch, 1, 1)
    };
    opened
        .session
        .commit(&meta)
        .await
        .expect("the commit lands");
    assert_eq!(published(&path, "events"), 2);
    let planner = SqlPlanner::try_new(Sqlite).expect("a planner");
    assert_eq!(published(&path, &planner.staging_table("events")), 0);
}
