use std::path::Path;
use std::time::{Duration, Instant};

use rdlt_connector::sqlgen::Statement;
use rdlt_connector::{ConnectorError, ConnectorErrorKind};
use rusqlite::config::DbConfig;
use rusqlite::limits::Limit;
use rusqlite::{Connection, TransactionBehavior, ffi};

use super::{connect, connect_waiting, failed, run};
use crate::limits::JOURNAL_BYTES;

fn refusal<T: std::fmt::Debug>(
    outcome: Result<T, ConnectorError>,
) -> (ConnectorErrorKind, Option<String>) {
    let error = outcome.expect_err("the call is refused");
    (error.kind(), error.code().map(str::to_owned))
}

fn statement(sql: &str) -> Statement {
    Statement {
        sql: sql.to_owned(),
        params: Vec::new(),
    }
}

/// What the pragma `name` answers on `connection`.
fn pragma(connection: &Connection, name: &str) -> rusqlite::types::Value {
    connection
        .pragma_query_value(None, name, |row| row.get(0))
        .expect("the pragma answers")
}

#[test]
fn a_connection_is_opened_hardened() {
    use rusqlite::types::Value;
    let directory = tempfile::tempdir().expect("a temporary directory");
    let connection = connect(&directory.path().join("hard.db")).expect("the database opens");
    let set = |config| connection.db_config(config).expect("the setting reads");
    assert!(set(DbConfig::SQLITE_DBCONFIG_DEFENSIVE));
    for off in [
        DbConfig::SQLITE_DBCONFIG_TRUSTED_SCHEMA,
        DbConfig::SQLITE_DBCONFIG_DQS_DML,
        DbConfig::SQLITE_DBCONFIG_DQS_DDL,
        DbConfig::SQLITE_DBCONFIG_ENABLE_ATTACH_CREATE,
        DbConfig::SQLITE_DBCONFIG_ENABLE_ATTACH_WRITE,
    ] {
        assert!(!set(off), "{off:?}");
    }
    let attached = connection.limit(Limit::SQLITE_LIMIT_ATTACHED);
    assert_eq!(attached.expect("the limit reads"), 0);
    assert_eq!(
        pragma(&connection, "journal_mode"),
        Value::Text("wal".into())
    );
    assert_eq!(pragma(&connection, "cell_size_check"), Value::Integer(1));
    assert_eq!(pragma(&connection, "mmap_size"), Value::Integer(0));
    assert_eq!(pragma(&connection, "trusted_schema"), Value::Integer(0));
    let journal = i64::try_from(JOURNAL_BYTES).expect("the limit fits");
    assert_eq!(
        pragma(&connection, "journal_size_limit"),
        Value::Integer(journal)
    );
}

#[test]
fn each_protection_refuses_what_a_plain_connection_does() {
    // A double-quoted name that is no column is an error, never a text, in rows and in schemas;
    // no database attaches; and the schema is not written as rows, asked to be or not.
    let refused = [
        "SELECT \"nope\" FROM t",
        "INSERT INTO t VALUES (\"nope\")",
        "CREATE INDEX i ON t (\"nope\")",
        "ATTACH DATABASE ':memory:' AS other",
        "PRAGMA writable_schema = ON; UPDATE sqlite_schema SET sql = sql WHERE name = 't'",
        "PRAGMA writable_schema = ON; DELETE FROM sqlite_schema WHERE name = 't'",
    ];
    for sql in refused {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let path = directory.path().join("hard.db");
        let connection = connect(&path).expect("the database opens");
        run(&connection, &statement("CREATE TABLE t (a INTEGER)")).expect("a table is created");
        assert!(connection.execute_batch(sql).is_err(), "{sql}");
        drop(connection);
        // The same statement on a connection opened as SQLite's defaults leave it runs: what
        // refuses it above is what the connector sets.
        let plain = Connection::open(&path).expect("the database opens");
        plain
            .execute_batch(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
}

#[cfg(unix)]
#[test]
fn a_new_database_and_its_log_are_private() {
    use std::os::unix::fs::PermissionsExt as _;
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("private.db");
    let connection = connect(&path).expect("the database opens");
    run(&connection, &statement("CREATE TABLE t (a INTEGER)")).expect("a table is created");
    for suffix in ["", "-wal", "-shm"] {
        let mut name = path.clone().into_os_string();
        name.push(suffix);
        let mode = std::fs::metadata(&name)
            .unwrap_or_else(|error| panic!("{suffix}: {error}"))
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "{suffix}");
    }
}

#[cfg(unix)]
#[test]
fn a_database_others_can_reach_is_refused() {
    use std::os::unix::fs::PermissionsExt as _;
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("shared.db");
    drop(connect(&path).expect("the database is created"));
    let exposed = (ConnectorErrorKind::Config, Some("not_private".to_owned()));
    for mode in [0o644, 0o640, 0o604, 0o660, 0o606, 0o610, 0o601] {
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).expect("a mode");
        assert_eq!(refusal(connect(&path)), exposed, "{mode:o}");
    }
    for mode in [0o600, 0o700] {
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).expect("a mode");
        connect(&path).unwrap_or_else(|error| panic!("{mode:o}: {error}"));
    }
    // What is no file is no database, whatever its mode.
    let (kind, _) = refusal(connect(directory.path()));
    assert_eq!(kind, ConnectorErrorKind::Config);
    let (kind, _) = refusal(connect(&directory.path().join("missing").join("x.db")));
    assert_eq!(kind, ConnectorErrorKind::Config);
}

#[test]
fn a_full_disk_is_transient() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let connection = connect(&directory.path().join("full.db")).expect("the database opens");
    run(&connection, &statement("CREATE TABLE t (a BLOB)")).expect("a table is created");
    connection
        .pragma_update(None, "max_page_count", 8)
        .expect("the database is bounded");
    let full = run(
        &connection,
        &statement("INSERT INTO t VALUES (zeroblob(1000000))"),
    );
    assert_eq!(
        refusal(full),
        (ConnectorErrorKind::Transient, Some("disk_full".to_owned()))
    );
}

#[test]
fn a_writer_another_holds_out_is_waited_for_a_bounded_time() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("busy.db");
    let mut waiting =
        connect_waiting(&path, Duration::from_millis(50)).expect("the database opens");
    let mut holder = Connection::open(&path).expect("the database opens again");
    let held = holder
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .expect("the write lock is taken");
    let started = Instant::now();
    let refused = waiting
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map(drop)
        .map_err(failed("starting a transaction"));
    assert_eq!(refusal(refused).0, ConnectorErrorKind::Transient);
    assert!(started.elapsed() < Duration::from_secs(10));
    drop(held);
    waiting
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .expect("the lock is free");
}

fn code(code: i32) -> rusqlite::Error {
    rusqlite::Error::SqliteFailure(ffi::Error::new(code), None)
}

#[test]
fn sqlite_errors_are_classified_by_what_a_retry_would_change() {
    let kind = |error| failed("running")(error).kind();
    for busy in [ffi::SQLITE_BUSY, ffi::SQLITE_LOCKED] {
        assert_eq!(kind(code(busy)), ConnectorErrorKind::Transient, "{busy}");
    }
    for setup in [
        ffi::SQLITE_CANTOPEN,
        ffi::SQLITE_READONLY,
        ffi::SQLITE_PERM,
        ffi::SQLITE_NOTADB,
    ] {
        assert_eq!(kind(code(setup)), ConnectorErrorKind::Config, "{setup}");
    }
    for data in [
        ffi::SQLITE_CONSTRAINT,
        ffi::SQLITE_MISMATCH,
        ffi::SQLITE_TOOBIG,
    ] {
        assert_eq!(kind(code(data)), ConnectorErrorKind::Data, "{data}");
    }
    assert_eq!(
        kind(code(ffi::SQLITE_CORRUPT)),
        ConnectorErrorKind::Internal
    );
    // A full disk may have room again; no other failure takes its code.
    let full = failed("running")(code(ffi::SQLITE_FULL));
    assert_eq!(
        (full.kind(), full.code()),
        (ConnectorErrorKind::Transient, Some("disk_full"))
    );
    assert_eq!(failed("running")(code(ffi::SQLITE_BUSY)).code(), None);
}

/// A database at `path` in which another program planted a trigger that empties `kept` when
/// `written` gains a row, a view of `kept`, and a row of `child` that goes with its parent.
fn planted(path: &Path) {
    let raw = Connection::open(path).expect("the database opens");
    raw.execute_batch(
        "CREATE TABLE kept (a INTEGER); INSERT INTO kept VALUES (1); \
         CREATE TABLE written (a INTEGER); \
         CREATE TRIGGER emptying AFTER INSERT ON written BEGIN DELETE FROM kept; END; \
         CREATE VIEW seen AS SELECT a FROM kept; \
         CREATE TABLE parent (id INTEGER PRIMARY KEY); INSERT INTO parent VALUES (1); \
         CREATE TABLE child (id INTEGER REFERENCES parent (id) ON DELETE CASCADE); \
         INSERT INTO child VALUES (1)",
    )
    .expect("the schema is planted");
}

/// What a connection finds after writing a row and deleting a parent in a planted database:
/// the rows left in `kept` and in `child`, and whether the view reads.
fn after_writing(connection: &Connection) -> (i64, i64, bool) {
    connection
        .execute_batch("INSERT INTO written VALUES (1); DELETE FROM parent")
        .expect("the rows are written");
    let count = |table: &str| -> i64 {
        connection
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .expect("the table counts")
    };
    let view = connection.execute_batch("SELECT a FROM seen").is_ok();
    (count("kept"), count("child"), view)
}

#[test]
fn a_trigger_a_view_or_a_foreign_key_planted_in_the_file_does_nothing() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    // A connection that is not hardened runs all three.
    let open = directory.path().join("open.db");
    drop(connect(&open).expect("the database is created"));
    planted(&open);
    let raw = Connection::open(&open).expect("the database opens");
    raw.pragma_update(None, "foreign_keys", true)
        .expect("the keys are on");
    assert_eq!(after_writing(&raw), (0, 0, true));
    // The connector's connection runs none.
    let hard = directory.path().join("hard.db");
    drop(connect(&hard).expect("the database is created"));
    planted(&hard);
    let connection = connect(&hard).expect("the database opens");
    assert_eq!(after_writing(&connection), (1, 1, false));
    let set = |config| connection.db_config(config).expect("the setting reads");
    for off in [
        DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER,
        DbConfig::SQLITE_DBCONFIG_ENABLE_VIEW,
        DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY,
    ] {
        assert!(!set(off), "{off:?}");
    }
}

#[test]
fn a_statement_sqlite_cannot_read_fails_without_its_text() {
    let connection = rusqlite::Connection::open_in_memory().expect("a database");
    let statement = r#"CREATE INDEX "_rdlt_key__secret_table" ON "secret_table" ()"#;
    let refused = connection.execute(statement, []).expect_err("no statement");
    assert!(refused.to_string().contains("secret_table"));
    let error = failed("running a statement")(refused);
    assert_eq!(error.kind(), ConnectorErrorKind::Internal);
    let said = error.to_string();
    assert!(
        said.starts_with("running a statement: ") && !said.contains("secret_table"),
        "{said}"
    );
}
