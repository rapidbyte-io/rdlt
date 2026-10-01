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
    run(&connection, &statement("CREATE TABLE t (a INTEGER)")).expect("a table is created");
    // A double-quoted name that is no column is an error, never a text, in rows and in schemas.
    let refused = [
        "SELECT \"nope\" FROM t",
        "INSERT INTO t VALUES (\"nope\")",
        "CREATE INDEX i ON t (\"nope\")",
        "ATTACH DATABASE ':memory:' AS other",
        "SELECT load_extension('nowhere')",
        "UPDATE sqlite_schema SET sql = 'x'",
        "DELETE FROM sqlite_schema",
    ];
    for sql in refused {
        let outcome = connection.execute_batch(sql);
        assert!(outcome.is_err(), "{sql}");
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
    let exposed = (
        ConnectorErrorKind::Config,
        Some("database_exposed".to_owned()),
    );
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
fn a_path_is_a_file_name_never_a_uri() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    // As a URI this would open a database in memory and create no file.
    let uri = format!("file:{}/uri.db?mode=memory", directory.path().display());
    let (kind, _) = refusal(connect(Path::new(&uri)));
    assert_eq!(kind, ConnectorErrorKind::Config);
    let named = directory.path().join("named.db?mode=memory");
    drop(connect(&named).expect("the database opens"));
    assert!(named.is_file());
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
