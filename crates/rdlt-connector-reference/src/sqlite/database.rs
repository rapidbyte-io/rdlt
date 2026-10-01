//! A SQLite connection used off the async runtime, and running `sqlgen`'s statements on it.

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::limits::{BUSY_WAIT, JOURNAL_BYTES};

use parking_lot::Mutex;
use rdlt_connector::sqlgen::{Column, SqlDialect, SqlValue, Statement};
use rdlt_connector::{ConnectorError, ConnectorErrorKind, Result};
use rusqlite::config::DbConfig;
use rusqlite::limits::Limit;
use rusqlite::types::Value;
use rusqlite::{Connection, ErrorCode, OpenFlags, TransactionBehavior};

use super::location::located;
use crate::blocking::blocking;

/// One connection to the database, used only on the blocking thread pool.
#[derive(Clone, Debug)]
pub(super) struct Database(Arc<Mutex<Connection>>);

impl Database {
    /// Opens the database at `path`, creating the file when missing.
    pub(super) async fn open(path: PathBuf) -> Result<Self> {
        let connection = blocking(move || connect(&path)).await?;
        Ok(Self(Arc::new(Mutex::new(connection))))
    }

    /// Runs `work` in an immediate transaction, which holds the database's write lock from its
    /// start, and commits it when `work` succeeds.
    pub(super) async fn transaction<T: Send + 'static>(
        &self,
        work: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let connection = Arc::clone(&self.0);
        blocking(move || {
            let mut connection = connection.lock();
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(failed("starting a transaction"))?;
            let value = work(&transaction)?;
            transaction.commit().map_err(failed("committing"))?;
            Ok(value)
        })
        .await
    }
}

/// A connection to the database at `path`, created when missing and private to its user, in
/// write-ahead-log mode so readers never wait for a writer, and hardened against a database
/// file another program wrote.
///
/// SQLite is handed the path from the root, which it reads as a file's name and never as a URI.
pub(super) fn connect(path: &Path) -> Result<Connection> {
    connect_waiting(path, BUSY_WAIT)
}

/// As [`connect`], each statement waiting `wait` for another connection's write to finish.
fn connect_waiting(path: &Path, wait: Duration) -> Result<Connection> {
    let Some(path) = located(path, true)? else {
        return Err(ConnectorError::internal(
            "a database asked for was not created",
        ));
    };
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let connection =
        Connection::open_with_flags(&path, flags).map_err(failed("opening the database"))?;
    let configuring = failed("configuring the database");
    harden(&connection).map_err(&configuring)?;
    connection.busy_timeout(wait).map_err(&configuring)?;
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .map_err(&configuring)?;
    let journal = i64::try_from(JOURNAL_BYTES).unwrap_or(i64::MAX);
    connection
        .pragma_update_and_check(None, "journal_size_limit", journal, |_| Ok(()))
        .map_err(&configuring)?;
    Ok(connection)
}

/// A connection that only reads the database at `path`, hardened as [`connect`]'s; none where
/// there is no database: reading creates none.
pub(super) fn reading(path: &Path) -> Result<Option<Connection>> {
    let Some(path) = located(path, false)? else {
        return Ok(None);
    };
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let connection =
        Connection::open_with_flags(&path, flags).map_err(failed("opening the database"))?;
    let configuring = failed("configuring the database");
    harden(&connection).map_err(&configuring)?;
    connection.busy_timeout(BUSY_WAIT).map_err(&configuring)?;
    Ok(Some(connection))
}

/// Sets `connection` to trust nothing its database file holds beyond tables and their rows:
/// the schema cannot be written as rows, no trigger fires, no view resolves, no foreign key
/// acts, a double-quoted name is an identifier or an error, no other database attaches, and
/// every page read is checked.
fn harden(connection: &Connection) -> rusqlite::Result<()> {
    let settings = [
        (DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true),
        (DbConfig::SQLITE_DBCONFIG_TRUSTED_SCHEMA, false),
        (DbConfig::SQLITE_DBCONFIG_DQS_DML, false),
        (DbConfig::SQLITE_DBCONFIG_DQS_DDL, false),
        (DbConfig::SQLITE_DBCONFIG_ENABLE_ATTACH_CREATE, false),
        (DbConfig::SQLITE_DBCONFIG_ENABLE_ATTACH_WRITE, false),
        // The destination creates no trigger, view or foreign key: one in the file is another
        // program's, and would run or resolve with the connection's rights.
        (DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, false),
        (DbConfig::SQLITE_DBCONFIG_ENABLE_VIEW, false),
        (DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, false),
    ];
    for (setting, on) in settings {
        connection.set_db_config(setting, on)?;
    }
    connection.set_limit(Limit::SQLITE_LIMIT_ATTACHED, 0)?;
    connection.pragma_update(None, "cell_size_check", true)?;
    connection.pragma_update_and_check(None, "mmap_size", 0, |_| Ok(()))
}

/// Runs `statement`; returns the number of rows it changed.
pub(super) fn run(connection: &Connection, statement: &Statement) -> Result<usize> {
    let params = rusqlite::params_from_iter(statement.params.iter().map(value));
    connection
        .execute(&statement.sql, params)
        .map_err(failed("running a statement"))
}

/// Runs every statement of `statements`, in order.
pub(super) fn run_all(connection: &Connection, statements: &[Statement]) -> Result<()> {
    for statement in statements {
        run(connection, statement)?;
    }
    Ok(())
}

/// The rows `statement` returns.
pub(super) fn query(connection: &Connection, statement: &Statement) -> Result<Vec<Vec<Value>>> {
    let mut prepared = connection
        .prepare_cached(&statement.sql)
        .map_err(failed("preparing a query"))?;
    let width = prepared.column_count();
    let params = rusqlite::params_from_iter(statement.params.iter().map(value));
    let rows = prepared
        .query_map(params, |row| {
            (0..width).map(|index| row.get(index)).collect()
        })
        .map_err(failed("running a query"))?;
    rows.collect::<rusqlite::Result<_>>()
        .map_err(failed("reading a query's rows"))
}

/// The columns `table` has, none when it is missing.
pub(super) fn columns(
    connection: &Connection,
    dialect: &impl SqlDialect,
    table: &str,
) -> Result<Vec<Column>> {
    query(connection, &dialect.columns(table))?
        .into_iter()
        .map(|row| match &row[..] {
            [Value::Text(name), Value::Text(declared)] => Ok(Column {
                name: name.clone(),
                declared: declared.clone(),
            }),
            _ => Err(ConnectorError::internal(format!(
                "table {table} lists a column the catalog query cannot read"
            ))),
        })
        .collect()
}

/// The text of `value`, or an internal error naming what the catalog holds instead.
pub(super) fn text(value: &Value) -> Result<String, ConnectorError> {
    match value {
        Value::Text(text) => Ok(text.clone()),
        other => Err(ConnectorError::internal(format!(
            "the catalog holds {other:?} where it keeps text"
        ))),
    }
}

/// The integer of `value`, or an internal error naming what the catalog holds instead.
pub(super) fn integer(value: &Value) -> Result<i64> {
    match value {
        Value::Integer(integer) => Ok(*integer),
        other => Err(ConnectorError::internal(format!(
            "the catalog holds {other:?} where it keeps an integer"
        ))),
    }
}

fn value(value: &SqlValue) -> Value {
    match value {
        SqlValue::Null => Value::Null,
        SqlValue::Integer(integer) => Value::Integer(*integer),
        SqlValue::Text(text) => Value::Text(text.clone()),
        SqlValue::Blob(blob) => Value::Blob(blob.clone()),
    }
}

/// Classifies a SQLite error from `what`: contention is transient, and so is a full disk, coded
/// `disk_full`, which may have room again; an unusable file is a configuration error, a refused
/// value is a data error, and anything else a bug.
pub(super) fn failed(what: &'static str) -> impl Fn(rusqlite::Error) -> ConnectorError {
    move |error| {
        let code = error.sqlite_error_code();
        let kind = match code {
            Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked | ErrorCode::DiskFull) => {
                ConnectorErrorKind::Transient
            }
            Some(
                ErrorCode::CannotOpen
                | ErrorCode::ReadOnly
                | ErrorCode::PermissionDenied
                | ErrorCode::NotADatabase,
            ) => ConnectorErrorKind::Config,
            Some(ErrorCode::ConstraintViolation | ErrorCode::TypeMismatch | ErrorCode::TooBig) => {
                ConnectorErrorKind::Data
            }
            _ => ConnectorErrorKind::Internal,
        };
        let failed = ConnectorError::new(kind, format!("{what}: {error}")).with_source(error);
        match code {
            Some(ErrorCode::DiskFull) => failed.with_code("disk_full"),
            _ => failed,
        }
    }
}
