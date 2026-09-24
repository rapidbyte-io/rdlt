//! A SQLite connection used off the async runtime, and running `sqlgen`'s statements on it.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rdlt_connector::sqlgen::{Column, SqlDialect, SqlValue, Statement};
use rdlt_connector::{ConnectorError, ConnectorErrorKind, Result};
use rusqlite::types::Value;
use rusqlite::{Connection, ErrorCode, TransactionBehavior};

use crate::blocking::blocking;

/// How long a statement waits for another connection's write to finish.
const BUSY_TIMEOUT: Duration = Duration::from_secs(30);

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

/// A connection to the database at `path`, created when missing, in write-ahead-log mode so
/// readers never wait for a writer.
pub(super) fn connect(path: &Path) -> Result<Connection> {
    let connection = Connection::open(path).map_err(failed("opening the database"))?;
    connection
        .busy_timeout(BUSY_TIMEOUT)
        .map_err(failed("configuring the database"))?;
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .map_err(failed("configuring the database"))?;
    Ok(connection)
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

/// Classifies a SQLite error from `what`: contention is transient, an unusable file is a
/// configuration error, a refused value is a data error, and anything else a bug.
pub(super) fn failed(what: &'static str) -> impl Fn(rusqlite::Error) -> ConnectorError {
    move |error| {
        let kind = match error.sqlite_error_code() {
            Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked) => {
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
        ConnectorError::new(kind, format!("{what}: {error}")).with_source(error)
    }
}
