//! THE duckdb-rs boundary: the shared database handle, per-session
//! setup replay, and the error-classification rulebook. Library types
//! stop at this module's edge.
//!
//! One `Connection` is opened per destination and every session CLONES
//! from it — two independent `Connection::open`s on the same file are
//! two database instances, and the second cannot see the first's
//! un-checkpointed catalog. A cloned connection inherits neither
//! session-scoped `SET`s nor `LOAD`s, so the recorded setup statements
//! replay on every clone; skipping the replay would leave the session
//! that actually writes silently unconfigured.

use std::sync::{Arc, Mutex};

use duckdb::Connection;
use rdlt_connector_sdk::spi::DestinationError;

/// One setup statement, recorded for replay-per-clone.
#[derive(Debug, Clone)]
enum Setup {
    /// `SET {key}='{value}'` — value escaped by `'` doubling; the key
    /// passed the bare-identifier gate at validation.
    Setting { key: String, value: String },
    /// `LOAD {name}` — name passed the bare-identifier gate.
    Extension { name: String },
}

impl Setup {
    fn render(&self) -> String {
        match self {
            Setup::Setting { key, value } => {
                format!("SET {key}='{}'", value.replace('\'', "''"))
            }
            Setup::Extension { name } => format!("LOAD {name}"),
        }
    }

    fn describe(&self) -> String {
        match self {
            Setup::Setting { key, .. } => format!("duckdb setting `{key}`"),
            Setup::Extension { name } => format!("duckdb extension `{name}`"),
        }
    }
}

/// The shared database instance.
#[derive(Clone)]
pub(crate) struct Db {
    conn: Arc<Mutex<Connection>>,
    setup: Arc<Vec<Setup>>,
}

impl std::fmt::Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db").finish_non_exhaustive()
    }
}

impl Db {
    /// Open (or create) the database file and apply every setup
    /// statement EAGERLY — a bad key or value errors here, at
    /// connect, not later inside a session.
    pub(crate) fn connect(
        path: &std::path::Path,
        settings: impl IntoIterator<Item = (String, String)>,
        extensions: impl IntoIterator<Item = String>,
    ) -> Result<Self, DestinationError> {
        // A locked or I/O-pressured file is the environment's problem,
        // not the pipeline's: transient, so the engine retries.
        let conn = Connection::open(path).map_err(classify)?;
        let mut setup = Vec::new();
        for (key, value) in settings {
            setup.push(Setup::Setting { key, value });
        }
        for name in extensions {
            setup.push(Setup::Extension { name });
        }
        for statement in &setup {
            conn.execute_batch(&statement.render())
                .map_err(|e| DestinationError::fatal(format!("{}: {e}", statement.describe())))?;
        }
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            setup: Arc::new(setup),
        })
    }

    /// A NEW session connection: cloned from the shared instance with
    /// the recorded setup replayed.
    pub(crate) fn session(&self) -> Result<Connection, DestinationError> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| DestinationError::fatal("connection poisoned"))?
            .try_clone()
            .map_err(classify)?;
        for statement in self.setup.iter() {
            conn.execute_batch(&statement.render())
                .map_err(|e| DestinationError::fatal(format!("{}: {e}", statement.describe())))?;
        }
        Ok(conn)
    }
}

/// The classification rulebook. DuckDB's C API reports NO structured
/// error category (the crate's probe pins `ErrorCode::Unknown`), so
/// the ONE transient key is a stable message prefix: `"IO Error"`
/// covers file locks (another process holding the database) and disk
/// pressure — both heal on retry. Everything else is fatal.
pub(crate) fn classify(e: duckdb::Error) -> DestinationError {
    if let duckdb::Error::DuckDBFailure(_, Some(message)) = &e
        && message.starts_with("IO Error")
    {
        return DestinationError::transient(e.to_string());
    }
    DestinationError::fatal(e.to_string())
}

/// The duplicate-merge-key diagnosis key, checked on the LIBRARY
/// error BEFORE wrapping — a message that merely mentions violations
/// (a table name, a quoted value) can never be misdiagnosed.
pub(crate) fn is_constraint_violation(e: &duckdb::Error) -> bool {
    matches!(e, duckdb::Error::DuckDBFailure(_, Some(message))
        if message.starts_with("Constraint Error"))
}

/// The default wrap for statements with no classification story.
pub(crate) fn fatal(e: duckdb::Error) -> DestinationError {
    DestinationError::fatal(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The replay-per-clone invariant, pinned at its seam: a cloned
    /// session connection inherits neither SETs nor LOADs from the
    /// builder connection, so `session()` must replay the recorded
    /// setup — this is the connection that actually writes.
    #[test]
    fn a_session_connection_carries_the_recorded_settings() {
        let dir = tempfile::tempdir().expect("dir");
        let db = Db::connect(
            &dir.path().join("x.duckdb"),
            [("threads".to_owned(), "1".to_owned())],
            [],
        )
        .expect("connect");
        let session = db.session().expect("clone + replay");
        let live: String = session
            .query_row("SELECT current_setting('threads')::VARCHAR", [], |row| {
                row.get(0)
            })
            .expect("query");
        assert_eq!(live, "1", "the setting reached the CLONED connection");
    }

    /// A bad setting value errors at CONNECT (eager application), with
    /// the frozen frame naming the key.
    #[test]
    fn a_bad_setting_value_errors_at_connect() {
        let dir = tempfile::tempdir().expect("dir");
        let err = Db::connect(
            &dir.path().join("x.duckdb"),
            [("threads".to_owned(), "zero".to_owned())],
            [],
        )
        .expect_err("refused")
        .to_string();
        assert!(err.contains("duckdb setting `threads`"), "{err}");
    }
}
