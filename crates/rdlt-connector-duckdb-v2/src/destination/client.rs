//! THE duckdb-rs boundary: the shared database handle, per-session
//! setup replay, and the error-classification rulebook. Library types
//! stop at this module's edge.
//!
//! One `Connection` is opened per destination and every session CLONES
//! from it — two independent `Connection::open`s on the same file are
//! two database instances, and the second cannot see the first's
//! un-checkpointed catalog. A cloned connection inherits neither
//! session-scoped `SET`s nor `LOAD`s, so the recorded setup statements
//! replay on every clone; the replay is what configures the clone that
//! performs the load at all — every `SET` and `LOAD` would otherwise
//! bind only to the idle builder connection.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use duckdb::Connection;
use rdlt_connector_sdk::spi::DestinationError;

/// Canonical paths held open READ-WRITE by this process, one entry per
/// live [`Db`]. Read-only oracles (`testhook`) open the library
/// directly and are deliberately exempt — a read-only open replays the
/// WAL without touching it.
fn open_paths() -> &'static Mutex<HashSet<PathBuf>> {
    static OPEN: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    OPEN.get_or_init(|| Mutex::new(HashSet::new()))
}

/// One registry claim, released on drop. Every clone of a [`Db`]
/// shares the one claim through its `Arc`, so the path frees when the
/// LAST clone goes — a sequential re-open stays legal.
#[derive(Debug)]
struct Claim(PathBuf);

impl Drop for Claim {
    fn drop(&mut self) {
        if let Ok(mut open) = open_paths().lock() {
            open.remove(&self.0);
        }
    }
}

/// The double-open refusal (031 review N2, measured): a second
/// read-write instance in this process replays AND TRUNCATES the live
/// instance's WAL at open, silently swallowing every commit the first
/// makes afterwards.
fn double_open_refusal(path: &Path) -> DestinationError {
    DestinationError::fatal(format!(
        "duckdb database `{}` is already open in this process — a second \
         instance cannot see the first's writes and a read-write open \
         would truncate its WAL; share the one destination",
        path.display()
    ))
}

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
    /// Held for its Drop alone: the registry entry that refuses a
    /// second in-process open of the same file.
    _claim: Arc<Claim>,
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
        // Refuse BEFORE the library open where the hazard is already
        // knowable: the second read-write `Connection::open` is itself
        // the WAL-truncating act, so a refusal after it would arrive
        // with the damage done. The file may not exist yet (first ever
        // open), in which case nobody can hold it and the check waits
        // for the definitive claim below.
        if let Ok(canonical) = path.canonicalize()
            && open_paths()
                .lock()
                .is_ok_and(|open| open.contains(&canonical))
        {
            return Err(double_open_refusal(path));
        }
        // A locked or I/O-pressured file is the environment's problem,
        // not the pipeline's: transient, so the engine retries.
        let conn = Connection::open(path).map_err(classify)?;
        // Canonicalize AFTER the open — the open creates the file, and
        // only the canonical spelling makes two documents naming one
        // file through different paths collide in the registry.
        let canonical = path.canonicalize().map_err(|e| {
            DestinationError::fatal(format!("cannot canonicalize `{}`: {e}", path.display()))
        })?;
        let claim = {
            let mut open = open_paths()
                .lock()
                .map_err(|_| DestinationError::fatal("open-path registry poisoned"))?;
            if !open.insert(canonical.clone()) {
                return Err(double_open_refusal(path));
            }
            Arc::new(Claim(canonical))
        };
        // Settings are recorded (and applied) BEFORE extensions
        // deliberately: probed benign for bundled builds, and it keeps
        // limits like memory_limit in force before any LOAD runs.
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
            _claim: claim,
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
///
/// Classification is UNIFORM across the whole load path (031 review
/// S2/A3): the receipt probe, the load-committed probe, and
/// `read_state`'s read arm all route here too — those reads are
/// idempotent, so a locked-file IO error there deserves the retry
/// budget, not a run abort. Only `read_state`'s serde-parse arm stays
/// unconditionally fatal (a corrupt document never heals on retry).
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

    /// The classifier wiring, pinned directly on constructed library
    /// errors: an `IO Error`-prefixed failure is TRANSIENT, anything
    /// else is FATAL. The commit path (receipt probe, load-committed
    /// probe, `read_state`'s read arm) relies on exactly this split.
    #[test]
    fn classify_splits_io_errors_transient_everything_else_fatal() {
        let failure = |message: &str| {
            duckdb::Error::DuckDBFailure(duckdb::ffi::Error::new(1), Some(message.to_owned()))
        };
        assert!(
            matches!(
                classify(failure("IO Error: could not set lock on file")),
                DestinationError::Transient(_)
            ),
            "an IO Error rides the retry budget"
        );
        assert!(
            matches!(
                classify(failure("Binder Error: no such column")),
                DestinationError::Fatal(_)
            ),
            "everything else stays fatal"
        );
    }

    /// The in-process double-open guard at the `Db` seam: a second
    /// connect on the SAME canonical path is refused typed-fatal, and
    /// dropping the first instance makes a sequential re-open legal
    /// again.
    #[test]
    fn a_second_in_process_connect_is_refused_until_the_first_drops() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("one.duckdb");
        let first = Db::connect(&path, [], []).expect("first open");
        let err = Db::connect(&path, [], [])
            .expect_err("second live instance refused")
            .to_string();
        assert!(err.contains("already open in this process"), "{err}");
        drop(first);
        Db::connect(&path, [], []).expect("sequential re-open is legal");
    }
}
