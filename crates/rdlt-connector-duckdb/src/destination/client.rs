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
        // The WHOLE check-open-claim sequence holds the registry lock:
        // the second read-write `Connection::open` is itself the
        // WAL-truncating act, so the refusal must come before it —
        // and without the lock held ACROSS the open, a second connect
        // could pass the check, sleep, and open long after the first
        // claimed and committed (the TOCTOU the 031 /code-review round
        // traced). Opens are rare and take milliseconds; serializing
        // them is free. `Claim::drop` takes this same lock, but no
        // claim is ever dropped while `connect` holds it.
        let claim = {
            let mut open = open_paths()
                .lock()
                .map_err(|_| DestinationError::fatal("open-path registry poisoned"))?;
            // The file may not exist yet (first ever open) — then
            // nobody can hold it and the pre-check is moot; the
            // post-open canonicalize below is the definitive spelling.
            if let Ok(canonical) = path.canonicalize()
                && open.contains(&canonical)
            {
                return Err(double_open_refusal(path));
            }
            // Transient here reaches the engine's retry budget only
            // through the SESSION path; through `assemble` it survives
            // as rendered text in the ConfigError (pinned) — either
            // way a locked or I/O-pressured file is the environment's
            // problem, not a config defect to rewrite.
            let conn = Connection::open(path).map_err(classify)?;
            // Canonicalize AFTER the open — the open creates the file,
            // and only the canonical spelling makes two documents
            // naming one file through different paths collide.
            let canonical = path.canonicalize().map_err(|e| {
                DestinationError::fatal(format!("cannot canonicalize `{}`: {e}", path.display()))
            })?;
            if !open.insert(canonical.clone()) {
                return Err(double_open_refusal(path));
            }
            (conn, Arc::new(Claim(canonical)))
        };
        let (conn, claim) = claim;
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
/// the transient key is a stable message prefix: `"IO Error"` covers
/// file locks (another process holding the database) and disk
/// pressure — both heal on retry, because the message text names the
/// operating condition, not the operation. Everything else is fatal.
///
/// A CARVE-OUT under that prefix (037 US6 / Task 19, S5) catches the
/// sub-cases that are deterministic instead: a missing parent
/// directory or a permission-denied path never heals on retry either —
/// no amount of retry budget opens a path that structurally cannot
/// open — so a run that treated them as transient would retry forever
/// and report a false "still trying" instead of failing loud. The two
/// fragments are PROBE-PINNED (`test_probes.rs`,
/// `probe_deterministic_io_message_spellings` +
/// `..._permission_denied`) against duckdb 1.5.x's own C++ source
/// (`local_file_system.cpp`), not guessed: both open failures render
/// through the SAME `"Cannot open file \"%s\": %s"` template, with
/// `strerror(errno)` supplying the deterministic suffix (`No such file
/// or directory` / `Permission denied`) — so the shared `"Cannot open
/// file"` fragment alone is the carve-out key, never mistaken for the
/// lock message (`"Could not set lock on file \"%s\": %s"`, a
/// DIFFERENT template with neither substring) or disk-pressure
/// messages (a `write()` failure, also a different template).
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
        // Deterministic sub-cases never heal on retry; the fragment is
        // probe-pinned (test_probes), not guessed. Locks and disk
        // pressure stay transient — the rulebook's whole point.
        const DETERMINISTIC: &[&str] = &["Cannot open file"];
        if DETERMINISTIC.iter().any(|f| message.contains(f)) {
            return DestinationError::fatal(e.to_string());
        }
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

/// The index-dependency refusal key (Task 15 probe, pinned live):
/// `ALTER … SET DATA TYPE` refuses outright — a CONTAINS check, not a
/// prefix, because the library's own wording leads with "Catalog
/// Error" — whenever ANY index, unique or plain, still depends on the
/// column being widened. The pre-ALTER drop (`load.rs`) clears the
/// UNIQUE arbiter out of the way before this can fire; a PLAIN index
/// (identity, `delete_insert`/`scd2` key columns, `merge_scope`) is
/// NOT pre-dropped (037 US5 fix round 2, F4 — a narrower, deliberate
/// scope), so this classifier is what turns that raw catalog error
/// into named advice instead.
pub(crate) fn is_index_dependency_error(e: &duckdb::Error) -> bool {
    matches!(e, duckdb::Error::DuckDBFailure(_, Some(message))
        if message.contains("an index depends on it!"))
}

/// The diagnosis for [`is_index_dependency_error`]: names the failing
/// statement (which already carries the table and column — no separate
/// parse needed) and the remedy, with the service's own wording kept
/// inside rather than discarded.
pub(crate) fn index_dependency_diagnosis(statement: &str, cause: &str) -> String {
    format!(
        "a cross-run widen is blocked by an index: `{statement}` — an index still \
         depends on this column (a plain identity/merge-key/scope index, not the \
         upsert arbiter, which this destination already clears itself); drop the \
         index manually and re-run so ensure recreates it after the widen, or leave \
         the column's type as it was if the index is load-bearing: {cause}"
    )
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
    /// else is FATAL — EXCEPT the deterministic carve-out (037 US6 /
    /// Task 19, S5), which routes fatal even under the `IO Error`
    /// prefix. The commit path (receipt probe, load-committed probe,
    /// `read_state`'s read arm) relies on exactly this split.
    #[test]
    fn classify_splits_io_errors_transient_everything_else_fatal() {
        let failure = |message: &str| {
            duckdb::Error::DuckDBFailure(duckdb::ffi::Error::new(1), Some(message.to_owned()))
        };
        assert!(
            matches!(
                classify(failure(
                    "IO Error: Could not set lock on file \"x.duckdb\": \
                                   Resource temporarily unavailable"
                )),
                DestinationError::Transient(_)
            ),
            "a lock conflict rides the retry budget — the rulebook's whole point"
        );
        assert!(
            matches!(
                classify(failure("Binder Error: no such column")),
                DestinationError::Fatal(_)
            ),
            "everything else stays fatal"
        );
        // The deterministic carve-out — one case per fragment
        // probe-pinned in test_probes.rs, plus the lock case above
        // proving the carve-out does NOT swallow the rulebook's
        // reason for the "IO Error" prefix existing at all.
        assert!(
            matches!(
                classify(failure(
                    "IO Error: Cannot open file \"/no/such/dir/x.duckdb\": \
                     No such file or directory"
                )),
                DestinationError::Fatal(_)
            ),
            "a missing parent directory never heals on retry"
        );
        assert!(
            matches!(
                classify(failure(
                    "IO Error: Cannot open file \"/no/perm/x.duckdb\": Permission denied"
                )),
                DestinationError::Fatal(_)
            ),
            "a permission-denied path never heals on retry"
        );
    }

    /// The index-dependency classifier, pinned against the EXACT
    /// library error shape probed live (Task 15): a `Catalog Error`
    /// wording, not a prefix match, so `is_index_dependency_error`
    /// must check CONTAINS. The diagnosis names the statement (which
    /// already carries table + column) and keeps the service's cause.
    #[test]
    fn index_dependency_errors_are_classified_and_diagnosed() {
        let e = duckdb::Error::DuckDBFailure(
            duckdb::ffi::Error::new(1),
            Some(
                "Catalog Error: Cannot change the type of this column: an index \
                 depends on it!"
                    .to_owned(),
            ),
        );
        assert!(is_index_dependency_error(&e));
        assert!(!is_index_dependency_error(&duckdb::Error::DuckDBFailure(
            duckdb::ffi::Error::new(1),
            Some("Binder Error: no such column".to_owned())
        )));

        let diagnosis = index_dependency_diagnosis(
            "ALTER TABLE \"t\" ALTER COLUMN \"id\" SET DATA TYPE VARCHAR",
            &e.to_string(),
        );
        assert!(
            diagnosis.contains("ALTER TABLE \"t\" ALTER COLUMN \"id\""),
            "{diagnosis}"
        );
        assert!(diagnosis.contains("drop the index manually"), "{diagnosis}");
        assert!(diagnosis.contains("an index depends on it!"), "{diagnosis}");
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
