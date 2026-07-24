//! Error-classification regressions: the destination must diagnose
//! duplicate merge keys only on genuine constraint violations, and must
//! leave a recoverable channel open for environmental failures (locked
//! database file, disk pressure) so the engine can retry instead of
//! aborting the run.

use rdlt_connector::DestinationError;
use rdlt_connector_duckdb::DuckDb;

/// A message that merely CONTAINS violation-adjacent words (here: a table
/// name) is not a constraint violation. The duplicate-merge-key diagnosis
/// keys on DuckDB's stable `Constraint Error` prefix, so a catalog error
/// about a table whose name contains "violate" surfaces as itself.
#[test]
fn violation_wording_in_other_errors_is_not_misdiagnosed() {
    let conn = duckdb::Connection::open_in_memory().expect("in-memory duckdb");
    let err = conn
        .execute_batch("CREATE UNIQUE INDEX x ON violates_log (k)")
        .expect_err("missing table");
    let msg = err.to_string();
    assert!(
        msg.contains("violate"),
        "precondition: the message must contain the broad needle: {msg}"
    );
    // The classifier the commit path uses must say NO for this error.
    assert!(
        !rdlt_connector_duckdb::dest::is_constraint_violation(&err),
        "catalog error misdiagnosed as a constraint violation: {msg}"
    );
    // And YES for a genuine one.
    conn.execute_batch("CREATE TABLE t (k INTEGER); INSERT INTO t VALUES (1), (1);")
        .expect("setup");
    let genuine = conn
        .execute_batch("CREATE UNIQUE INDEX t_k ON t (k)")
        .expect_err("duplicate keys");
    assert!(rdlt_connector_duckdb::dest::is_constraint_violation(
        &genuine
    ));
}

/// An unopenable database file is an environmental failure: it must ride
/// the engine's retry budget (transient), not abort the run (fatal).
#[test]
fn unopenable_file_is_transient_not_fatal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("no-such-dir").join("db.duckdb");
    match DuckDb::open(path) {
        Err(DestinationError::Transient { .. }) => {}
        Err(other) => panic!("environmental open failure classified unrecoverable: {other}"),
        Ok(_) => panic!("open succeeded against a missing directory"),
    }
}
