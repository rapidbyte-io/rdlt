//! Probe pinning how duckdb-rs surfaces constraint violations.
//!
//! The destination diagnoses upsert-precondition failures (duplicate merge
//! keys at unique-index creation) by looking at the error duckdb returns.
//! Two facts make that diagnosis message-based rather than code-based, and
//! this probe exists to fail loudly if either fact changes:
//!
//! 1. The structured channel is degenerate: `ffi::Error` carries
//!    `code: ErrorCode::Unknown` and a generic result code, because the
//!    DuckDB C API reports no error category.
//! 2. DuckDB renders constraint violations with a stable `Constraint Error`
//!    message prefix, which is therefore the classification key.
//!
//! If (1) fails, duckdb-rs started exposing structured categories — move
//! classification onto them. If (2) fails, DuckDB reworded its message —
//! update the prefix in `dest/commit.rs` in the same change.

use duckdb::{Connection, Error as DuckError};

fn constraint_violation() -> DuckError {
    let conn = Connection::open_in_memory().expect("in-memory duckdb");
    conn.execute_batch("CREATE TABLE t (k INTEGER); INSERT INTO t VALUES (1), (1);")
        .expect("setup");
    conn.execute_batch("CREATE UNIQUE INDEX t_k ON t (k)")
        .expect_err("duplicate keys must fail unique-index creation")
}

#[test]
fn structured_channel_carries_no_category() {
    match constraint_violation() {
        DuckError::DuckDBFailure(ffi, _) => {
            assert_eq!(
                ffi.code,
                duckdb::ffi::ErrorCode::Unknown,
                "duckdb-rs now populates ErrorCode — move classification onto it"
            );
        }
        other => panic!("constraint violation surfaced as unexpected variant: {other:?}"),
    }
}

#[test]
fn constraint_violation_message_prefix_is_stable() {
    match constraint_violation() {
        DuckError::DuckDBFailure(_, Some(msg)) => {
            assert!(
                msg.starts_with("Constraint Error"),
                "DuckDB reworded its constraint message — update the \
                 classifier prefix in dest/commit.rs: {msg}"
            );
        }
        other => panic!("constraint violation carried no message: {other:?}"),
    }
}
