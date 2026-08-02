//! Classification through the public surface, plus the two library
//! probes that keep the message-prefix rulebook honest.
//!
//! DuckDB's C API reports no structured error category, so this crate
//! classifies on stable message PREFIXES — which only works if a
//! message that merely MENTIONS violation-adjacent words is never
//! misread, and if the prefixes themselves stay put. The first two
//! cells pin the former end to end; the last two pin the latter
//! directly on the library so a wording change fails loudly here
//! instead of silently reclassifying production errors.

use duckdb::Connection;
use rdlt_connector_duckdb_v2::destination::{Config, ConfigError, MergeStrategy, Shell};
use rdlt_connector_sdk::spi::core::{LoadId, PipelineId, WriteMode};
use rdlt_connector_sdk::spi::{Destination, DestinationError, OpenContext};
use rdlt_testkit::schema_for;

async fn ensure_under_upsert(
    path: &std::path::Path,
    table: &str,
    key: &str,
) -> Result<(), DestinationError> {
    let mut config = Config::new(path);
    config.merge_strategy = Some(MergeStrategy::Upsert);
    let shell = Shell::new(config).expect("valid document");
    let mut session = shell
        .open(OpenContext::new(
            PipelineId::new("classify"),
            LoadId::new("load-1"),
        ))
        .await
        .expect("open");
    let keyed = WriteMode::Merge {
        key: vec![key.to_owned()],
    };
    session.ensure_table(&schema_for(table), &keyed).await
}

/// A GENUINE duplicate-key failure — the upsert arbiter index refused
/// over pre-existing duplicate rows — renders sqlcore's shared
/// diagnosis, naming the strategy, the table, and the colliding key.
#[tokio::test]
async fn preexisting_duplicate_keys_render_the_upsert_diagnosis() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dupes.duckdb");
    // Seed duplicates the way a pre-rdlt database would carry them.
    let seed = Connection::open(&path).expect("seed connection");
    seed.execute_batch(
        "CREATE TABLE \"events\" (\"id\" BIGINT); INSERT INTO \"events\" VALUES (7), (7);",
    )
    .expect("seed");
    drop(seed);

    let err = ensure_under_upsert(&path, "events", "id")
        .await
        .expect_err("the arbiter index cannot build over duplicates");
    let text = err.to_string();
    assert!(
        text.contains("cannot create the unique index the upsert strategy requires"),
        "the shared diagnosis wording: {text}"
    );
    assert!(text.contains("`events`"), "names the table: {text}");
    assert!(
        text.contains("(id)"),
        "names the colliding merge key: {text}"
    );
}

/// A fatal error whose MESSAGE merely mentions violation-adjacent
/// wording must surface as ITSELF — never dressed in the duplicate-key
/// diagnosis, which keys on DuckDB's `Constraint Error` prefix alone.
/// The hard case: the failure happens ON the unique-index statement
/// (a merge key naming a column called `violates_nothing` that does
/// not exist draws a Binder error naming it), which is exactly the
/// branch where a broad-needle classifier would misfire.
#[tokio::test]
async fn violation_wording_on_the_index_statement_is_not_the_diagnosis() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("binder.duckdb");
    let err = ensure_under_upsert(&path, "events", "violates_nothing")
        .await
        .expect_err("the arbiter index cannot bind a missing column");
    assert!(
        matches!(err, DestinationError::Fatal(_)),
        "a binder failure is fatal, not retryable: {err}"
    );
    let text = err.to_string();
    assert!(
        text.contains("violate"),
        "precondition — the broad needle is present: {text}"
    );
    assert!(
        !text.contains("cannot create the unique index"),
        "misdiagnosed as a duplicate-key violation: {text}"
    );
}

/// An unopenable database path is ENVIRONMENTAL: the classifier calls
/// it transient (rides the retry budget), and because assembly happens
/// inside `Shell::new`, that transience arrives wrapped in the config
/// error's Invalid arm with the SPI framing intact.
#[test]
fn an_unopenable_path_classifies_transient_through_shell_new() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("never-created").join("db.duckdb");
    let err = Shell::new(Config::new(missing)).expect_err("no parent directory");
    assert!(
        matches!(err, ConfigError::Invalid(_)),
        "assemble failures land in the Invalid arm: {err}"
    );
    let text = err.to_string();
    assert!(
        text.contains("transient destination error"),
        "the SPI transient framing survives the wrap: {text}"
    );
    assert!(text.contains("IO Error"), "the classifier's key: {text}");
}

/// One genuine constraint violation straight from the library, for the
/// two probes below.
fn library_constraint_violation() -> duckdb::Error {
    let c = Connection::open_in_memory().expect("in-memory duckdb");
    c.execute_batch("CREATE TABLE probe (k BIGINT); INSERT INTO probe VALUES (3), (3);")
        .expect("seed");
    c.execute_batch("CREATE UNIQUE INDEX probe_k ON probe (k)")
        .expect_err("duplicates must refuse the unique index")
}

/// Library probe 1 — the structured channel stays DEGENERATE: the ffi
/// error carries `ErrorCode::Unknown` because the C API reports no
/// category. If this fails, duckdb-rs began populating real codes —
/// move classification onto them and retire the prefix matching.
#[test]
fn the_structured_error_channel_is_still_degenerate() {
    match library_constraint_violation() {
        duckdb::Error::DuckDBFailure(ffi, _) => assert_eq!(
            ffi.code,
            duckdb::ffi::ErrorCode::Unknown,
            "duckdb-rs now reports a structured category — reclassify on it"
        ),
        other => panic!("unexpected error variant for a constraint violation: {other:?}"),
    }
}

/// Library probe 2 — the `Constraint Error` message prefix is stable.
/// If this fails, DuckDB reworded the message — update the prefix in
/// destination/client.rs in the same change.
#[test]
fn the_constraint_message_prefix_is_still_stable() {
    match library_constraint_violation() {
        duckdb::Error::DuckDBFailure(_, Some(message)) => assert!(
            message.starts_with("Constraint Error"),
            "DuckDB reworded its constraint message — update destination/client.rs: {message}"
        ),
        other => panic!("constraint violation carried no message: {other:?}"),
    }
}
