use rdlt_connector::ConnectorErrorKind;
use rusqlite::ffi;

use super::failed;

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
}
