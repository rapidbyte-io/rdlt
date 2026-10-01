use std::io::{Error, ErrorKind};
use std::path::Path;

use rdlt_connector::{ConnectorErrorKind, LimitExceeded};

use super::{INVALID_NAME, NOT_A_REGULAR_FILE, failed, listed, retried};
use crate::limits::PUBLISH_ATTEMPTS;
use crate::rooted::Refusal;

#[test]
fn a_filesystem_error_is_classified_by_what_refused() {
    let path = Path::new("root/file");
    let classified = |error: Error| {
        let error = failed("opening", path)(error);
        (error.kind(), error.code().map(str::to_owned))
    };
    let (config, transient, data) = (
        ConnectorErrorKind::Config,
        ConnectorErrorKind::Transient,
        ConnectorErrorKind::Data,
    );
    for kind in [
        ErrorKind::PermissionDenied,
        ErrorKind::ReadOnlyFilesystem,
        ErrorKind::NotADirectory,
    ] {
        assert_eq!(classified(kind.into()), (config, None), "{kind:?}");
    }
    for kind in [
        ErrorKind::NotFound,
        ErrorKind::StorageFull,
        ErrorKind::AlreadyExists,
        ErrorKind::Other,
        ErrorKind::InvalidInput,
        ErrorKind::InvalidData,
    ] {
        assert_eq!(classified(kind.into()), (transient, None), "{kind:?}");
    }
    assert_eq!(
        classified(Refusal::Name.into()),
        (data, Some(INVALID_NAME.to_owned()))
    );
    assert_eq!(
        classified(Refusal::NotRegular.into()),
        (data, Some(NOT_A_REGULAR_FILE.to_owned()))
    );
    assert_eq!(classified(Refusal::Shared.into()), (config, None));
    let too_large = Refusal::TooLarge {
        name: "manifest bytes",
        limit: 8,
        actual: 9,
    };
    let error = failed("reading", path)(too_large.into());
    assert_eq!(error.kind(), data);
    assert_eq!(error.code(), Some("limit_exceeded"));
    let limit = LimitExceeded {
        name: "manifest bytes",
        limit: 8,
        actual: 9,
    };
    assert_eq!(error.limit(), Some(limit));
}

#[test]
fn a_listed_file_that_is_missing_is_lost_for_good() {
    let path = Path::new("root/gone.jsonl");
    let error = listed("opening", path)(ErrorKind::NotFound.into());
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
    assert_eq!(error.code(), Some("file_missing"));
    // Anything else is classified as any filesystem error is.
    let error = listed("opening", path)(ErrorKind::PermissionDenied.into());
    assert_eq!(error.kind(), ConnectorErrorKind::Config);
    let error = listed("opening", path)(Refusal::NotRegular.into());
    assert_eq!(error.code(), Some(NOT_A_REGULAR_FILE));
}

#[test]
fn work_that_keeps_losing_is_tried_a_bounded_number_of_times() {
    let mut tries = 0;
    let lost = retried("opening", || {
        tries += 1;
        Ok(None::<()>)
    });
    assert_eq!(lost.unwrap_err().kind(), ConnectorErrorKind::Transient);
    assert_eq!(tries, PUBLISH_ATTEMPTS);
    // It ends with the first value, or the first error.
    let mut tries = 0;
    let won = retried("opening", || {
        tries += 1;
        Ok((tries == 3).then_some(tries))
    });
    assert_eq!((won.unwrap(), tries), (3, 3));
    let mut tries = 0;
    let failed = retried("opening", || {
        tries += 1;
        Err::<Option<()>, _>(rdlt_connector::ConnectorError::data("no"))
    });
    assert_eq!(
        (failed.unwrap_err().kind(), tries),
        (ConnectorErrorKind::Data, 1)
    );
}
