use std::error::Error as _;
use std::io;
use std::path::Path;

use rustix::io::Errno;

use super::{OsError, SandboxError};

/// The launcher failed, as `error` says.
fn failed(error: io::Error) -> SandboxError {
    SandboxError::failed(Path::new("/usr/bin/bwrap"), error)
}

#[test]
fn launchers_that_failed_are_equal_where_the_system_gave_one_error() {
    let denied = || failed(io::Error::from(Errno::ACCESS));
    let refused = denied();
    assert_eq!(refused, refused.clone());
    assert_eq!(refused, denied());
    // Of the same kind, each a permission denied, and of another error number.
    let unpermitted = failed(io::Error::from(Errno::PERM));
    assert_ne!(refused, unpermitted);
    assert_ne!(refused, failed(io::Error::from(Errno::NOENT)));
}

/// A step that failed, for a reason of its own.
#[derive(Debug, thiserror::Error)]
#[error("the step failed")]
struct Step(#[source] Why);

#[derive(Debug, thiserror::Error)]
#[error("the reason")]
struct Why;

#[test]
fn a_launcher_that_failed_shows_the_systems_error_and_its_causes() {
    let refused = failed(io::Error::other(Step(Why)));
    assert_eq!(refused.code(), "sandbox_failed");
    let cause = refused.source().expect("a cause");
    let os = cause.downcast_ref::<OsError>().expect("the system's error");
    assert_eq!(cause.to_string(), os.io().to_string());
    let why = cause.source().expect("the system's error's own cause");
    assert!(why.downcast_ref::<Why>().is_some(), "{why}");
}
