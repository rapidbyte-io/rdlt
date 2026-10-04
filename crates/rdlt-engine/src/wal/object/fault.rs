//! What an object store's answers mean to a log: an outcome the contract names, a refusal no retry
//! changes, or a failure the next attempt may not meet.

use std::io;

use crate::limits::{WAL_STORAGE_DENIED, WAL_STORAGE_UNAVAILABLE, WAL_STORAGE_UNSUPPORTED};

/// Why an object-store log refused or gave up a request, carried by the [`io::Error`] it fails
/// with.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ObjectFault {
    /// The store does not do what the log needs of it.
    #[error("the object store does not {what}")]
    Unsupported {
        /// What it does not do.
        what: &'static str,
        /// The store's answer, where it gave one.
        #[source]
        source: Option<object_store::Error>,
    },
    /// The store refused the credentials, or what they may do, at `key`.
    #[error("the object store refused access to {key}")]
    Denied {
        /// The object asked for.
        key: String,
        /// The store's answer.
        #[source]
        source: object_store::Error,
    },
    /// Every attempt of a request at `key` failed or ran past its deadline.
    #[error("the object store failed every attempt at {key}")]
    Unavailable {
        /// The object asked for.
        key: String,
        /// The last attempt's failure; none where it ran past its deadline.
        #[source]
        source: Option<object_store::Error>,
    },
}

impl From<ObjectFault> for io::Error {
    fn from(fault: ObjectFault) -> Self {
        let kind = match fault {
            ObjectFault::Unsupported { .. } => io::ErrorKind::Unsupported,
            ObjectFault::Denied { .. } => io::ErrorKind::PermissionDenied,
            ObjectFault::Unavailable { .. } => io::ErrorKind::TimedOut,
        };
        Self::new(kind, fault)
    }
}

/// What an attempt's answer means.
#[derive(Debug)]
pub(super) enum Answer {
    /// An answer the contract names, or a refusal: reported as it is.
    Final(io::Error),
    /// A failure another attempt may not meet.
    Transient(object_store::Error),
}

/// What `error`, the store's answer at `key`, means to the log.
pub(super) fn answer(key: &str, error: object_store::Error) -> Answer {
    use object_store::Error as Store;
    match error {
        Store::NotFound { .. } => Answer::Final(io::Error::new(io::ErrorKind::NotFound, error)),
        // A store answers a create of a name taken with a failed precondition, or with no
        // change, as S3 compatible stores may.
        Store::AlreadyExists { .. } | Store::Precondition { .. } | Store::NotModified { .. } => {
            Answer::Final(io::Error::new(io::ErrorKind::AlreadyExists, error))
        }
        Store::PermissionDenied { .. } | Store::Unauthenticated { .. } => Answer::Final(
            ObjectFault::Denied {
                key: key.to_owned(),
                source: error,
            }
            .into(),
        ),
        Store::NotSupported { .. } | Store::NotImplemented { .. } | Store::InvalidPath { .. } => {
            Answer::Final(
                ObjectFault::Unsupported {
                    what: "take a request the log makes",
                    source: Some(error),
                }
                .into(),
            )
        }
        error => Answer::Transient(error),
    }
}

/// The code of what an object-store log refused, which `error` carries, where it carries one.
pub(crate) fn code(error: &io::Error) -> Option<&'static str> {
    let fault = error.get_ref()?.downcast_ref::<ObjectFault>()?;
    Some(match fault {
        ObjectFault::Unsupported { .. } => WAL_STORAGE_UNSUPPORTED,
        ObjectFault::Denied { .. } => WAL_STORAGE_DENIED,
        ObjectFault::Unavailable { .. } => WAL_STORAGE_UNAVAILABLE,
    })
}
