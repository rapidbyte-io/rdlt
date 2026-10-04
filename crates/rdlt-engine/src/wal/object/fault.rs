//! What an object store's answers mean to a log: an outcome the contract names, a refusal no retry
//! changes, or a failure the next attempt may not meet.

use std::io;

use crate::limits::{
    WAL_STORAGE_DENIED, WAL_STORAGE_REFUSED, WAL_STORAGE_UNAVAILABLE, WAL_STORAGE_UNSUPPORTED,
    WAL_UNREADABLE,
};

/// A refusal of an object store's request that no retry changes, as a certificate its client
/// refused or a request the store answers as malformed: an `object_store` store's client puts
/// it among an error's sources, and the log reports the error at once, as `wal_storage_refused`.
#[derive(Debug, thiserror::Error)]
#[error("the object store's request was refused for good: {reason}")]
pub struct StoreRefusal {
    reason: String,
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl StoreRefusal {
    /// A refusal for `reason`, caused by `source` where there is one.
    pub fn new(
        reason: impl Into<String>,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    ) -> Self {
        Self {
            reason: reason.into(),
            source,
        }
    }
}

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
    /// The store's client refused the request at `key` for good.
    #[error("the object store refused the request at {key} for good")]
    Refused {
        /// The object asked for.
        key: String,
        /// The store's answer, a [`StoreRefusal`] among its sources.
        #[source]
        source: object_store::Error,
    },
    /// A listing of `dir` held more objects than a log keeps there.
    #[error("the listing of {dir} holds more than {most} objects")]
    Crowded {
        /// The directory listed.
        dir: String,
        /// Objects a listing holds at most.
        most: usize,
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
            ObjectFault::Crowded { .. } => io::ErrorKind::InvalidData,
            ObjectFault::Refused { .. } => io::ErrorKind::ConnectionRefused,
        };
        Self::new(kind, fault)
    }
}

/// What a request asks, which decides what a name taken means.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Ask {
    /// A create where no object of the name exists: a name taken is its answer.
    Create,
    /// Anything else: a conflict a store answers it with passes, as S3's does.
    Other,
}

/// What an attempt's answer means.
#[derive(Debug)]
pub(super) enum Answer {
    /// An answer the contract names, or a refusal: reported as it is.
    Final(io::Error),
    /// A failure another attempt may not meet.
    Transient(object_store::Error),
}

/// What `error`, the store's answer at `key` to `ask`, means to the log.
pub(super) fn answer(key: &str, error: object_store::Error, ask: Ask) -> Answer {
    use object_store::Error as Store;
    if refused(&error) {
        let key = key.to_owned();
        return Answer::Final(ObjectFault::Refused { key, source: error }.into());
    }
    match error {
        Store::NotFound { .. } => Answer::Final(io::Error::new(io::ErrorKind::NotFound, error)),
        // A store answers a create of a name taken with a failed precondition, or with no
        // change, as S3 compatible stores may.
        Store::AlreadyExists { .. } | Store::Precondition { .. } | Store::NotModified { .. }
            if ask == Ask::Create =>
        {
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
        ObjectFault::Crowded { .. } => WAL_UNREADABLE,
        ObjectFault::Refused { .. } => WAL_STORAGE_REFUSED,
    })
}

/// Whether a [`StoreRefusal`] is among `error`'s causes, or those an I/O error among them holds.
fn refused(error: &(dyn std::error::Error + 'static)) -> bool {
    if error.is::<StoreRefusal>() {
        return true;
    }
    let held = error
        .downcast_ref::<io::Error>()
        .and_then(io::Error::get_ref)
        .is_some_and(|inner| refused(inner));
    held || error.source().is_some_and(refused)
}
