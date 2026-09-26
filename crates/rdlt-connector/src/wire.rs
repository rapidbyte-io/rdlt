//! The contract's types on the wire: each converts into its `rdlt.connector.v1` message, and each
//! message back into the type, checked as the type's constructors check it.
//!
//! Encoding cannot fail; decoding a message from the other end of a connection fails with
//! [`Invalid`] when a required field is missing, an enum holds a value this end does not know, or
//! a value breaks the type's rules.

mod catalog;
mod destination;
mod error;
mod state;
mod status;
#[cfg(test)]
mod tests;
mod types;

use std::error::Error as StdError;

#[cfg(feature = "serve")]
pub(crate) use destination::load_id;
pub use rdlt_wire::v1;
pub use status::{MALFORMED_FRAME, TRANSPORT, error, frame_error, status};

/// A message from the wire that does not decode into the contract's type.
#[derive(Debug, thiserror::Error)]
pub enum Invalid {
    /// A required field is absent.
    #[error("{0} is missing")]
    Missing(&'static str),
    /// An enum field holds its unspecified value or one this end does not know.
    #[error("{0} is unspecified or unknown")]
    Unknown(&'static str),
    /// A key that must be unique repeats.
    #[error("{0} repeats")]
    Duplicate(&'static str),
    /// A number does not fit the type.
    #[error("{0} is out of range")]
    OutOfRange(&'static str),
    /// A value breaks the type's rules.
    #[error("{what} is invalid")]
    Rejected {
        /// The value.
        what: &'static str,
        /// The rule it breaks.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },
}

impl Invalid {
    /// A `what` that breaks a rule, as `source` says.
    pub fn rejected(what: &'static str, source: impl StdError + Send + Sync + 'static) -> Self {
        Self::Rejected {
            what,
            source: Box::new(source),
        }
    }
}

/// The value of a required field.
pub(crate) fn required<T>(what: &'static str, value: Option<T>) -> Result<T, Invalid> {
    value.ok_or(Invalid::Missing(what))
}

/// `value` as a narrower integer.
pub(crate) fn narrow<T: TryFrom<u64>>(
    what: &'static str,
    value: impl Into<u64>,
) -> Result<T, Invalid> {
    T::try_from(value.into()).map_err(|_| Invalid::OutOfRange(what))
}
