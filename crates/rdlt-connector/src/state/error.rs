//! A state record that cannot be read.

/// A state record that cannot be read.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum StateError {
    /// The key is not a state key.
    #[error("state key {key:?} is malformed")]
    MalformedKey {
        /// The key.
        key: String,
    },
    /// The value is not a state value.
    #[error("state value for {key:?} is malformed: {reason}")]
    MalformedValue {
        /// The key.
        key: String,
        /// What is wrong.
        reason: String,
    },
    /// The value was written by a newer format.
    #[error("state value for {key:?} is format {version}; this build reads format 1")]
    UnsupportedVersion {
        /// The key.
        key: String,
        /// The format found.
        version: u16,
    },
    /// The value belongs to a different key.
    #[error("state value stored under {key:?} belongs to another key")]
    KeyMismatch {
        /// The key.
        key: String,
    },
}
