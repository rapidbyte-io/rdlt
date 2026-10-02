//! A state record that cannot be read.

/// Bytes: what an error keeps of a key or a reason, as it is shown.
const SHOWN_BYTES: usize = 256;

/// `text`, a record's key or what is wrong with its value, as an error shows it: a record's
/// text is what its destination stored, so nothing in it is obeyed by a terminal and it is
/// bounded.
pub(super) fn shown(text: &str) -> String {
    crate::text::shown(text, SHOWN_BYTES)
}

/// A state record that cannot be read.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum StateError {
    /// The key is not a state key.
    #[error("state key `{key}` is malformed")]
    MalformedKey {
        /// The key, as it is shown.
        key: String,
    },
    /// The value is not a state value.
    #[error("state value for `{key}` is malformed: {reason}")]
    MalformedValue {
        /// The key, as it is shown.
        key: String,
        /// What is wrong, as it is shown.
        reason: String,
    },
    /// The value was written by a newer format.
    #[error("state value for `{key}` is format {version}; this build reads format 1")]
    UnsupportedVersion {
        /// The key, as it is shown.
        key: String,
        /// The format found.
        version: u16,
    },
    /// Two records hold the key.
    #[error("state key `{key}` is held by two records")]
    Repeated {
        /// The key, as it is shown.
        key: String,
    },
    /// The value belongs to a different key.
    #[error("state value stored under `{key}` belongs to another key")]
    KeyMismatch {
        /// The key, as it is shown.
        key: String,
    },
}
