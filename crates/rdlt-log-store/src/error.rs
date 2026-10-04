//! Why a log store could not be opened: the field, never its value.

use rdlt_host::SecretFault;

/// Why a log store could not be opened, naming the field at fault and never what it holds.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum LogStoreError {
    /// The configuration is not a log store's.
    #[error("the log store's configuration is not valid")]
    Document(#[source] rdlt_connector::ConnectorError),
    /// A field holds what a log store does not take.
    #[error("log store config field {field}: {why}")]
    Config {
        /// The field.
        field: &'static str,
        /// What is wrong with it.
        why: &'static str,
    },
    /// A credential's reference did not resolve.
    #[error("log store config field {field}: its secret did not resolve")]
    Secret {
        /// The field.
        field: &'static str,
        /// Why it did not resolve.
        #[source]
        source: SecretFault,
    },
    /// The object store's client could not be made.
    #[error("the object store's client could not be made")]
    Client(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// The store refused, or failed, as the log was opened in it.
    #[error("the log store could not be opened")]
    Store(#[source] rdlt_engine::Error),
}

/// What kind of failure a [`LogStoreError`] is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogStoreErrorKind {
    /// The configuration is wrong, which its operator mends.
    Config,
    /// A secret did not resolve.
    Secret,
    /// The store refused, or failed.
    Store,
}

impl LogStoreError {
    /// What kind of failure it is.
    pub fn kind(&self) -> LogStoreErrorKind {
        match self {
            Self::Document(_) | Self::Config { .. } | Self::Client(_) => LogStoreErrorKind::Config,
            Self::Secret { .. } => LogStoreErrorKind::Secret,
            Self::Store(_) => LogStoreErrorKind::Store,
        }
    }

    /// The error's stable code: `config_invalid` for a document that is not a log store's,
    /// `log_store_config` for a field that holds what a log store does not take,
    /// `secret_refused` and `secret_unresolved` for a credential, `log_store_client` for a
    /// client that could not be made, and the engine's code for the store, such as
    /// `wal_storage_unsupported`.
    pub fn code(&self) -> &str {
        match self {
            Self::Document(_) => "config_invalid",
            Self::Config { .. } => "log_store_config",
            Self::Secret {
                source: SecretFault::Refused,
                ..
            } => "secret_refused",
            Self::Secret { .. } => "secret_unresolved",
            Self::Client(_) => "log_store_client",
            Self::Store(error) => error.code().unwrap_or("log_store_failed"),
        }
    }

    /// Whether opening the store again may succeed: only where the store failed as it may not
    /// again.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Store(error) if error.is_retryable())
    }
}
