//! Connector error taxonomy.
//!
//! Connectors classify; the engine acts: `Transient`/`RateLimited` are retried with
//! backoff, `Fatal` aborts the run. Connectors never write retry loops of their own.

use std::time::Duration;

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SourceError {
    /// Engine retries with backoff + jitter.
    #[error("transient source error: {0}")]
    Transient(#[source] BoxError),
    /// Engine waits (`retry_after` honored when present) and retries.
    #[error("source rate limited")]
    RateLimited {
        retry_after: Option<Duration>,
        #[source]
        source: BoxError,
    },
    /// Run aborts with a classified error.
    #[error("fatal source error: {0}")]
    Fatal(#[source] BoxError),
}

impl SourceError {
    pub fn transient(err: impl Into<BoxError>) -> Self {
        SourceError::Transient(err.into())
    }

    pub fn fatal(err: impl Into<BoxError>) -> Self {
        SourceError::Fatal(err.into())
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DestError {
    /// Engine retries with backoff + jitter.
    #[error("transient destination error: {0}")]
    Transient(#[source] BoxError),
    /// Run aborts with a classified error.
    #[error("fatal destination error: {0}")]
    Fatal(#[source] BoxError),
}

impl DestError {
    pub fn transient(err: impl Into<BoxError>) -> Self {
        DestError::Transient(err.into())
    }

    pub fn fatal(err: impl Into<BoxError>) -> Self {
        DestError::Fatal(err.into())
    }
}
