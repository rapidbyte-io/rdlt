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
    /// The Display carries the inner message so context attached by
    /// wrappers stays visible without walking the source() chain.
    #[error("source rate limited: {source}")]
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

    pub fn rate_limited(err: impl Into<BoxError>, retry_after: Option<Duration>) -> Self {
        SourceError::RateLimited {
            retry_after,
            source: err.into(),
        }
    }

    pub fn fatal(err: impl Into<BoxError>) -> Self {
        SourceError::Fatal(err.into())
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DestinationError {
    /// Engine retries with backoff + jitter.
    #[error("transient destination error: {0}")]
    Transient(#[source] BoxError),
    /// Engine waits (`retry_after` honored when present) and retries —
    /// REST catalogs and warehouses rate-limit in practice, and losing
    /// the hint would burn backoff budget guessing.
    #[error("destination rate limited: {source}")]
    RateLimited {
        retry_after: Option<Duration>,
        #[source]
        source: BoxError,
    },
    /// Run aborts with a classified error.
    #[error("fatal destination error: {0}")]
    Fatal(#[source] BoxError),
}

impl DestinationError {
    pub fn transient(err: impl Into<BoxError>) -> Self {
        DestinationError::Transient(err.into())
    }

    pub fn rate_limited(err: impl Into<BoxError>, retry_after: Option<Duration>) -> Self {
        DestinationError::RateLimited {
            retry_after,
            source: err.into(),
        }
    }

    pub fn fatal(err: impl Into<BoxError>) -> Self {
        DestinationError::Fatal(err.into())
    }
}
