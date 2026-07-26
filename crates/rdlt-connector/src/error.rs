//! Connector error taxonomy.
//!
//! Connectors classify; the engine acts: `Transient`/`RateLimited` are retried with
//! backoff, `Fatal` aborts the run. Connectors never write retry loops of their own.

use std::time::Duration;

/// The boxed cause every connector error carries.
///
/// Deliberately a boxed `std::error::Error` rather than a generic parameter:
/// connectors report causes from whatever library they wrap (a driver error, an
/// HTTP error, an object-store error), and the engine only ever needs to
/// classify and render them. `Send + Sync + 'static` so the cause survives
/// crossing task boundaries.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// How a source failure is CLASSIFIED for the engine.
///
/// The variant chosen decides what happens next, so choosing it is the
/// connector's real responsibility: the engine retries `Transient`, waits and
/// retries `RateLimited`, and aborts on `Fatal`. A connector that classifies a
/// permanent failure as transient burns the whole retry budget losing; one that
/// classifies a blip as fatal fails a run that would have succeeded.
///
/// `#[non_exhaustive]`: new classifications can be added without a breaking
/// change, so match on it with a wildcard arm.
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
        /// The server's own wait hint (a `Retry-After`, typically). `None` means
        /// it gave none and the engine falls back to its backoff curve — the
        /// hint is worth carrying because guessing wastes budget.
        retry_after: Option<Duration>,
        /// What reported the limit.
        #[source]
        source: BoxError,
    },
    /// Run aborts with a classified error.
    #[error("fatal source error: {0}")]
    Fatal(#[source] BoxError),
}

impl SourceError {
    /// Classify a cause as retryable: the engine retries with backoff and jitter.
    pub fn transient(err: impl Into<BoxError>) -> Self {
        SourceError::Transient(err.into())
    }

    /// Classify a cause as rate limiting, passing on the server's wait hint when
    /// it gave one.
    pub fn rate_limited(err: impl Into<BoxError>, retry_after: Option<Duration>) -> Self {
        SourceError::RateLimited {
            retry_after,
            source: err.into(),
        }
    }

    /// Classify a cause as permanent: the run aborts rather than retrying
    /// something that cannot succeed.
    pub fn fatal(err: impl Into<BoxError>) -> Self {
        SourceError::Fatal(err.into())
    }
}

/// How a destination failure is CLASSIFIED for the engine.
///
/// The mirror of [`SourceError`], and it carries the same weight: the engine
/// retries `Transient`, waits and retries `RateLimited`, and aborts on `Fatal`.
/// Classification matters more here than at the source, because a retried
/// destination operation must be idempotent — the SPI contract requires that
/// re-committing a `(load_id, commit_seq)` republish nothing.
///
/// `#[non_exhaustive]`: match with a wildcard arm.
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
        /// The server's own wait hint. `None` means it gave none and the engine
        /// falls back to its backoff curve.
        retry_after: Option<Duration>,
        /// What reported the limit.
        #[source]
        source: BoxError,
    },
    /// Run aborts with a classified error.
    #[error("fatal destination error: {0}")]
    Fatal(#[source] BoxError),
}

impl DestinationError {
    /// Classify a cause as retryable: the engine retries with backoff and jitter.
    pub fn transient(err: impl Into<BoxError>) -> Self {
        DestinationError::Transient(err.into())
    }

    /// Classify a cause as rate limiting, passing on the server's wait hint when
    /// it gave one.
    pub fn rate_limited(err: impl Into<BoxError>, retry_after: Option<Duration>) -> Self {
        DestinationError::RateLimited {
            retry_after,
            source: err.into(),
        }
    }

    /// Classify a cause as permanent: the run aborts rather than retrying
    /// something that cannot succeed.
    pub fn fatal(err: impl Into<BoxError>) -> Self {
        DestinationError::Fatal(err.into())
    }
}
