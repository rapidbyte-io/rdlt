//! The connector error taxonomy.
//!
//! Connectors CLASSIFY; the host ACTS. `Transient` and `RateLimited` are
//! retried by the engine with backoff (honoring a server's wait hint when
//! one came with the failure); `Fatal` aborts the run. A connector never
//! writes a retry loop of its own — its whole responsibility is choosing
//! the variant truthfully.
//!
//! Context attaches through [`SourceError::context`] /
//! [`DestinationError::context`], which wrap the variant's INNER cause.
//! Wrapping the rendered `Display` instead prints the classification
//! frame twice ("transient source error: …: transient source error: …") —
//! a defect that arose independently in two connectors before this method
//! existed, and cannot be expressed through it.

use std::time::Duration;

/// The boxed cause every classified failure carries.
///
/// A boxed `std::error::Error` rather than a generic parameter, on
/// purpose: causes come from whatever library a connector wraps (a
/// database driver, an HTTP client, an object store), and a host only
/// classifies and renders them. `Send + Sync + 'static` so a cause can
/// cross task boundaries.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// A source failure, classified for the host.
///
/// Choosing the variant IS the connector's judgment call, and both wrong
/// directions cost real runs: a permanent failure classified transient
/// burns the whole retry budget before reporting the true cause; a blip
/// classified fatal aborts a run that a single retry would have saved.
///
/// `#[non_exhaustive]`: classifications can grow without a breaking
/// change — match with a wildcard arm.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SourceError {
    /// Worth another attempt: the host retries with backoff and jitter.
    #[error("transient source error: {0}")]
    Transient(#[source] BoxError),
    /// The server asked for pacing. The host waits (`retry_after` when
    /// the server named a window) and retries.
    ///
    /// The `Display` carries the inner message, so context attached by
    /// wrappers stays visible without walking the `source()` chain.
    #[error("source rate limited: {source}")]
    RateLimited {
        /// The server's own wait hint (typically a `Retry-After`).
        /// `None` means it gave none and the host falls back to its
        /// backoff curve — carrying the hint matters because guessing
        /// wastes budget the server already priced.
        retry_after: Option<Duration>,
        /// What reported the limit.
        #[source]
        source: BoxError,
    },
    /// Retrying cannot help: the run aborts carrying this cause.
    #[error("fatal source error: {0}")]
    Fatal(#[source] BoxError),
}

impl SourceError {
    /// Classify a cause as worth retrying.
    pub fn transient(cause: impl Into<BoxError>) -> Self {
        SourceError::Transient(cause.into())
    }

    /// Classify a cause as rate limiting, forwarding the server's wait
    /// hint when it gave one.
    pub fn rate_limited(cause: impl Into<BoxError>, retry_after: Option<Duration>) -> Self {
        SourceError::RateLimited {
            retry_after,
            source: cause.into(),
        }
    }

    /// Classify a cause as permanent: the run aborts rather than retrying
    /// what cannot succeed.
    pub fn fatal(cause: impl Into<BoxError>) -> Self {
        SourceError::Fatal(cause.into())
    }

    /// Attach context without changing what the host will do: the variant
    /// survives (`retry_after` included) and the context wraps the INNER
    /// cause, so the classification frame renders exactly once.
    ///
    /// The match is deliberately exhaustive — `#[non_exhaustive]` does
    /// not bind the defining crate, so adding a classification refuses to
    /// compile until this method states how context attaches to it.
    #[must_use = "context returns the wrapped error; it does not mutate in place"]
    pub fn context(self, context: impl std::fmt::Display) -> Self {
        match self {
            SourceError::Transient(cause) => SourceError::transient(format!("{context}: {cause}")),
            SourceError::RateLimited {
                retry_after,
                source,
            } => SourceError::RateLimited {
                retry_after,
                source: format!("{context}: {source}").into(),
            },
            SourceError::Fatal(cause) => SourceError::fatal(format!("{context}: {cause}")),
        }
    }
}

/// A destination failure, classified for the host.
///
/// [`SourceError`]'s mirror, carrying the same judgment weight — and one
/// more obligation: a retried destination operation must be idempotent,
/// because the SPI contract makes re-committing a `(load_id, commit_seq)`
/// republish nothing.
///
/// `#[non_exhaustive]`: match with a wildcard arm.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DestinationError {
    /// Worth another attempt: the host retries with backoff and jitter.
    #[error("transient destination error: {0}")]
    Transient(#[source] BoxError),
    /// The service asked for pacing — warehouses and REST catalogs
    /// rate-limit in practice, and dropping the hint would burn backoff
    /// budget guessing at a window the server already named.
    #[error("destination rate limited: {source}")]
    RateLimited {
        /// The server's own wait hint; `None` falls back to the host's
        /// backoff curve.
        retry_after: Option<Duration>,
        /// What reported the limit.
        #[source]
        source: BoxError,
    },
    /// Retrying cannot help: the run aborts carrying this cause.
    #[error("fatal destination error: {0}")]
    Fatal(#[source] BoxError),
}

impl DestinationError {
    /// Classify a cause as worth retrying.
    pub fn transient(cause: impl Into<BoxError>) -> Self {
        DestinationError::Transient(cause.into())
    }

    /// Classify a cause as rate limiting, forwarding the server's wait
    /// hint when it gave one.
    pub fn rate_limited(cause: impl Into<BoxError>, retry_after: Option<Duration>) -> Self {
        DestinationError::RateLimited {
            retry_after,
            source: cause.into(),
        }
    }

    /// Classify a cause as permanent: the run aborts rather than retrying
    /// what cannot succeed.
    pub fn fatal(cause: impl Into<BoxError>) -> Self {
        DestinationError::Fatal(cause.into())
    }

    /// Attach context without changing what the host will do — the same
    /// single-frame rule and compiler-forced exhaustiveness as
    /// [`SourceError::context`].
    #[must_use = "context returns the wrapped error; it does not mutate in place"]
    pub fn context(self, context: impl std::fmt::Display) -> Self {
        match self {
            DestinationError::Transient(cause) => {
                DestinationError::transient(format!("{context}: {cause}"))
            }
            DestinationError::RateLimited {
                retry_after,
                source,
            } => DestinationError::RateLimited {
                retry_after,
                source: format!("{context}: {source}").into(),
            },
            DestinationError::Fatal(cause) => {
                DestinationError::fatal(format!("{context}: {cause}"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The classification frames are an operator-facing contract:
    /// downstream tests pin these exact spellings.
    #[test]
    fn the_six_frames_render_verbatim() {
        assert!(
            SourceError::transient("x")
                .to_string()
                .starts_with("transient source error: ")
        );
        assert!(
            SourceError::rate_limited("x", None)
                .to_string()
                .starts_with("source rate limited: ")
        );
        assert!(
            SourceError::fatal("x")
                .to_string()
                .starts_with("fatal source error: ")
        );
        assert!(
            DestinationError::transient("x")
                .to_string()
                .starts_with("transient destination error: ")
        );
        assert!(
            DestinationError::rate_limited("x", None)
                .to_string()
                .starts_with("destination rate limited: ")
        );
        assert!(
            DestinationError::fatal("x")
                .to_string()
                .starts_with("fatal destination error: ")
        );
    }

    /// Context keeps the classification (retry_after included) and renders
    /// the frame exactly once, wrapping the inner cause.
    #[test]
    fn context_preserves_classification_and_renders_one_frame() {
        let wrapped = SourceError::transient("HTTP 502").context("stream `users`");
        let rendered = wrapped.to_string();
        assert_eq!(
            rendered.matches("transient source error:").count(),
            1,
            "{rendered}"
        );
        assert!(rendered.contains("stream `users`: HTTP 502"), "{rendered}");
        assert!(matches!(wrapped, SourceError::Transient(_)));

        let wrapped = SourceError::rate_limited("HTTP 429", Some(Duration::from_secs(7)))
            .context("stream `users`");
        assert_eq!(
            wrapped.to_string().matches("source rate limited:").count(),
            1
        );
        match wrapped {
            SourceError::RateLimited { retry_after, .. } => {
                assert_eq!(retry_after, Some(Duration::from_secs(7)));
            }
            other => panic!("classification must survive context: {other:?}"),
        }

        let wrapped = DestinationError::fatal("HTTP 404").context("table `events`");
        assert_eq!(
            wrapped
                .to_string()
                .matches("fatal destination error:")
                .count(),
            1
        );
        assert!(matches!(wrapped, DestinationError::Fatal(_)));
    }

    /// Chained context nests outermost-last-applied-first and still
    /// renders a single frame.
    #[test]
    fn chained_context_renders_one_frame() {
        let rendered = SourceError::fatal("boom")
            .context("inner step")
            .context("outer step")
            .to_string();
        assert_eq!(rendered.matches("fatal source error:").count(), 1);
        assert!(
            rendered.contains("outer step: inner step: boom"),
            "{rendered}"
        );
    }

    /// The cause chain stays walkable: the boxed cause is the `source()`.
    #[test]
    fn the_inner_cause_is_reachable_through_source() {
        let error = SourceError::fatal(std::io::Error::other("disk gone"));
        let cause = std::error::Error::source(&error).expect("cause attached");
        assert!(cause.to_string().contains("disk gone"));
    }
}
