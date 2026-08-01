//! The source half of the SPI: [`Source`] and its per-read request.

use async_trait::async_trait;

use crate::channel::RecordsOut;
use crate::core::Cursor;
use crate::error::SourceError;
use crate::spec::ConnectorSpec;
use crate::stream::StreamSpec;

/// A data source: declares streams, then pushes records for one stream
/// per [`Source::read`] call.
#[async_trait]
pub trait Source: Send + Sync + 'static {
    /// Name, version, config JSON-schema.
    fn spec(&self) -> ConnectorSpec;

    /// A cheap connectivity probe: can this source reach what it reads
    /// from, with the credentials it holds?
    ///
    /// Distinct from [`Source::read`] on purpose — an operator wants "are
    /// the credentials right" answered in seconds, without moving data.
    /// Failures classify exactly as `read` would classify them: a bad
    /// credential is fatal, an unreachable host transient.
    ///
    /// The default body returns `Ok(())` WITHOUT probing anything, so a
    /// source that has not implemented it reports success trivially — a
    /// host needing a real answer must know which connectors implement
    /// the probe. Implementations replace it wholesale.
    async fn check(&self) -> Result<(), SourceError> {
        Ok(())
    }

    /// Discover the streams this source can read.
    ///
    /// Called once per run, before any [`Source::read`]. The host plans
    /// table ownership from the result, so two streams must not claim the
    /// same destination table.
    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError>;

    /// Read ONE stream, pushing through `request.out`. The host schedules
    /// streams concurrently. Given `request.since`, the source MUST
    /// resume — never re-emit rows already covered by that cursor.
    /// Awaiting the push IS the flow control; treat
    /// [`ChannelClosed`](crate::channel::ChannelClosed) as cancellation
    /// and return promptly.
    async fn read(&self, request: ReadRequest) -> Result<(), SourceError>;
}

/// Everything a source needs to read one stream.
///
/// Deliberately an exhaustive pub-field struct (a DX choice, semver
/// gated); [`ReadRequest::new`] is the compatibility hedge, and every
/// consumer in the tree constructs through it.
#[derive(Debug)]
pub struct ReadRequest {
    /// The one stream this call must read.
    pub stream: StreamSpec,
    /// Last committed cursor — only ever a cursor the destination
    /// previously committed.
    pub since: Option<Cursor>,
    /// Push handle.
    pub out: RecordsOut,
}

impl ReadRequest {
    /// Build a request. Present so a later field addition is not a
    /// breaking change for callers constructing through here rather than
    /// with a struct literal.
    pub fn new(stream: StreamSpec, since: Option<Cursor>, out: RecordsOut) -> Self {
        Self { stream, since, out }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Probeless;

    #[async_trait]
    impl Source for Probeless {
        fn spec(&self) -> ConnectorSpec {
            ConnectorSpec::new("probeless", "0.0.0")
        }
        async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
            Ok(vec![])
        }
        async fn read(&self, _request: ReadRequest) -> Result<(), SourceError> {
            Ok(())
        }
    }

    /// The default `check` is a trivial success BY CONTRACT: a host that
    /// needs a real probe must know the connector implements one.
    #[tokio::test]
    async fn the_default_check_reports_success_without_probing() {
        assert!(Probeless.check().await.is_ok());
    }
}
