//! The source framework: implement [`SourceConnector`], get the SPI.
//!
//! What the framework owns — the plumbing every source used to hand-roll
//! in its own SPI-impl shell: `spec()` assembly, stream declaration
//! dispatch, the per-read hand-off, and the closed-channel idiom. What
//! the author owns: how streams are declared from the config and how one
//! stream's records are produced.

use async_trait::async_trait;
use rdlt_connector::core::Cursor;
use rdlt_connector::{
    ChannelClosed, ConnectorSpec, ReadRequest, RecordBatch, RecordsOut, Source, SourceError,
    StreamSpec,
};
use std::ops::ControlFlow;

use crate::config::Document;

/// A connector authored on the framework.
///
/// Implementers are ASSEMBLED from a validated [`Document`] (the
/// framework never constructs one around validation) and then serve the
/// three read-side questions: what streams exist, is the system
/// reachable, and how does one stream's data flow into a [`Feed`].
#[async_trait]
pub trait SourceConnector: Send + Sync + 'static {
    /// The connector's stable identifier (`postgres`, `rest`).
    const NAME: &'static str;
    /// The connector's own version, independent of the host's —
    /// spell it `env!("CARGO_PKG_VERSION")` in the connector crate.
    const VERSION: &'static str;

    /// The validated configuration document this connector is built
    /// from.
    type Config: Document;

    /// Build the runtime from an already-validated config (clients,
    /// pools — whatever must exist once per connector rather than once
    /// per read). Errors are the config error type: assembly failures
    /// are configuration problems an embedder should get typed.
    fn assemble(config: Self::Config) -> Result<Self, <Self::Config as Document>::Error>
    where
        Self: Sized;

    /// The config document's generated JSON schema, when the connector
    /// provides one (spell it `Some(config::schema_of::<Self::Config>())`
    /// under the sdk's `schema` feature). `None` means the connector
    /// does not describe its configuration.
    fn config_schema() -> Option<serde_json::Value> {
        None
    }

    /// A cheap connectivity probe — the SPI's `check` contract verbatim:
    /// classify exactly as a read would; the default reports success
    /// without probing.
    async fn check(&self) -> Result<(), SourceError> {
        Ok(())
    }

    /// Declare the streams this source offers.
    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError>;

    /// Read ONE stream into `feed`. The stream is the host's request
    /// verbatim; resolving it against the connector's own configuration
    /// (and refusing an unknown one with the connector's own wording)
    /// stays here, where the config's shape is known.
    async fn read_stream(
        &self,
        stream: &StreamSpec,
        since: Option<Cursor>,
        feed: &mut Feed,
    ) -> Result<(), SourceError>;
}

/// The push handle a framework source writes into.
///
/// A thin wrapper over the SPI channel that makes the
/// closed-channel-is-cancellation contract a property of the TYPE:
/// every push returns [`ControlFlow`], and `Break` means the host hung
/// up — return `Ok(())` promptly, it is an instruction to stop, never
/// an error. Before this type, every connector hand-wrote the
/// `is_err() → return Ok(())` idiom at each push site.
#[derive(Debug)]
pub struct Feed {
    out: RecordsOut,
}

impl Feed {
    /// Wrap a raw SPI handle (the framework does this; harnesses may
    /// too).
    pub fn new(out: RecordsOut) -> Self {
        Self { out }
    }

    fn flow(result: Result<(), ChannelClosed>) -> ControlFlow<()> {
        match result {
            Ok(()) => ControlFlow::Continue(()),
            Err(ChannelClosed) => ControlFlow::Break(()),
        }
    }

    /// Push raw JSON bytes (the perf path). `Break` = host hung up.
    #[must_use = "Break means the host hung up — return Ok(()) promptly"]
    pub async fn raw_json(&mut self, bytes: bytes::Bytes) -> ControlFlow<()> {
        Self::flow(self.out.raw_json(bytes).await)
    }

    /// Push programmatically built rows. `Break` = host hung up.
    #[must_use = "Break means the host hung up — return Ok(()) promptly"]
    pub async fn rows(
        &mut self,
        rows: impl IntoIterator<Item = serde_json::Value>,
    ) -> ControlFlow<()> {
        Self::flow(self.out.rows(rows).await)
    }

    /// Push a source-native Arrow batch. `Break` = host hung up.
    #[must_use = "Break means the host hung up — return Ok(()) promptly"]
    pub async fn arrow(&mut self, batch: RecordBatch) -> ControlFlow<()> {
        Self::flow(self.out.arrow(batch).await)
    }

    /// Declare rows-so-far complete up to `cursor`. `Break` = host hung
    /// up.
    #[must_use = "Break means the host hung up — return Ok(()) promptly"]
    pub async fn checkpoint(&mut self, cursor: Cursor) -> ControlFlow<()> {
        Self::flow(self.out.checkpoint(cursor).await)
    }
}

/// The SPI shell around a [`SourceConnector`] — what [`shell`] returns.
#[derive(Debug)]
pub struct SourceShell<C> {
    connector: C,
}

/// Wrap a framework connector as an SPI [`Source`].
///
/// Written once, here, instead of once per connector: `spec()` assembled
/// from the connector's constants and schema, `check`/`streams`
/// delegated, and each read's channel wrapped into a [`Feed`] before the
/// hand-off.
pub fn shell<C: SourceConnector>(connector: C) -> SourceShell<C> {
    SourceShell { connector }
}

#[async_trait]
impl<C: SourceConnector> Source for SourceShell<C> {
    fn spec(&self) -> ConnectorSpec {
        let mut spec = ConnectorSpec::new(C::NAME, C::VERSION);
        spec.config_schema = C::config_schema();
        spec
    }

    async fn check(&self) -> Result<(), SourceError> {
        self.connector.check().await
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        self.connector.streams().await
    }

    async fn read(&self, request: ReadRequest) -> Result<(), SourceError> {
        let mut feed = Feed::new(request.out);
        self.connector
            .read_stream(&request.stream, request.since, &mut feed)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, serde::Deserialize)]
    struct ProbeConfig {
        row: i64,
    }

    #[derive(Debug, thiserror::Error)]
    enum ProbeError {
        #[error("probe yaml: {0}")]
        Yaml(#[from] serde_yaml::Error),
        #[error("probe json: {0}")]
        Json(#[from] serde_json::Error),
    }

    impl Document for ProbeConfig {
        type Error = ProbeError;
        fn validate(&self) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    struct Probe {
        row: i64,
    }

    #[async_trait]
    impl SourceConnector for Probe {
        const NAME: &'static str = "probe";
        const VERSION: &'static str = "9.9.9";
        type Config = ProbeConfig;

        fn assemble(config: ProbeConfig) -> Result<Self, ProbeError> {
            Ok(Self { row: config.row })
        }

        async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
            Ok(vec![StreamSpec::new("numbers")])
        }

        async fn read_stream(
            &self,
            _stream: &StreamSpec,
            _since: Option<Cursor>,
            feed: &mut Feed,
        ) -> Result<(), SourceError> {
            if feed
                .rows([serde_json::json!({"n": self.row})])
                .await
                .is_break()
            {
                return Ok(());
            }
            Ok(())
        }
    }

    /// The shell assembles spec() from the connector's constants and
    /// serves a full read through the Feed.
    #[tokio::test]
    async fn the_shell_serves_the_spi_from_the_connector_parts() {
        let connector =
            Probe::assemble(ProbeConfig::from_yaml("row: 7").expect("valid")).expect("assemble");
        let source = shell(connector);
        let spec = source.spec();
        assert_eq!(
            (spec.name.as_str(), spec.version.as_str()),
            ("probe", "9.9.9")
        );
        assert!(spec.config_schema.is_none(), "default: no schema declared");
        source.check().await.expect("default probe");

        let (out, mut input) = rdlt_connector::records_channel(1 << 16);
        let streams = source.streams().await.expect("declared");
        source
            .read(ReadRequest::new(streams[0].clone(), None, out))
            .await
            .expect("read");
        let push = input.recv().await.expect("one push");
        match push.payload {
            rdlt_connector::PushPayload::RawJson(bytes) => {
                assert_eq!(&bytes[..], b"{\"n\":7}\n");
            }
            other => panic!("rows land as RawJson: {other:?}"),
        }
    }

    /// A closed host channel surfaces as Break from every Feed method —
    /// the connector returns Ok, exactly the SPI's cancellation
    /// contract.
    #[tokio::test]
    async fn a_closed_channel_is_break_not_error() {
        let (out, mut input) = rdlt_connector::records_channel(1 << 16);
        input.close();
        let mut feed = Feed::new(out);
        assert!(feed.rows([serde_json::json!({})]).await.is_break());
        assert!(
            feed.checkpoint(Cursor::new(serde_json::json!(1)))
                .await
                .is_break()
        );
    }
}
