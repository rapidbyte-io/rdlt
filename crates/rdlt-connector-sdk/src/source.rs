//! The source framework: implement [`SourceConnector`], get the SPI.
//!
//! The framework owns the plumbing — `spec()` assembly, stream
//! declaration dispatch, the per-read hand-off, and the closed-channel
//! idiom. The author owns how streams are declared from the config and
//! how one stream's records are produced.

use std::ops::ControlFlow;

use async_trait::async_trait;
use rdlt_connector::arrow::RecordBatch;
use rdlt_connector::channel::{ChannelClosed, RecordsOut};
use rdlt_connector::core::Cursor;
use rdlt_connector::error::SourceError;
use rdlt_connector::source::{ReadRequest, Source, StreamSpec};
use rdlt_connector::spec::ConnectorSpec;

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
/// an error.
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
#[derive(Debug, Clone)]
pub struct Shell<C> {
    connector: C,
}

/// Wrap a framework connector as an SPI [`Source`]: `spec()` assembled
/// from the connector's constants and schema, `check`/`streams`
/// delegated, and each read's channel wrapped into a [`Feed`] before the
/// hand-off.
pub fn shell<C: SourceConnector>(connector: C) -> Shell<C> {
    Shell { connector }
}

/// The from-text constructor family: parse (through the config's
/// [`Document`] gate), assemble, shell. `Shell::<Rest>::from_yaml(yaml)`
/// is a running SPI source in one call.
impl<C: SourceConnector> Shell<C> {
    /// Validate an already-parsed document, assemble, and shell — the
    /// entry for a caller holding a config VALUE rather than text (a
    /// hand-built or programmatically composed document has not passed
    /// any gate yet, so this one validates).
    pub fn new(config: C::Config) -> Result<Self, <C::Config as Document>::Error> {
        config.validate()?;
        Ok(shell(C::assemble(config)?))
    }

    /// Parse YAML text, validate, assemble, shell.
    pub fn from_yaml(yaml: &str) -> Result<Self, <C::Config as Document>::Error> {
        Ok(shell(C::assemble(C::Config::from_yaml(yaml)?)?))
    }

    /// Parse JSON text, validate, assemble, shell.
    pub fn from_json(json: &str) -> Result<Self, <C::Config as Document>::Error> {
        Ok(shell(C::assemble(C::Config::from_json(json)?)?))
    }

    /// The embedder entry point: an already-parsed `serde_json::Value`
    /// straight through the same gate.
    pub fn from_value(value: serde_json::Value) -> Result<Self, <C::Config as Document>::Error> {
        Ok(shell(C::assemble(C::Config::from_value(value)?)?))
    }
}

#[async_trait]
impl<C: SourceConnector> Source for Shell<C> {
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
    use rdlt_connector::channel::{self, PushPayload};

    use super::*;

    #[derive(Debug, serde::Deserialize)]
    struct ProbeConfig {
        row: i64,
    }

    #[derive(Debug, thiserror::Error)]
    enum ProbeError {
        #[error("probe yaml: {0}")]
        Yaml(#[from] serde_yaml_ng::Error),
        #[error("probe json: {0}")]
        Json(#[from] serde_json::Error),
        #[error("probe config: {0}")]
        Invalid(String),
    }

    impl Document for ProbeConfig {
        type Error = ProbeError;
        fn validate(&self) -> Result<(), Self::Error> {
            if self.row < 0 {
                return Err(ProbeError::Invalid(format!(
                    "`row` is {} — negatives are refused",
                    self.row
                )));
            }
            Ok(())
        }
    }

    #[derive(Debug)]
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

        let (out, mut input) = channel::records(1 << 16);
        let streams = source.streams().await.expect("declared");
        source
            .read(ReadRequest::new(streams[0].clone(), None, out))
            .await
            .expect("read");
        let push = input.recv().await.expect("one push");
        match push.payload {
            PushPayload::RawJson(bytes) => {
                assert_eq!(&bytes[..], b"{\"n\":7}\n");
            }
            other => panic!("rows land as RawJson: {other:?}"),
        }
    }

    /// A closed host channel surfaces as Break from EVERY Feed method —
    /// the connector returns Ok, exactly the SPI's cancellation
    /// contract.
    #[tokio::test]
    async fn a_closed_channel_is_break_not_error() {
        let (out, mut input) = channel::records(1 << 16);
        input.close();
        let mut feed = Feed::new(out);
        assert!(feed.rows([serde_json::json!({})]).await.is_break());
        assert!(
            feed.checkpoint(Cursor::new(serde_json::json!(1)))
                .await
                .is_break()
        );
        assert!(
            feed.raw_json(bytes::Bytes::from_static(b"{}\n"))
                .await
                .is_break()
        );
        assert!(feed.arrow(rdlt_testkit::batch_of(&[1])).await.is_break());
    }

    /// A source-native Arrow batch flows through the Feed unrepackaged.
    #[tokio::test]
    async fn an_arrow_batch_arrives_as_arrow() {
        let (out, mut input) = channel::records(1 << 16);
        let mut feed = Feed::new(out);
        assert!(
            feed.arrow(rdlt_testkit::batch_of(&[1, 2, 3]))
                .await
                .is_continue()
        );
        drop(feed);
        let push = input.recv().await.expect("one push");
        match push.payload {
            PushPayload::Arrow(batch) => assert_eq!(batch.num_rows(), 3),
            other => panic!("arrow stays arrow: {other:?}"),
        }
    }

    /// The shell's from-text constructor family runs the whole path —
    /// parse through the Document gate, assemble, shell — and `new`
    /// validates a hand-built document rather than trusting it.
    #[tokio::test]
    async fn the_shell_constructors_run_the_document_gate() {
        let from_yaml = Shell::<Probe>::from_yaml("row: 7").expect("valid yaml");
        assert_eq!(from_yaml.spec().name, "probe");
        let from_json = Shell::<Probe>::from_json("{\"row\": 7}").expect("valid json");
        assert_eq!(from_json.spec().name, "probe");
        let from_value =
            Shell::<Probe>::from_value(serde_json::json!({"row": 7})).expect("valid value");
        assert_eq!(from_value.spec().name, "probe");

        let parse = Shell::<Probe>::from_yaml(": not yaml").unwrap_err();
        assert!(
            parse.to_string().starts_with("probe yaml: "),
            "the connector's own parse framing survives: {parse}"
        );
        let refused = Shell::<Probe>::new(ProbeConfig { row: -1 }).unwrap_err();
        assert_eq!(
            refused.to_string(),
            "probe config: `row` is -1 — negatives are refused",
            "new() validates the hand-built document"
        );
    }

    /// A connector that DOES declare a schema and a real probe sees both
    /// carried by the shell — the None/Ok defaults above are defaults,
    /// not the wiring.
    #[tokio::test]
    async fn the_shell_carries_a_declared_schema_and_check_verdict() {
        struct Declared;

        #[async_trait]
        impl SourceConnector for Declared {
            const NAME: &'static str = "declared";
            const VERSION: &'static str = "0.0.0";
            type Config = ProbeConfig;

            fn assemble(_config: ProbeConfig) -> Result<Self, ProbeError> {
                Ok(Self)
            }

            fn config_schema() -> Option<serde_json::Value> {
                Some(serde_json::json!({"type": "object"}))
            }

            async fn check(&self) -> Result<(), SourceError> {
                Err(SourceError::fatal("declared probe refuses"))
            }

            async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
                Ok(vec![])
            }

            async fn read_stream(
                &self,
                _stream: &StreamSpec,
                _since: Option<Cursor>,
                _feed: &mut Feed,
            ) -> Result<(), SourceError> {
                Ok(())
            }
        }

        let source = shell(Declared);
        assert_eq!(
            source.spec().config_schema,
            Some(serde_json::json!({"type": "object"}))
        );
        let refused = source.check().await.expect_err("the override propagates");
        assert!(refused.to_string().contains("declared probe refuses"));
    }
}
