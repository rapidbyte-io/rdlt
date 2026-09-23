use std::num::NonZeroUsize;

use bytes::Bytes;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{Partition, ReadRequest, ReadStream, Source, SourceConnector, Streams, source_factory};
use crate::catalog::StreamSpec;
use crate::cursor::Cursor;
use crate::emitter::Emitter;
use crate::error::{ConnectorError, ConnectorErrorKind, Result};
use crate::id::{PartitionId, StreamName};
use crate::sink::{Push, SourceEvent, partition_channel};
use crate::spec::{ConnectContext, Role};
use crate::state::StreamState;

#[derive(Deserialize, JsonSchema)]
struct CounterConfig {
    limit: u64,
}

struct Counter {
    limit: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct Next {
    n: u64,
}

impl SourceConnector for Counter {
    const ID: &'static str = "io.test.counter";
    const VERSION: &'static str = "1.2.3";
    type Config = CounterConfig;

    async fn connect(config: CounterConfig, _context: &ConnectContext) -> Result<Self> {
        if config.limit > 100 {
            return Err(ConnectorError::config("limit is over 100"));
        }
        Ok(Self {
            limit: config.limit,
        })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new().with(Numbers).with(Silent)
    }
}

struct Numbers;

impl ReadStream<Counter> for Numbers {
    type Cursor = Next;
    const CURSOR_VERSION: u16 = 2;

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(StreamName::new("numbers").unwrap())
    }

    async fn read(
        &self,
        source: &Counter,
        _partition: &Partition,
        cursor: Next,
        out: &mut Emitter<Next>,
    ) -> Result<()> {
        if source.limit == 42 {
            return Err(ConnectorError::data("forty-two rows are refused"));
        }
        for n in cursor.n..source.limit {
            out.rows(&[json!({ "n": n })]).await?;
            out.checkpoint(&Next { n: n + 1 }).await?;
        }
        Ok(())
    }

    async fn committed(&self, _source: &Counter, cursors: &[(PartitionId, Next)]) -> Result<()> {
        match cursors.iter().find(|(_, next)| next.n == 99) {
            Some((partition, _)) => {
                Err(ConnectorError::data(format!("{partition} committed n 99")))
            }
            None => Ok(()),
        }
    }
}

/// A stream that relies on every default of [`ReadStream`].
struct Silent;

impl ReadStream<Counter> for Silent {
    type Cursor = ();

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(StreamName::new("silent").unwrap())
    }

    async fn read(
        &self,
        _source: &Counter,
        _partition: &Partition,
        _cursor: (),
        _out: &mut Emitter<()>,
    ) -> Result<()> {
        Ok(())
    }
}

fn numbers() -> StreamName {
    StreamName::new("numbers").unwrap()
}

async fn connect(limit: u64) -> Box<dyn Source> {
    source_factory::<Counter>()
        .connect(json!({ "limit": limit }), ConnectContext::new())
        .await
        .unwrap()
}

async fn read_all(source: &dyn Source, cursor: Option<Cursor>) -> (Result<()>, Vec<SourceEvent>) {
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(64).unwrap());
    let request = ReadRequest {
        stream: numbers(),
        partition: Partition::single(),
        cursor,
    };
    let read = source.read(request, sink);
    let collect = async {
        let mut events = Vec::new();
        while let Some(event) = feed.recv().await {
            events.push(event);
        }
        events
    };
    tokio::join!(read, collect)
}

fn rows(events: &[SourceEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| match event {
            SourceEvent::Push(Push::Json(json)) => Some(String::from_utf8(json.to_vec()).unwrap()),
            _ => None,
        })
        .collect()
}

#[test]
fn the_factory_publishes_the_connector_identity_and_config_schema() {
    let factory = source_factory::<Counter>();
    let spec = factory.spec();
    assert_eq!(spec.id.as_str(), "io.test.counter");
    assert_eq!(spec.version, "1.2.3");
    assert_eq!(spec.role, Role::Source);
    assert_eq!(spec.config_schema["properties"]["limit"]["type"], "integer");
}

#[tokio::test]
async fn invalid_configuration_fails_before_connecting() {
    let factory = source_factory::<Counter>();
    let error = factory
        .connect(json!({ "limit": "many" }), ConnectContext::new())
        .await
        .err()
        .unwrap();
    assert_eq!(error.code(), Some("config_invalid"));
    let refused = factory
        .connect(json!({ "limit": 101 }), ConnectContext::new())
        .await
        .err()
        .unwrap();
    assert_eq!(refused.kind(), ConnectorErrorKind::Config);
}

#[tokio::test]
async fn discover_and_plan_use_the_registered_streams() {
    let source = connect(3).await;
    source.check().await.unwrap();
    let catalog = source.discover().await.unwrap();
    assert_eq!(
        catalog
            .iter()
            .map(|stream| stream.name().to_string())
            .collect::<Vec<_>>(),
        ["numbers", "silent"]
    );
    assert_eq!(
        source
            .plan(&numbers(), &StreamState::default())
            .await
            .unwrap(),
        vec![Partition::single()]
    );
}

#[tokio::test]
async fn a_read_from_the_start_emits_every_row_with_checkpoints() {
    let source = connect(3).await;
    let (result, events) = read_all(source.as_ref(), None).await;
    result.unwrap();
    assert_eq!(
        rows(&events),
        [r#"[{"n":0}]"#, r#"[{"n":1}]"#, r#"[{"n":2}]"#]
    );
    assert_eq!(events.len(), 6);
}

#[tokio::test]
async fn a_read_resumes_from_a_cursor() {
    let source = connect(3).await;
    let cursor = Cursor::encode(2, &Next { n: 2 }).unwrap();
    let (result, events) = read_all(source.as_ref(), Some(cursor)).await;
    result.unwrap();
    assert_eq!(rows(&events), [r#"[{"n":2}]"#]);
}

#[tokio::test]
async fn a_cursor_in_another_format_is_refused() {
    let source = connect(3).await;
    let cursor = Cursor::encode(1, &Next { n: 2 }).unwrap();
    let (result, _) = read_all(source.as_ref(), Some(cursor)).await;
    assert_eq!(result.unwrap_err().code(), Some("cursor_version"));
}

#[tokio::test]
async fn a_stopped_read_ends_cleanly() {
    let source = connect(100).await;
    let (sink, feed) = partition_channel(NonZeroUsize::MIN);
    feed.stop();
    let request = ReadRequest {
        stream: numbers(),
        partition: Partition::single(),
        cursor: None,
    };
    source.read(request, sink).await.unwrap();
}

#[tokio::test]
async fn unknown_streams_are_config_errors() {
    let source = connect(1).await;
    let other = StreamName::new("other").unwrap();
    let (sink, _feed) = partition_channel(NonZeroUsize::MIN);
    let request = ReadRequest {
        stream: other.clone(),
        partition: Partition::single(),
        cursor: None,
    };
    assert_eq!(
        source.read(request, sink).await.unwrap_err().code(),
        Some("unknown_stream")
    );
    assert_eq!(
        source
            .plan(&other, &StreamState::default())
            .await
            .unwrap_err()
            .code(),
        Some("unknown_stream")
    );
    assert_eq!(
        source.committed(&other, &[]).await.unwrap_err().code(),
        Some("unknown_stream")
    );
}

#[tokio::test]
async fn committed_cursors_reach_the_stream_decoded() {
    let source = connect(1).await;
    let whole = Partition::single().id().clone();
    let fine = [(whole.clone(), Cursor::encode(2, &Next { n: 4 }).unwrap())];
    source.committed(&numbers(), &fine).await.unwrap();
    let sentinel = [(whole.clone(), Cursor::encode(2, &Next { n: 99 }).unwrap())];
    assert_eq!(
        source
            .committed(&numbers(), &sentinel)
            .await
            .unwrap_err()
            .to_string(),
        "whole committed n 99"
    );
    let malformed = [(whole, Cursor::new(2, Bytes::from_static(b"{")).unwrap())];
    assert_eq!(
        source
            .committed(&numbers(), &malformed)
            .await
            .unwrap_err()
            .kind(),
        ConnectorErrorKind::Data
    );
}

#[tokio::test]
async fn default_stream_methods_plan_one_partition_and_accept_commits() {
    let source = connect(1).await;
    let silent = StreamName::new("silent").unwrap();
    assert_eq!(
        source.plan(&silent, &StreamState::default()).await.unwrap(),
        vec![Partition::single()]
    );
    let cursors = [(
        Partition::single().id().clone(),
        Cursor::encode(1, &()).unwrap(),
    )];
    source.committed(&silent, &cursors).await.unwrap();
    assert_eq!(
        format!("{:?}", Streams::<Counter>::default()),
        "Streams { count: 0 }"
    );
}

#[tokio::test]
async fn a_failing_read_reports_its_error() {
    let source = connect(42).await;
    let (result, events) = read_all(source.as_ref(), None).await;
    assert_eq!(result.unwrap_err().kind(), ConnectorErrorKind::Data);
    assert!(events.is_empty());
}
