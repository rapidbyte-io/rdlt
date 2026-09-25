//! A source that pushes the Arrow batches or JSON a test hands it, one partition per stream,
//! with a checkpoint after each push.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use arrow_array::RecordBatch;
use bytes::Bytes;
use parking_lot::Mutex;
use rdlt_connector::{
    ConnectContext, ConnectorError, Emitter, Partition, Push, ReadMode, ReadStream, Result, Source,
    SourceConnector, StreamName, StreamSpec, StreamState, Streams, TableSchema, source_factory,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

/// One stream of a batch source.
#[derive(Clone, Debug)]
pub(crate) struct BatchStream {
    pub(crate) name: String,
    pub(crate) pushes: Vec<Push>,
    pub(crate) primary_key: Option<Vec<String>>,
    pub(crate) schema: Option<TableSchema>,
    /// Whether the stream checkpoints only after its last push, so its pushes share a segment.
    pub(crate) one_segment: bool,
    /// Whether the stream never checkpoints, so its partition ends with rows after no cursor.
    pub(crate) unchecked: bool,
}

impl BatchStream {
    /// A stream pushing `batches`, with no primary key and no declared schema.
    pub(crate) fn new(name: &str, batches: Vec<RecordBatch>) -> Self {
        Self {
            name: name.to_owned(),
            pushes: batches.into_iter().map(Push::Arrow).collect(),
            primary_key: None,
            schema: None,
            one_segment: false,
            unchecked: false,
        }
    }

    /// A stream pushing each of `pushes` as JSON, with no primary key and no declared schema.
    pub(crate) fn json(name: &str, pushes: &[&str]) -> Self {
        Self {
            pushes: pushes
                .iter()
                .map(|push| Push::Json(Bytes::from(push.to_string())))
                .collect(),
            ..Self::new(name, Vec::new())
        }
    }

    /// Declares `columns` as the stream's primary key.
    pub(crate) fn primary_key(mut self, columns: &[&str]) -> Self {
        self.primary_key = Some(columns.iter().map(|column| (*column).to_owned()).collect());
        self
    }

    /// Checkpoints only after the last push.
    pub(crate) fn one_segment(mut self) -> Self {
        self.one_segment = true;
        self
    }

    /// Never checkpoints.
    pub(crate) fn unchecked(mut self) -> Self {
        self.unchecked = true;
        self
    }

    /// Declares `schema` in the catalog.
    pub(crate) fn declared(mut self, schema: TableSchema) -> Self {
        self.schema = Some(schema);
        self
    }
}

static SOURCES: LazyLock<Mutex<BTreeMap<String, Vec<BatchStream>>>> = LazyLock::new(Mutex::default);

/// A source serving `streams`, registered as `name`; registering the name again replaces them,
/// so a test can change what the next run reads.
pub(crate) async fn batches(name: &str, streams: Vec<BatchStream>) -> Arc<dyn Source> {
    SOURCES.lock().insert(name.to_owned(), streams);
    let source = source_factory::<BatchSource>()
        .connect(json!({ "name": name }), ConnectContext::new())
        .await
        .expect("the batches are registered");
    Arc::from(source)
}

#[derive(Debug, Deserialize, JsonSchema)]
struct BatchConfig {
    name: String,
}

struct BatchSource {
    streams: Vec<BatchStream>,
}

impl SourceConnector for BatchSource {
    const ID: &'static str = "io.test.batches";
    const VERSION: &'static str = "0.0.0";
    type Config = BatchConfig;

    async fn connect(config: BatchConfig, _context: &ConnectContext) -> Result<Self> {
        let streams = SOURCES
            .lock()
            .get(&config.name)
            .cloned()
            .ok_or_else(|| ConnectorError::config("no such batches"))?;
        Ok(Self { streams })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        (0..self.streams.len()).fold(Streams::new(), |streams, index| {
            streams.with(Pushing {
                index,
                stream: self.streams[index].clone(),
            })
        })
    }
}

struct Pushing {
    index: usize,
    stream: BatchStream,
}

impl ReadStream<BatchSource> for Pushing {
    type Cursor = usize;

    fn spec(&self) -> StreamSpec {
        let name = StreamName::new(&self.stream.name).expect("valid stream name");
        let mut spec =
            StreamSpec::new(name).with_read_modes([ReadMode::Full, ReadMode::Incremental]);
        if let Some(key) = &self.stream.primary_key {
            spec = spec.with_primary_key(key.iter().map(String::as_str));
        }
        if let Some(schema) = &self.stream.schema {
            spec = spec.with_schema(schema.clone());
        }
        spec
    }

    async fn partitions(
        &self,
        _source: &BatchSource,
        _state: &StreamState,
    ) -> Result<Vec<Partition>> {
        Ok(vec![Partition::single()])
    }

    async fn read(
        &self,
        source: &BatchSource,
        _partition: &Partition,
        next: usize,
        out: &mut Emitter<usize>,
    ) -> Result<()> {
        let stream = &source.streams[self.index];
        for (index, push) in stream.pushes.iter().enumerate().skip(next) {
            match push {
                Push::Json(json) => out.json(json.clone()).await?,
                Push::Arrow(batch) | Push::Changes(batch) => out.batch(batch.clone()).await?,
            }
            if !stream.unchecked && (!stream.one_segment || index + 1 == stream.pushes.len()) {
                out.checkpoint(&(index + 1)).await?;
            }
        }
        Ok(())
    }
}
