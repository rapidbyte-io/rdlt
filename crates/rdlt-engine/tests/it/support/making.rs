//! A source that makes each push as it reads and keeps none of them, so what stays in memory is
//! what the engine holds.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use arrow_array::RecordBatch;
use parking_lot::Mutex;
use rdlt_connector::{
    ConnectContext, ConnectorError, Emitter, Partition, ReadMode, ReadStream, Result, Source,
    SourceConnector, StreamName, StreamSpec, StreamState, Streams, source_factory,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

/// One thing a making source emits.
pub(crate) enum Step {
    /// An Arrow batch.
    Batch(RecordBatch),
    /// A checkpoint whose cursor is this many bytes.
    Checkpoint(usize),
}

/// What a making source emits at each step of its read; `None` ends the read.
pub(crate) type Steps = Arc<dyn Fn(usize) -> Option<Step> + Send + Sync>;

static SOURCES: LazyLock<Mutex<BTreeMap<String, Steps>>> = LazyLock::new(Mutex::default);

/// A source of one stream, `events`, of one partition, emitting what `steps` makes, registered
/// as `name`.
pub(crate) async fn making(name: &str, steps: Steps) -> Arc<dyn Source> {
    SOURCES.lock().insert(name.to_owned(), steps);
    let source = source_factory::<MakingSource>()
        .connect(json!({ "name": name }), ConnectContext::new())
        .await
        .expect("the steps are registered");
    Arc::from(source)
}

#[derive(Debug, Deserialize, JsonSchema)]
struct MakingConfig {
    name: String,
}

struct MakingSource {
    steps: Steps,
}

impl SourceConnector for MakingSource {
    const ID: &'static str = "io.test.making";
    const VERSION: &'static str = "0.0.0";
    type Config = MakingConfig;

    async fn connect(config: MakingConfig, _context: &ConnectContext) -> Result<Self> {
        let steps = SOURCES
            .lock()
            .get(&config.name)
            .cloned()
            .ok_or_else(|| ConnectorError::config("no such steps"))?;
        Ok(Self { steps })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new().with(Events)
    }
}

struct Events;

impl ReadStream<MakingSource> for Events {
    type Cursor = String;

    fn spec(&self) -> StreamSpec {
        let name = StreamName::new("events").expect("valid stream name");
        StreamSpec::new(name).with_read_modes([ReadMode::Full, ReadMode::Incremental])
    }

    async fn partitions(
        &self,
        _source: &MakingSource,
        _state: &StreamState,
    ) -> Result<Vec<Partition>> {
        Ok(vec![Partition::single()])
    }

    async fn read(
        &self,
        source: &MakingSource,
        _partition: &Partition,
        _next: String,
        out: &mut Emitter<String>,
    ) -> Result<()> {
        let mut step = 0;
        while let Some(made) = (source.steps)(step) {
            match made {
                Step::Batch(batch) => out.batch(batch).await?,
                // The cursor's JSON text is the bytes and its two quotes.
                Step::Checkpoint(bytes) => {
                    out.checkpoint(&"c".repeat(bytes.saturating_sub(2))).await?;
                }
            }
            step += 1;
        }
        Ok(())
    }
}
