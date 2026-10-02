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
    /// A JSON push.
    Json(bytes::Bytes),
    /// A checkpoint whose cursor is this many bytes.
    Checkpoint(usize),
    /// How far behind the read is.
    Behind(u64),
    /// That the stream's partitions changed.
    Replan,
    /// Nothing, once the future completes: a point the test holds the read at.
    Wait(rdlt_connector::BoxFuture<'static, ()>),
}

/// What a making source emits at each step of its read; `None` ends the read.
pub(crate) type Steps = Arc<dyn Fn(usize) -> Option<Step> + Send + Sync>;

static SOURCES: LazyLock<Mutex<BTreeMap<String, (Steps, usize)>>> = LazyLock::new(Mutex::default);

/// A source of one stream, `events`, of one partition, emitting what `steps` makes, registered
/// as `name`.
pub(crate) async fn making(name: &str, steps: Steps) -> Arc<dyn Source> {
    making_parts(name, steps, 1).await
}

/// A source as [`making`] makes it, whose stream has `parts` partitions, each emitting what
/// `steps` makes.
pub(crate) async fn making_parts(name: &str, steps: Steps, parts: usize) -> Arc<dyn Source> {
    SOURCES.lock().insert(name.to_owned(), (steps, parts));
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
    parts: usize,
}

impl SourceConnector for MakingSource {
    const ID: &'static str = "io.test.making";
    const VERSION: &'static str = "0.0.0";
    type Config = MakingConfig;

    async fn connect(config: MakingConfig, _context: &ConnectContext) -> Result<Self> {
        let (steps, parts) = SOURCES
            .lock()
            .get(&config.name)
            .cloned()
            .ok_or_else(|| ConnectorError::config("no such steps"))?;
        Ok(Self { steps, parts })
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
        source: &MakingSource,
        _state: &StreamState,
    ) -> Result<Vec<Partition>> {
        if source.parts == 1 {
            return Ok(vec![Partition::single()]);
        }
        let part = |index| {
            let id = rdlt_connector::PartitionId::parse(format!("p{index}"));
            Partition::new(id.expect("a valid partition id"))
        };
        Ok((0..source.parts).map(part).collect())
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
                Step::Json(json) => out.json(json).await?,
                // The cursor's JSON text is the bytes and its two quotes.
                Step::Checkpoint(bytes) => {
                    out.checkpoint(&"c".repeat(bytes.saturating_sub(2))).await?;
                }
                Step::Behind(records) => out.behind(records).await?,
                Step::Replan => out.replan().await?,
                Step::Wait(held) => held.await,
            }
            step += 1;
        }
        Ok(())
    }
}
