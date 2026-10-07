//! Connectors that cost as little as a real one can: a source replaying pushes it holds, and
//! destinations that encode each batch as Arrow IPC into a buffer they reuse, or discard it.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};
use std::time::UNIX_EPOCH;

use arrow_array::RecordBatch;
use bytes::Bytes;
use parking_lot::Mutex;
use rdlt_connector::{
    Capabilities, CommitMeta, ConnectContext, ConnectorError, DestinationConnector,
    DestinationFactory, Emitter, Epoch, OpenContext, Opened, Partition, ReadMode, ReadStream,
    Receipt, Result, SegmentId, Session, Source, SourceConnector, SourceFactory, StreamName,
    StreamSpec, StreamState, Streams, TableChange, TableRef, TableWriter, TypeKind, WriteStats,
    destination_factory, source_factory,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

static REPLAYS: LazyLock<Mutex<BTreeMap<String, Replayed>>> = LazyLock::new(Mutex::default);

/// What a replay pushes, in order, a checkpoint after each push.
#[derive(Clone, Debug, PartialEq)]
pub enum Replayed {
    /// Arrow batches.
    Batches(Vec<RecordBatch>),
    /// JSON records, newline-delimited, a push each.
    Json(Vec<Bytes>),
}

impl Replayed {
    /// How many pushes the replay makes.
    pub fn pushes(&self) -> usize {
        match self {
            Self::Batches(batches) => batches.len(),
            Self::Json(pushes) => pushes.len(),
        }
    }
}

/// Registers `replayed` as the replay `name`, which a source connected with
/// [`replay_config`]`(name)` pushes, in this process or served from it.
pub fn register(name: &str, replayed: Replayed) {
    REPLAYS.lock().insert(name.to_owned(), replayed);
}

/// The configuration of a replay source pushing the replay registered as `name`.
pub fn replay_config(name: &str) -> Value {
    json!({ "name": name })
}

/// The factory of replay sources, to serve.
pub fn replay_factory() -> Box<dyn SourceFactory> {
    source_factory::<Replay>()
}

/// A source of one stream, `events`, of one partition, pushing `replayed` in order with a
/// checkpoint after each push; registered as `name`.
pub async fn replay(name: &str, replayed: Replayed) -> Arc<dyn Source> {
    register(name, replayed);
    let source = replay_factory()
        .connect(replay_config(name), ConnectContext::new())
        .await
        .expect("the replay is registered");
    Arc::from(source)
}

/// Names the registered replay a [`Replay`] pushes.
#[derive(Debug, Deserialize, JsonSchema)]
pub(super) struct ReplayConfig {
    name: String,
}

/// The source [`replay`] connects.
pub(super) struct Replay {
    replayed: Replayed,
}

impl SourceConnector for Replay {
    const ID: &'static str = "io.rapidbyte.bench.replay";
    const VERSION: &'static str = "0.0.0";
    type Config = ReplayConfig;

    async fn connect(config: ReplayConfig, _context: &ConnectContext) -> Result<Self> {
        let replayed = REPLAYS
            .lock()
            .get(&config.name)
            .cloned()
            .ok_or_else(|| ConnectorError::config("no such replay"))?;
        Ok(Self { replayed })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new().with(Events)
    }
}

struct Events;

impl ReadStream<Replay> for Events {
    type Cursor = usize;

    fn spec(&self) -> StreamSpec {
        let name = StreamName::new("events").expect("a valid stream name");
        StreamSpec::new(name).with_read_modes([ReadMode::Full])
    }

    async fn partitions(&self, _source: &Replay, _state: &StreamState) -> Result<Vec<Partition>> {
        Ok(vec![Partition::single()])
    }

    async fn read(
        &self,
        source: &Replay,
        _partition: &Partition,
        next: usize,
        out: &mut Emitter<usize>,
    ) -> Result<()> {
        for index in next..source.replayed.pushes() {
            match &source.replayed {
                Replayed::Batches(batches) => out.batch(batches[index].clone()).await?,
                Replayed::Json(pushes) => out.json(pushes[index].clone()).await?,
            }
            out.checkpoint(&(index + 1)).await?;
        }
        Ok(())
    }
}

/// What a sink does with each batch it stages, keeping nothing.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Sinking {
    /// Encodes it as Arrow IPC into a buffer it reuses: the least a real destination does.
    Ipc,
    /// Counts its rows and drops it, so a run measures the engine alone.
    Discard,
}

impl Sinking {
    /// The configuration of a sink that does this.
    pub fn config(self) -> Value {
        json!({ "sinking": self })
    }
}

/// The factory of sinks, to serve.
pub fn sink_factory() -> Box<dyn DestinationFactory> {
    destination_factory::<Sink>()
}

/// A destination encoding every batch it stages as Arrow IPC into a buffer it reuses, keeping
/// nothing.
pub async fn ipc_sink() -> Arc<dyn rdlt_connector::Destination> {
    sink(Sinking::Ipc).await
}

/// A destination counting the rows of every batch it stages and dropping it.
pub async fn null_sink() -> Arc<dyn rdlt_connector::Destination> {
    sink(Sinking::Discard).await
}

/// A sink doing `sinking` with each batch.
async fn sink(sinking: Sinking) -> Arc<dyn rdlt_connector::Destination> {
    let destination = sink_factory()
        .connect(sinking.config(), ConnectContext::new())
        .await
        .expect("the sink connects");
    Arc::from(destination)
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SinkConfig {
    sinking: Sinking,
}

struct Sink {
    sinking: Sinking,
}

impl DestinationConnector for Sink {
    const ID: &'static str = "io.rapidbyte.bench.sink";
    const VERSION: &'static str = "0.0.0";
    type Config = SinkConfig;
    type Session = SinkSession;

    fn capabilities(&self) -> Capabilities {
        let mut capabilities = Capabilities::minimal();
        capabilities.types.extend([TypeKind::Uuid, TypeKind::Json]);
        capabilities
    }

    async fn connect(config: SinkConfig, _context: &ConnectContext) -> Result<Self> {
        Ok(Self {
            sinking: config.sinking,
        })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    async fn open(&self, _context: &OpenContext) -> Result<Opened<SinkSession>> {
        Ok(Opened {
            session: SinkSession {
                staged: Arc::default(),
                sinking: self.sinking,
            },
            epoch: Epoch(1),
            state: Vec::new(),
        })
    }
}

/// The sink's session: rows staged per segment, which commits count.
#[derive(Debug)]
pub struct SinkSession {
    staged: Arc<Mutex<BTreeMap<SegmentId, u64>>>,
    sinking: Sinking,
}

impl Session for SinkSession {
    type Writer = SinkWriter;

    async fn apply_schema(&mut self, _change: &TableChange) -> Result<()> {
        Ok(())
    }

    async fn writer(&mut self, _table: &TableRef) -> Result<SinkWriter> {
        Ok(SinkWriter::new(Arc::clone(&self.staged), self.sinking))
    }

    async fn discard_staged(&mut self) -> Result<()> {
        Ok(())
    }

    async fn commit(&mut self, meta: &CommitMeta) -> Result<Receipt> {
        let mut staged = self.staged.lock();
        let rows = meta
            .segments
            .iter()
            .filter_map(|segment| staged.remove(&segment))
            .sum();
        Ok(Receipt {
            load_id: meta.load_id,
            commit_seq: meta.commit_seq,
            committed_at: UNIX_EPOCH,
            rows,
            bytes: 0,
        })
    }

    async fn close(self) -> Result<()> {
        Ok(())
    }
}

/// Does what its sink does with each batch, as Arrow IPC into one buffer cleared before each,
/// or nothing.
#[derive(Debug)]
pub struct SinkWriter {
    staged: Arc<Mutex<BTreeMap<SegmentId, u64>>>,
    sinking: Sinking,
    buffer: Vec<u8>,
    stats: WriteStats,
}

impl SinkWriter {
    /// A writer doing `sinking` with each batch and counting its rows into `staged`.
    pub fn new(staged: Arc<Mutex<BTreeMap<SegmentId, u64>>>, sinking: Sinking) -> Self {
        Self {
            staged,
            sinking,
            buffer: Vec::new(),
            stats: WriteStats::default(),
        }
    }

    /// Encodes `batch` as an Arrow IPC stream into the buffer, returning the encoding.
    pub fn encode(&mut self, batch: &RecordBatch) -> Result<&[u8]> {
        self.buffer.clear();
        let mut writer =
            arrow_ipc::writer::StreamWriter::try_new(&mut self.buffer, &batch.schema())
                .map_err(|error| ConnectorError::internal(error.to_string()))?;
        writer
            .write(batch)
            .and_then(|()| writer.finish())
            .map_err(|error| ConnectorError::internal(error.to_string()))?;
        Ok(&self.buffer)
    }
}

impl TableWriter for SinkWriter {
    async fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> Result<()> {
        let bytes = match self.sinking {
            Sinking::Ipc => self.encode(&batch)?.len() as u64,
            Sinking::Discard => 0,
        };
        let rows = batch.num_rows() as u64;
        *self.staged.lock().entry(segment).or_default() += rows;
        self.stats.rows += rows;
        self.stats.bytes += bytes;
        Ok(())
    }

    async fn flush(&mut self) -> Result<WriteStats> {
        Ok(std::mem::take(&mut self.stats))
    }
}
