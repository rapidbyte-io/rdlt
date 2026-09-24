//! Connectors that cost as little as a real one can: a source replaying batches it holds, and a
//! destination encoding each batch as Arrow IPC into a buffer it reuses.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};
use std::time::UNIX_EPOCH;

use arrow_array::RecordBatch;
use parking_lot::Mutex;
use rdlt_connector::{
    Capabilities, CommitMeta, ConnectContext, ConnectorError, DestinationConnector, Emitter, Epoch,
    OpenContext, Opened, Partition, ReadMode, ReadStream, Receipt, Result, SegmentId, Session,
    Source, SourceConnector, StreamName, StreamSpec, StreamState, Streams, TableChange, TableRef,
    TableWriter, TypeKind, WriteStats, destination_factory, source_factory,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

static REPLAYS: LazyLock<Mutex<BTreeMap<String, Vec<RecordBatch>>>> = LazyLock::new(Mutex::default);

/// A source of one stream, `events`, of one partition, pushing `batches` in order with a
/// checkpoint after each; registered as `name`.
pub async fn replay(name: &str, batches: Vec<RecordBatch>) -> Arc<dyn Source> {
    REPLAYS.lock().insert(name.to_owned(), batches);
    let source = source_factory::<Replay>()
        .connect(json!({ "name": name }), ConnectContext::new())
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
    batches: Vec<RecordBatch>,
}

impl SourceConnector for Replay {
    const ID: &'static str = "io.rapidbyte.bench.replay";
    const VERSION: &'static str = "0.0.0";
    type Config = ReplayConfig;

    async fn connect(config: ReplayConfig, _context: &ConnectContext) -> Result<Self> {
        let batches = REPLAYS
            .lock()
            .get(&config.name)
            .cloned()
            .ok_or_else(|| ConnectorError::config("no such replay"))?;
        Ok(Self { batches })
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
        for (index, batch) in source.batches.iter().enumerate().skip(next) {
            out.batch(batch.clone()).await?;
            out.checkpoint(&(index + 1)).await?;
        }
        Ok(())
    }
}

/// A destination encoding every batch it stages as Arrow IPC into a buffer it reuses, keeping
/// nothing.
pub async fn ipc_sink() -> Arc<dyn rdlt_connector::Destination> {
    let destination = destination_factory::<IpcSink>()
        .connect(json!({}), ConnectContext::new())
        .await
        .expect("the sink connects");
    Arc::from(destination)
}

#[derive(Debug, Deserialize, JsonSchema)]
struct IpcSinkConfig {}

struct IpcSink;

impl DestinationConnector for IpcSink {
    const ID: &'static str = "io.rapidbyte.bench.ipc_sink";
    const VERSION: &'static str = "0.0.0";
    type Config = IpcSinkConfig;
    type Session = SinkSession;

    fn capabilities(&self) -> Capabilities {
        let mut capabilities = Capabilities::minimal();
        capabilities.types.extend([TypeKind::Uuid, TypeKind::Json]);
        capabilities
    }

    async fn connect(_config: IpcSinkConfig, _context: &ConnectContext) -> Result<Self> {
        Ok(Self)
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    async fn open(&self, _context: &OpenContext) -> Result<Opened<SinkSession>> {
        Ok(Opened {
            session: SinkSession::default(),
            epoch: Epoch(1),
            state: Vec::new(),
        })
    }
}

/// The sink's session: rows staged per segment, which commits count.
#[derive(Debug, Default)]
pub struct SinkSession {
    staged: Arc<Mutex<BTreeMap<SegmentId, u64>>>,
}

impl Session for SinkSession {
    type Writer = SinkWriter;

    async fn apply_schema(&mut self, _change: &TableChange) -> Result<()> {
        Ok(())
    }

    async fn writer(&mut self, _table: &TableRef) -> Result<SinkWriter> {
        Ok(SinkWriter::new(Arc::clone(&self.staged)))
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

/// Encodes each batch as Arrow IPC into one buffer, cleared before each.
#[derive(Debug)]
pub struct SinkWriter {
    staged: Arc<Mutex<BTreeMap<SegmentId, u64>>>,
    buffer: Vec<u8>,
    stats: WriteStats,
}

impl SinkWriter {
    /// A writer counting rows into `staged`.
    pub fn new(staged: Arc<Mutex<BTreeMap<SegmentId, u64>>>) -> Self {
        Self {
            staged,
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
        let bytes = self.encode(&batch)?.len() as u64;
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
