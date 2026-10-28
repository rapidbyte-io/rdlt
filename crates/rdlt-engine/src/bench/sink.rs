//! Destinations that cost as little as a real one can: each encodes every batch it stages as
//! Arrow IPC into a buffer it reuses, or discards it.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::num::NonZeroU16;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::UNIX_EPOCH;

use arrow_array::RecordBatch;
use parking_lot::Mutex;
use rdlt_connector::{
    Capabilities, CommitMeta, ConnectContext, ConnectorError, DestinationConnector,
    DestinationFactory, Epoch, OpenContext, Opened, Receipt, Result, SegmentId, Session,
    TableChange, TableRef, TableWriter, TypeKind, WriteStats, destination_factory,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::tally::{Staged, tally};

/// Batches the sinks of this process have written.
static WRITTEN: AtomicU64 = AtomicU64::new(0);

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

    /// The configuration of a sink that does this through as many as `writers` writers at once.
    pub fn writing(self, writers: NonZeroU16) -> Value {
        json!({ "sinking": self, "writers": writers })
    }

    /// The configuration of a sink that does this and tallies what it stages under `name`, for
    /// [`staged`](super::staged) to take.
    pub fn tallied(self, name: &str) -> Value {
        json!({ "sinking": self, "tally": name })
    }
}

/// The factory of sinks, to serve.
pub fn sink_factory() -> Box<dyn DestinationFactory> {
    destination_factory::<Sink>()
}

/// A destination encoding every batch it stages as Arrow IPC into a buffer it reuses, keeping
/// nothing.
pub async fn ipc_sink() -> Arc<dyn rdlt_connector::Destination> {
    connect_sink(Sinking::Ipc.config()).await
}

/// A destination counting the rows of every batch it stages and dropping it.
pub async fn null_sink() -> Arc<dyn rdlt_connector::Destination> {
    connect_sink(Sinking::Discard.config()).await
}

/// Batches the bench's sinks in this process have written so far.
pub fn written() -> u64 {
    WRITTEN.load(Ordering::Relaxed)
}

/// A sink doing `sinking` with each batch, declaring it runs as many as `writers` writers at
/// once.
pub(super) async fn sink(
    sinking: Sinking,
    writers: NonZeroU16,
) -> Arc<dyn rdlt_connector::Destination> {
    connect_sink(sinking.writing(writers)).await
}

/// A sink connected with `config`.
async fn connect_sink(config: Value) -> Arc<dyn rdlt_connector::Destination> {
    let destination = sink_factory()
        .connect(config, ConnectContext::new())
        .await
        .expect("the sink connects");
    Arc::from(destination)
}

/// What a [`Sink`] does with each batch, the writers it declares it can run at once, one where it
/// names none, and the tally it notes what it stages in, where it names one.
#[derive(Debug, Deserialize, JsonSchema)]
struct SinkConfig {
    sinking: Sinking,
    writers: Option<NonZeroU16>,
    tally: Option<String>,
}

struct Sink {
    sinking: Sinking,
    writers: NonZeroU16,
    tally: Option<Arc<Mutex<Staged>>>,
}

impl DestinationConnector for Sink {
    const ID: &'static str = "io.rapidbyte.bench.sink";
    const VERSION: &'static str = "0.0.0";
    type Config = SinkConfig;
    type Session = SinkSession;

    fn capabilities(&self) -> Capabilities {
        let mut capabilities = Capabilities::minimal();
        capabilities.types.extend([TypeKind::Uuid, TypeKind::Json]);
        // It keeps no row a merge could replace, so a merge stages as an append does.
        capabilities.write_modes.merge = true;
        capabilities.max_parallel_writers = self.writers;
        capabilities
    }

    async fn connect(config: SinkConfig, _context: &ConnectContext) -> Result<Self> {
        Ok(Self {
            sinking: config.sinking,
            writers: config.writers.unwrap_or(NonZeroU16::MIN),
            tally: config.tally.as_deref().map(tally),
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
                tally: self.tally.as_ref().map(Arc::clone),
            },
            epoch: Epoch(1),
            landed: None,
            state: Vec::new(),
        })
    }
}

/// The sink's session: rows staged per segment, which commits count, and the tally it notes
/// what it stages in, where its sink keeps one.
#[derive(Debug)]
pub struct SinkSession {
    staged: Arc<Mutex<BTreeMap<SegmentId, u64>>>,
    sinking: Sinking,
    tally: Option<Arc<Mutex<Staged>>>,
}

impl Session for SinkSession {
    type Writer = SinkWriter;

    async fn apply_schema(&mut self, _change: &TableChange) -> Result<()> {
        Ok(())
    }

    async fn writer(&mut self, table: &TableRef) -> Result<SinkWriter> {
        let writer = SinkWriter::new(Arc::clone(&self.staged), self.sinking);
        Ok(match &self.tally {
            Some(tally) => writer.tallying(Arc::clone(tally), table.name.to_string()),
            None => writer,
        })
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
        if let Some(tally) = &self.tally {
            tally.lock().commits += 1;
        }
        Ok(Receipt {
            key: meta.key,
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
    /// The tally the writer notes its rows in, and its table's name there.
    tally: Option<(Arc<Mutex<Staged>>, String)>,
}

impl SinkWriter {
    /// A writer doing `sinking` with each batch and counting its rows into `staged`.
    pub fn new(staged: Arc<Mutex<BTreeMap<SegmentId, u64>>>, sinking: Sinking) -> Self {
        Self {
            staged,
            sinking,
            buffer: Vec::new(),
            stats: WriteStats::default(),
            tally: None,
        }
    }

    /// The writer, noting the rows it writes in `tally` as the table `table`'s.
    #[must_use]
    fn tallying(self, tally: Arc<Mutex<Staged>>, table: String) -> Self {
        Self {
            tally: Some((tally, table)),
            ..self
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
        WRITTEN.fetch_add(1, Ordering::Relaxed);
        let bytes = match self.sinking {
            Sinking::Ipc => self.encode(&batch)?.len() as u64,
            Sinking::Discard => 0,
        };
        let rows = batch.num_rows() as u64;
        *self.staged.lock().entry(segment).or_default() += rows;
        if let Some((tally, table)) = &self.tally {
            tally.lock().stage(table, rows);
        }
        self.stats.rows += rows;
        self.stats.bytes += bytes;
        Ok(())
    }

    async fn flush(&mut self) -> Result<WriteStats> {
        Ok(std::mem::take(&mut self.stats))
    }
}
