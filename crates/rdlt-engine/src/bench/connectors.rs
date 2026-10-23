//! Connectors that cost as little as a real one can: a source replaying pushes it holds, and
//! destinations that encode each batch as Arrow IPC into a buffer they reuse, or discard it.

use std::collections::BTreeMap;
use std::num::{NonZeroU16, NonZeroUsize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::UNIX_EPOCH;

use arrow_array::RecordBatch;
use bytes::Bytes;
use parking_lot::Mutex;
use rdlt_connector::{
    Capabilities, CommitMeta, ConnectContext, ConnectorError, DestinationConnector,
    DestinationFactory, Emitter, Epoch, OpenContext, Opened, Partition, PartitionId, Push,
    ReadMode, ReadRequest, ReadStream, Receipt, Result, ResultExt as _, SegmentId, Session, Source,
    SourceConnector, SourceEvent, SourceFactory, StreamName, StreamSpec, StreamState, Streams,
    TableChange, TableRef, TableWriter, TypeKind, WriteStats, destination_factory,
    partition_channel, source_factory,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::{Value, json};

use super::tally::{Staged, tally};

static REPLAYS: LazyLock<Mutex<BTreeMap<String, Replayed>>> = LazyLock::new(Mutex::default);

/// Batches the sinks of this process have written.
static WRITTEN: AtomicU64 = AtomicU64::new(0);

/// What a replay pushes, in order.
#[derive(Clone, Debug, PartialEq)]
pub enum Replayed {
    /// Arrow batches.
    Batches(Vec<RecordBatch>),
    /// JSON records, newline-delimited, a push each.
    Json(Vec<Bytes>),
    /// JSON records, newline-delimited, each push's records pushed as one call of
    /// [`Emitter::rows`] makes them.
    Rows(Vec<Bytes>),
}

impl Replayed {
    /// How many pushes the replay makes.
    pub fn pushes(&self) -> usize {
        match self {
            Self::Batches(batches) => batches.len(),
            Self::Json(pushes) | Self::Rows(pushes) => pushes.len(),
        }
    }
}

/// The pushes a source makes calling [`Emitter::rows`] once with the records of each of `lines`,
/// a push of newline-delimited JSON objects each.
///
/// # Panics
///
/// Panics where a push of `lines` is not JSON a line, which no corpus's is.
pub fn rows_pushed(lines: Vec<Bytes>) -> Vec<Bytes> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime starts");
    runtime.block_on(async {
        register("rows", Replayed::Rows(lines));
        let source = connect_replay(replay_config("rows")).await;
        let (sink, mut feed) = partition_channel(NonZeroUsize::MIN);
        let request = ReadRequest::new(stream_name(0), Partition::single(), None);
        let received = async {
            let mut pushes = Vec::new();
            while let Some(event) = feed.recv().await {
                if let SourceEvent::Push(Push::Json(json)) = event {
                    pushes.push(json);
                }
            }
            pushes
        };
        let (read, pushes) = tokio::join!(source.read(request, sink), received);
        read.expect("each push is JSON a line");
        pushes
    })
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

/// The name of a replay's stream numbered `index`: `events` for the first, so a replay of one
/// stream pushes `events`, then `events_1`, `events_2` and on.
pub fn stream_name(index: usize) -> StreamName {
    let name = match index {
        0 => "events".to_owned(),
        index => format!("events_{index}"),
    };
    StreamName::new(name).expect("a valid stream name")
}

/// The configuration of a replay source pushing the replay registered as `name` from
/// `partitions` partitions of each of `streams` streams, each an even share of its pushes in
/// order.
pub fn split_replay_config(name: &str, partitions: NonZeroUsize, streams: NonZeroUsize) -> Value {
    json!({ "name": name, "partitions": partitions, "streams": streams })
}

/// `config`, the configuration of a replay source, checkpointing after every `every`-th push of
/// each partition and after its last.
pub(super) fn checkpointing(mut config: Value, every: NonZeroUsize) -> Value {
    config["every"] = json!(every);
    config
}

/// The factory of replay sources, to serve.
pub fn replay_factory() -> Box<dyn SourceFactory> {
    source_factory::<Replay>()
}

/// A source of one stream, `events`, of one partition, pushing `replayed` in order with a
/// checkpoint after each push; registered as `name`.
pub async fn replay(name: &str, replayed: Replayed) -> Arc<dyn Source> {
    register(name, replayed);
    connect_replay(replay_config(name)).await
}

/// A source pushing `replayed` from `partitions` partitions of each of `streams` streams, an even
/// share each, in order, a checkpoint after each push, the last `unreplayable` of whose streams it
/// cannot read again, each read incrementally; registered as `name`.
pub(super) async fn mixed_replay(
    name: &str,
    replayed: Replayed,
    partitions: NonZeroUsize,
    streams: NonZeroUsize,
    unreplayable: usize,
) -> Arc<dyn Source> {
    register(name, replayed);
    let config = json!({
        "name": name,
        "partitions": partitions,
        "streams": streams,
        "unreplayable": unreplayable,
    });
    connect_replay(config).await
}

/// The pushes of `pushes` that the partition numbered `partition` of the stream numbered
/// `stream` makes, of `partitions` partitions of each of `streams` streams: its even share, in
/// order.
pub(super) fn share(
    pushes: usize,
    (partitions, streams): (NonZeroUsize, NonZeroUsize),
    stream: usize,
    partition: usize,
) -> std::ops::Range<usize> {
    let units = streams.get() * partitions.get();
    let unit = stream * partitions.get() + partition;
    unit * pushes / units..(unit + 1) * pushes / units
}

/// A replay source connected with `config`, whose replay is registered.
pub(super) async fn connect_replay(config: Value) -> Arc<dyn Source> {
    let source = replay_factory()
        .connect(config, ConnectContext::new())
        .await
        .expect("the replay is registered");
    Arc::from(source)
}

/// Names the registered replay a [`Replay`] pushes, the partitions it pushes from, the streams
/// it pushes, one of each where it names none, how many of them, the last, it cannot read again,
/// none where it names none, and the pushes of a partition between its checkpoints, one where it
/// names none.
#[derive(Debug, Deserialize, JsonSchema)]
pub(super) struct ReplayConfig {
    name: String,
    partitions: Option<NonZeroUsize>,
    streams: Option<NonZeroUsize>,
    unreplayable: Option<usize>,
    every: Option<NonZeroUsize>,
}

/// The source a replay's configuration connects.
pub(super) struct Replay {
    replayed: Replayed,
    partitions: NonZeroUsize,
    streams: NonZeroUsize,
    unreplayable: usize,
    every: NonZeroUsize,
}

impl Replay {
    /// The pushes the partition numbered `partition` of the stream numbered `stream` makes: its
    /// even share, in order.
    fn share(&self, stream: usize, partition: usize) -> std::ops::Range<usize> {
        let pushes = self.replayed.pushes();
        share(pushes, (self.partitions, self.streams), stream, partition)
    }
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
        Ok(Self {
            replayed,
            partitions: config.partitions.unwrap_or(NonZeroUsize::MIN),
            streams: config.streams.unwrap_or(NonZeroUsize::MIN),
            unreplayable: config.unreplayable.unwrap_or(0),
            every: config.every.unwrap_or(NonZeroUsize::MIN),
        })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        let replayable = self.streams.get().saturating_sub(self.unreplayable);
        (0..self.streams.get()).fold(Streams::new(), |streams, index| {
            streams.with(Events {
                index,
                replayable: index < replayable,
            })
        })
    }
}

/// The stream numbered `index` of a replay, named by [`stream_name`]: read in full where its
/// source can read it again, incrementally otherwise.
struct Events {
    index: usize,
    replayable: bool,
}

impl ReadStream<Replay> for Events {
    type Cursor = usize;

    fn spec(&self) -> StreamSpec {
        let spec = StreamSpec::new(stream_name(self.index));
        if self.replayable {
            spec.with_read_modes([ReadMode::Full])
        } else {
            spec.with_read_modes([ReadMode::Incremental])
                .with_replayable(false)
        }
    }

    async fn partitions(&self, source: &Replay, _state: &StreamState) -> Result<Vec<Partition>> {
        if source.partitions == NonZeroUsize::MIN {
            return Ok(vec![Partition::single()]);
        }
        let id = |index: usize| PartitionId::parse(index.to_string()).expect("a valid partition");
        Ok((0..source.partitions.get())
            .map(|index| Partition::new(id(index)))
            .collect())
    }

    /// Pushes the partition's share from `next`, the index of the push after its last
    /// checkpoint, with a checkpoint after every `every`-th push of the share and after its last.
    async fn read(
        &self,
        source: &Replay,
        partition: &Partition,
        next: usize,
        out: &mut Emitter<usize>,
    ) -> Result<()> {
        let index = match partition.id().as_str().parse() {
            Ok(index) if index < source.partitions.get() => index,
            _ if source.partitions == NonZeroUsize::MIN => 0,
            _ => return Err(ConnectorError::config("no such partition")),
        };
        let share = source.share(self.index, index);
        for index in next.max(share.start)..share.end {
            match &source.replayed {
                Replayed::Batches(batches) => out.batch(batches[index].clone()).await?,
                Replayed::Json(pushes) => out.json(pushes[index].clone()).await?,
                Replayed::Rows(pushes) => out.rows(&records(&pushes[index])?).await?,
            }
            let pushed = index + 1;
            if (pushed - share.start).is_multiple_of(source.every.get()) || pushed == share.end {
                out.checkpoint(&pushed).await?;
            }
        }
        Ok(())
    }
}

/// The records of `lines`, a JSON object a line, each as its text.
fn records(lines: &[u8]) -> Result<Vec<&RawValue>> {
    let text = std::str::from_utf8(lines).data("reading a replay's rows")?;
    let records = text.lines().map(serde_json::from_str);
    records
        .collect::<std::result::Result<_, _>>()
        .data("reading a replay's rows")
}

/// What a sink does with each batch it stages, keeping nothing.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Sinking {
    /// Encodes it as Arrow IPC into a buffer it reuses: the least a real destination does.
    Ipc,
    /// Counts its rows and drops it, so a run measures the engine alone.
    Discard,
    /// Counts its rows and drops it, and refuses every commit, so a logged run leaves its log.
    Refuse,
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
        if self.sinking == Sinking::Refuse {
            return Err(ConnectorError::internal(
                "the bench sink refuses every commit",
            ));
        }
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
            Sinking::Discard | Sinking::Refuse => 0,
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
