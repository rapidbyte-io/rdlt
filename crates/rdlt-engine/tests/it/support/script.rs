//! A source whose behaviour each test scripts: rows, checkpoints, faults, hangs and idling.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use parking_lot::Mutex;
use rdlt_connector::{
    Checkpointing, ConnectContext, ConnectorError, ConnectorErrorKind, Emitter, Field, LogicalType,
    Partition, PartitionId, Partitioning, ReadMode, ReadStream, Result, Source, SourceConnector,
    StreamName, StreamSpec, StreamState, Streams, TableSchema, source_factory,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;

/// What a stream pushes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PushKind {
    /// Arrow batches matching the declared schema.
    Arrow,
    /// The same rows as JSON.
    Json,
    /// Arrow batches whose `id` column is text.
    WrongType,
    /// Arrow batches whose `id` column holds nulls.
    Nulls,
}

/// A failure injected before a batch.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Fault {
    /// The script-wide batch number the failure replaces, counting from 1 across runs.
    pub(crate) batch: u64,
    pub(crate) kind: ConnectorErrorKind,
    pub(crate) retry_after: Option<Duration>,
}

/// One scripted stream.
pub(crate) struct ScriptStream {
    pub(crate) name: String,
    /// Rows each partition holds; tests grow them between runs.
    pub(crate) rows: Vec<AtomicU64>,
    pub(crate) batch_rows: u64,
    pub(crate) checkpointing: Checkpointing,
    /// Batches between checkpoints of a naturally checkpointing stream.
    pub(crate) checkpoint_every: u64,
    /// Whether the stream checkpoints after its last row.
    pub(crate) final_checkpoint: bool,
    pub(crate) push: PushKind,
    pub(crate) declares_schema: bool,
    /// Whether reads wait for more rows at their end until the engine stops them.
    pub(crate) idle: bool,
    idle_flag: AtomicBool,
    /// A partition whose read never returns and ignores stop requests.
    pub(crate) hang: Option<usize>,
}

impl ScriptStream {
    /// A stream of `partitions` partitions of `rows` rows each, pushed as Arrow in batches of
    /// `batch_rows`, checkpointing after every batch.
    pub(crate) fn new(name: &str, partitions: usize, rows: u64, batch_rows: u64) -> Self {
        Self {
            name: name.to_owned(),
            rows: (0..partitions).map(|_| AtomicU64::new(rows)).collect(),
            batch_rows,
            checkpointing: Checkpointing::Natural,
            checkpoint_every: 1,
            final_checkpoint: true,
            push: PushKind::Arrow,
            declares_schema: true,
            idle: false,
            idle_flag: AtomicBool::new(true),
            hang: None,
        }
    }

    /// Lets reads end at their last row again.
    pub(crate) fn idle_off(&self) {
        self.idle_flag.store(false, Ordering::SeqCst);
    }

    /// Adds `rows` rows to every partition.
    pub(crate) fn grow(&self, rows: u64) {
        for partition in &self.rows {
            partition.fetch_add(rows, Ordering::SeqCst);
        }
    }
}

/// Everything one scripted source does and observes.
#[derive(Default)]
pub(crate) struct Script {
    pub(crate) streams: Vec<ScriptStream>,
    /// Whether discovering the catalog fails.
    pub(crate) fail_discover: bool,
    /// Whether planning partitions fails.
    pub(crate) fail_plan: bool,
    /// Whether acknowledging committed cursors fails.
    pub(crate) fail_ack: bool,
    pub(crate) faults: Mutex<Vec<Fault>>,
    batches: AtomicU64,
    /// Every acknowledged cursor: stream, partition and resume offset.
    pub(crate) acks: Mutex<Vec<(String, String, u64)>>,
    /// Whether any read was ever asked for a checkpoint.
    pub(crate) asked: AtomicBool,
    reading: AtomicUsize,
    /// Reads started.
    pub(crate) reads: AtomicUsize,
    /// The most partitions ever read at once.
    pub(crate) peak_reading: AtomicUsize,
}

static SCRIPTS: LazyLock<Mutex<BTreeMap<String, Arc<Script>>>> = LazyLock::new(Mutex::default);

impl Script {
    /// A script of `streams`.
    pub(crate) fn new(streams: Vec<ScriptStream>) -> Self {
        Self {
            streams,
            ..Self::default()
        }
    }

    /// Adds `fault`.
    pub(crate) fn fail(self, fault: Fault) -> Self {
        self.faults.lock().push(fault);
        self
    }

    /// Registers the script as `name` and connects a source that follows it.
    pub(crate) async fn connect(self, name: &str) -> (Arc<Script>, Arc<dyn Source>) {
        let script = Arc::new(self);
        SCRIPTS.lock().insert(name.to_owned(), Arc::clone(&script));
        (Arc::clone(&script), reconnect(name).await)
    }
}

/// Connects another source following the script registered as `name`.
pub(crate) async fn reconnect(name: &str) -> Arc<dyn Source> {
    let source = source_factory::<ScriptSource>()
        .connect(json!({ "script": name }), ConnectContext::new())
        .await
        .expect("the script is registered");
    Arc::from(source)
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ScriptConfig {
    script: String,
}

struct ScriptSource {
    script: Arc<Script>,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
struct Offset {
    next: u64,
}

impl SourceConnector for ScriptSource {
    const ID: &'static str = "io.test.script";
    const VERSION: &'static str = "0.0.0";
    type Config = ScriptConfig;

    async fn connect(config: ScriptConfig, _context: &ConnectContext) -> Result<Self> {
        let script = SCRIPTS
            .lock()
            .get(&config.script)
            .cloned()
            .ok_or_else(|| ConnectorError::config("no such script"))?;
        Ok(Self { script })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    async fn discover(&self) -> Result<rdlt_connector::Catalog> {
        if self.script.fail_discover {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Transient,
                "discover failed",
            ));
        }
        self.streams()
            .catalog()
            .map_err(|_| ConnectorError::internal("the script names a stream twice"))
    }

    fn streams(&self) -> Streams<Self> {
        let streams = self.script.streams.iter().enumerate();
        streams.fold(Streams::new(), |streams, (index, stream)| {
            streams.with(Scripted {
                index,
                name: stream.name.clone(),
                checkpointing: stream.checkpointing,
                declares_schema: stream.declares_schema,
            })
        })
    }
}

struct Scripted {
    index: usize,
    name: String,
    checkpointing: Checkpointing,
    declares_schema: bool,
}

/// The schema every scripted stream declares: one non-null `id`.
pub(crate) fn schema() -> TableSchema {
    TableSchema::new(vec![Field::new("id", LogicalType::Int64, false)])
        .expect("one field is a valid schema")
}

impl ReadStream<ScriptSource> for Scripted {
    type Cursor = Offset;

    fn spec(&self) -> StreamSpec {
        let spec = StreamSpec::new(StreamName::new(&self.name).expect("valid stream name"))
            .with_read_modes([ReadMode::Full, ReadMode::Incremental])
            .with_partitioning(Partitioning::Planned)
            .with_checkpointing(self.checkpointing);
        if self.declares_schema {
            spec.with_schema(schema())
        } else {
            spec
        }
    }

    async fn partitions(
        &self,
        source: &ScriptSource,
        _state: &StreamState,
    ) -> Result<Vec<Partition>> {
        if source.script.fail_plan {
            return Err(ConnectorError::data("planning failed"));
        }
        let count = source.script.streams[self.index].rows.len();
        Ok((0..count)
            .map(|index| Partition::new(PartitionId::parse(format!("p{index}")).expect("valid id")))
            .collect())
    }

    async fn read(
        &self,
        source: &ScriptSource,
        partition: &Partition,
        cursor: Offset,
        out: &mut Emitter<Offset>,
    ) -> Result<()> {
        let script = &source.script;
        script.reads.fetch_add(1, Ordering::SeqCst);
        let reading = script.reading.fetch_add(1, Ordering::SeqCst) + 1;
        script.peak_reading.fetch_max(reading, Ordering::SeqCst);
        let result = self.read_rows(script, partition, cursor, out).await;
        script.reading.fetch_sub(1, Ordering::SeqCst);
        result
    }

    async fn committed(
        &self,
        source: &ScriptSource,
        cursors: &[(PartitionId, Offset)],
    ) -> Result<()> {
        if source.script.fail_ack {
            return Err(ConnectorError::data("acknowledging failed"));
        }
        let mut acks = source.script.acks.lock();
        for (partition, offset) in cursors {
            acks.push((self.name.clone(), partition.to_string(), offset.next));
        }
        Ok(())
    }
}

impl Scripted {
    async fn read_rows(
        &self,
        script: &Script,
        partition: &Partition,
        cursor: Offset,
        out: &mut Emitter<Offset>,
    ) -> Result<()> {
        let stream = &script.streams[self.index];
        let index: usize = partition.id().as_str()[1..]
            .parse()
            .expect("partition ids are p<n>");
        if stream.hang == Some(index) {
            std::future::pending::<()>().await;
        }
        let mut next = cursor.next;
        let mut batches = 0;
        loop {
            let rows = stream.rows[index].load(Ordering::SeqCst);
            if next >= rows {
                if stream.idle && stream.idle_flag.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    self.checkpoint_if_asked(script, stream, next, out).await?;
                    out.metric("idle", 0.0).await?;
                    continue;
                }
                break;
            }
            let batch = script.batches.fetch_add(1, Ordering::SeqCst) + 1;
            let fault = {
                let mut faults = script.faults.lock();
                let position = faults.iter().position(|fault| fault.batch == batch);
                position.map(|position| faults.remove(position))
            };
            if let Some(fault) = fault {
                let error = match fault.retry_after {
                    Some(after) => ConnectorError::rate_limited("scripted rate limit", Some(after)),
                    None => ConnectorError::new(fault.kind, "scripted failure"),
                };
                return Err(error);
            }
            let end = (next + stream.batch_rows).min(rows);
            push(stream.push, index, next..end, out).await?;
            next = end;
            batches += 1;
            match self.checkpointing {
                Checkpointing::Natural if batches % stream.checkpoint_every == 0 => {
                    out.checkpoint(&Offset { next }).await?;
                }
                Checkpointing::Natural => {}
                Checkpointing::OnDemand => {
                    self.checkpoint_if_asked(script, stream, next, out).await?;
                }
            }
        }
        if stream.final_checkpoint {
            out.checkpoint(&Offset { next }).await?;
        }
        Ok(())
    }

    async fn checkpoint_if_asked(
        &self,
        script: &Script,
        stream: &ScriptStream,
        next: u64,
        out: &mut Emitter<Offset>,
    ) -> Result<()> {
        if out.checkpoint_due() {
            script.asked.store(true, Ordering::SeqCst);
            if stream.checkpointing == Checkpointing::OnDemand {
                out.checkpoint(&Offset { next }).await?;
            }
        }
        Ok(())
    }
}

/// The id of row `offset` of `partition`.
pub(crate) fn id(partition: usize, offset: u64) -> i64 {
    i64::try_from(partition * 1_000_000).unwrap_or(i64::MAX)
        + i64::try_from(offset).unwrap_or(i64::MAX)
}

async fn push(
    kind: PushKind,
    partition: usize,
    offsets: std::ops::Range<u64>,
    out: &mut Emitter<Offset>,
) -> Result<()> {
    let ids: Vec<i64> = offsets.map(|offset| id(partition, offset)).collect();
    match kind {
        PushKind::Arrow => {
            let batch = RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(ids)) as _)]);
            out.batch(batch.expect("one column makes a batch")).await
        }
        PushKind::Json => {
            let rows: Vec<_> = ids.iter().map(|id| json!({ "id": id })).collect();
            out.rows(&rows).await
        }
        PushKind::WrongType => {
            let text = StringArray::from_iter_values(ids.iter().map(ToString::to_string));
            let batch = RecordBatch::try_from_iter([("id", Arc::new(text) as _)]);
            out.batch(batch.expect("one column makes a batch")).await
        }
        PushKind::Nulls => {
            let nulls = Int64Array::from(vec![None::<i64>; ids.len()]);
            let batch = RecordBatch::try_from_iter([("id", Arc::new(nulls) as _)]);
            out.batch(batch.expect("one column makes a batch")).await
        }
    }
}
