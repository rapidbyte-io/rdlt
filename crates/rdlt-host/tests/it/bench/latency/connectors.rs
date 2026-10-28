//! The cell's connectors: a following source that stamps each push with the instant it pushes it,
//! and a destination that keeps its pipeline's state and reads the clock as each commit lands.
//!
//! Each appends a line a push to a file of its run's: the stream, the push's stamp and the
//! instant it read, in nanoseconds of the host's monotonic clock, which every process on the host
//! reads alike.

use std::collections::BTreeMap;
use std::collections::hash_map::RandomState;
use std::fmt::Write as _;
use std::hash::BuildHasher as _;
use std::io::Write as _;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use arrow_array::{Array as _, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parking_lot::Mutex;
use rdlt_connector::{
    Capabilities, CommitMeta, ConnectContext, ConnectorError, ConnectorErrorKind,
    DestinationConnector, DestinationFactory, Emitter, Epoch, OpenContext, Opened, Partition,
    PartitionId, ReadMode, ReadStream, Receipt, Result, SegmentId, Session, SourceConnector,
    SourceFactory, StateChange, StateRecord, StreamName, StreamSpec, StreamState, Streams,
    TableChange, TableRef, TableWriter, WriteStats, destination_factory, source_factory,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

/// Rows a push holds.
pub(crate) const ROWS: usize = 1_000;

/// The source's streams: the first it can read again, the second it cannot, so the engine logs
/// what it reads of it.
pub(crate) const STREAMS: [&str; 2] = ["replayable", "unreplayable"];

/// The column holding each row's push instant.
const PUSHED: &str = "pushed";

/// Nanoseconds on the host's monotonic clock.
fn now() -> i64 {
    let time = nix::time::clock_gettime(nix::time::ClockId::CLOCK_MONOTONIC)
        .expect("the monotonic clock reads");
    time.tv_sec() * 1_000_000_000 + time.tv_nsec()
}

/// Whether to fail this call, failing one in `one_in`: drawn afresh each call, since each run's
/// connectors are new and make few calls.
fn drawn(one_in: Option<NonZeroU64>) -> bool {
    one_in.is_some_and(|one_in| RandomState::new().hash_one(()).is_multiple_of(one_in.get()))
}

/// A failure the cell asks for, which may succeed on retry.
fn transient(message: &str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Transient, message)
}

/// Appends `lines` to the file at `path`, off the runtime's workers.
async fn append(path: &Path, lines: String) -> Result<()> {
    let path = path.to_owned();
    let appended = tokio::task::spawn_blocking(move || {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        file.write_all(lines.as_bytes())
    });
    let failed = || ConnectorError::internal("appending to the cell's record failed");
    let appended = appended
        .await
        .map_err(|error| failed().with_source(error))?;
    appended.map_err(|error| failed().with_source(error))
}

/// How a [`Stamped`] source pushes, and where it appends what it is told.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub(crate) struct StampedConfig {
    /// The wait between a stream's pushes; none pushes as fast as the engine takes them.
    pub(crate) pace: Option<Duration>,
    /// The file each push a report covers is appended to.
    pub(crate) record: PathBuf,
    /// Refuses one report in this many.
    pub(crate) refuse: Option<NonZeroU64>,
}

/// The factory of stamped sources, to serve.
pub(crate) fn stamped_factory() -> Box<dyn SourceFactory> {
    source_factory::<Stamped>()
}

/// A following source of [`STREAMS`], one unbounded partition each, pushing rows of one column,
/// the instant of their push, a checkpoint after each push.
struct Stamped {
    config: StampedConfig,
    schema: SchemaRef,
    pushed: [Mutex<Pushed>; 2],
}

/// A stream's pushes no report has covered yet, by position, and the last position it was told.
#[derive(Default)]
struct Pushed {
    stamps: BTreeMap<u64, i64>,
    told: u64,
}

impl Stamped {
    /// A push of [`ROWS`] rows stamped `stamp`.
    fn push(&self, stamp: i64) -> RecordBatch {
        let stamps = Int64Array::from_value(stamp, ROWS);
        RecordBatch::try_new(Arc::clone(&self.schema), vec![Arc::new(stamps)])
            .expect("a column of stamps fits the schema of one")
    }
}

impl SourceConnector for Stamped {
    const ID: &'static str = "io.rapidbyte.bench.stamped";
    const VERSION: &'static str = "0.0.0";
    type Config = StampedConfig;

    async fn connect(config: StampedConfig, _context: &ConnectContext) -> Result<Self> {
        let pushed = Field::new(PUSHED, DataType::Int64, false);
        Ok(Self {
            config,
            schema: Arc::new(Schema::new(vec![pushed])),
            pushed: Default::default(),
        })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new().with(Stream(0)).with(Stream(1))
    }
}

/// The stream [`STREAMS`] names at this index.
struct Stream(usize);

impl ReadStream<Stamped> for Stream {
    /// The position of the last push.
    type Cursor = u64;

    fn spec(&self) -> StreamSpec {
        let name = StreamName::new(STREAMS[self.0]).expect("the streams' names are valid");
        let spec = StreamSpec::new(name).with_read_modes([ReadMode::Incremental]);
        spec.with_replayable(self.0 == 0)
    }

    async fn partitions(&self, _source: &Stamped, _state: &StreamState) -> Result<Vec<Partition>> {
        Ok(vec![Partition::single().unbounded()])
    }

    /// Pushes again, with their first stamps, the pushes after `cursor` no report has covered,
    /// then new ones until the read is stopped.
    async fn read(
        &self,
        source: &Stamped,
        _partition: &Partition,
        cursor: u64,
        out: &mut Emitter<u64>,
    ) -> Result<()> {
        let again: Vec<(u64, i64)> = {
            let pushed = source.pushed[self.0].lock();
            if cursor < pushed.told {
                return Err(transient(
                    "the source cannot read from before what it was told",
                ));
            }
            let again = pushed.stamps.range(cursor + 1..);
            again.map(|(&position, &stamp)| (position, stamp)).collect()
        };
        for (position, stamp) in again {
            out.batch(source.push(stamp)).await?;
            out.checkpoint(&position).await?;
        }
        let mut due = Instant::now();
        loop {
            if let Some(pace) = source.config.pace {
                tokio::select! {
                    biased;
                    // A stop wins: the run is ending.
                    () = out.stopped() => return Ok(()),
                    () = tokio::time::sleep_until(due) => due += pace,
                }
            }
            let stamp = now();
            let position = {
                let mut pushed = source.pushed[self.0].lock();
                let last = pushed.stamps.keys().next_back().copied();
                let position = last.unwrap_or(pushed.told).max(cursor) + 1;
                pushed.stamps.insert(position, stamp);
                position
            };
            out.batch(source.push(stamp)).await?;
            out.checkpoint(&position).await?;
        }
    }

    /// Appends each push `cursors` cover, the instant they were told; or refuses the report,
    /// where the draw says.
    async fn committed(&self, source: &Stamped, cursors: &[(PartitionId, u64)]) -> Result<()> {
        if drawn(source.config.refuse) {
            return Err(transient("the source refuses this report"));
        }
        let (told, mut lines) = (now(), String::new());
        {
            let mut pushed = source.pushed[self.0].lock();
            for &(_, position) in cursors {
                let after = pushed.stamps.split_off(&(position + 1));
                for stamp in std::mem::replace(&mut pushed.stamps, after).values() {
                    writeln!(lines, "{} {stamp} {told}", STREAMS[self.0]).expect("a line writes");
                }
                pushed.told = pushed.told.max(position);
            }
        }
        append(&source.config.record, lines).await
    }
}

/// Where a [`Landing`] destination appends what lands, and how it fails.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub(crate) struct LandingConfig {
    /// The file each push a commit lands is appended to.
    pub(crate) record: PathBuf,
    /// Answers one commit in this many `Transient`, before it lands.
    pub(crate) transient: Option<NonZeroU64>,
}

/// The factory of landing destinations, to serve.
pub(crate) fn landing_factory() -> Box<dyn DestinationFactory> {
    destination_factory::<Landing>()
}

/// A destination that keeps its pipeline's state across sessions, and of the rows it is written
/// only their pushes.
struct Landing {
    config: Arc<LandingConfig>,
    kept: Arc<Mutex<Kept>>,
}

/// The epoch of a [`Landing`]'s latest session, and the state its commits landed.
#[derive(Default)]
struct Kept {
    epoch: Epoch,
    state: BTreeMap<String, StateRecord>,
}

impl DestinationConnector for Landing {
    const ID: &'static str = "io.rapidbyte.bench.landing";
    const VERSION: &'static str = "0.0.0";
    type Config = LandingConfig;
    type Session = LandingSession;

    fn capabilities(&self) -> Capabilities {
        Capabilities::minimal()
    }

    async fn connect(config: LandingConfig, _context: &ConnectContext) -> Result<Self> {
        Ok(Self {
            config: Arc::new(config),
            kept: Arc::default(),
        })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    async fn open(&self, _context: &OpenContext) -> Result<Opened<LandingSession>> {
        let mut kept = self.kept.lock();
        kept.epoch = Epoch(kept.epoch.0 + 1);
        let session = LandingSession {
            config: Arc::clone(&self.config),
            kept: Arc::clone(&self.kept),
            epoch: kept.epoch,
            staged: Arc::default(),
        };
        let (epoch, state) = (kept.epoch, kept.state.values().cloned().collect());
        Ok(Opened {
            session,
            epoch,
            landed: None,
            state,
        })
    }
}

/// Each staged segment's pushes, in the order written.
type Staged = Arc<Mutex<BTreeMap<SegmentId, Vec<Push>>>>;

/// A push a table was written: its stream's table and its stamp.
struct Push {
    table: Arc<str>,
    stamp: i64,
}

/// A [`Landing`] session.
struct LandingSession {
    config: Arc<LandingConfig>,
    kept: Arc<Mutex<Kept>>,
    epoch: Epoch,
    staged: Staged,
}

impl Session for LandingSession {
    type Writer = LandingWriter;

    async fn apply_schema(&mut self, _change: &TableChange) -> Result<()> {
        Ok(())
    }

    async fn writer(&mut self, table: &TableRef) -> Result<LandingWriter> {
        Ok(LandingWriter {
            staged: Arc::clone(&self.staged),
            table: Arc::clone(&table.name),
        })
    }

    async fn discard_staged(&mut self) -> Result<()> {
        self.staged.lock().clear();
        Ok(())
    }

    /// Lands the commit's segments and state, then appends each push they hold, the instant
    /// they landed; or answers `Transient` before landing anything, where the draw says.
    async fn commit(&mut self, meta: &CommitMeta) -> Result<Receipt> {
        if meta.key.epoch != self.epoch || self.kept.lock().epoch != self.epoch {
            return Err(ConnectorError::fenced("a newer session holds the pipeline"));
        }
        if drawn(self.config.transient) {
            return Err(transient("the destination fails this commit"));
        }
        let pushes: Vec<Push> = {
            let mut staged = self.staged.lock();
            let landed = meta
                .segments
                .iter()
                .filter_map(|segment| staged.remove(&segment));
            landed.flatten().collect()
        };
        {
            let state = &mut self.kept.lock().state;
            for change in &meta.state_delta {
                match change {
                    StateChange::Put(record) => state.insert(record.key.clone(), record.clone()),
                    StateChange::Delete(key) => state.remove(key),
                };
            }
        }
        let (landed, mut lines) = (now(), String::new());
        for Push { table, stamp } in &pushes {
            writeln!(lines, "{table} {stamp} {landed}").expect("a line writes");
        }
        append(&self.config.record, lines).await?;
        Ok(Receipt {
            key: meta.key,
            committed_at: UNIX_EPOCH,
            rows: (pushes.len() * ROWS) as u64,
            bytes: 0,
        })
    }

    async fn close(self) -> Result<()> {
        Ok(())
    }
}

/// Notes each push its table is written, by segment.
struct LandingWriter {
    staged: Staged,
    table: Arc<str>,
}

impl TableWriter for LandingWriter {
    async fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> Result<()> {
        let stamps = batch.column_by_name(PUSHED);
        let stamps = stamps.and_then(|column| column.as_any().downcast_ref::<Int64Array>());
        let stamps = stamps.ok_or_else(|| ConnectorError::data("a batch without push instants"))?;
        let mut staged = self.staged.lock();
        let pushes = staged.entry(segment).or_default();
        for &stamp in stamps.values() {
            if pushes.last().is_none_or(|last| last.stamp != stamp) {
                let table = Arc::clone(&self.table);
                pushes.push(Push { table, stamp });
            }
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<WriteStats> {
        Ok(WriteStats::default())
    }
}
