//! A source that costs as little as a real one can: it replays pushes it holds, registered by
//! name in this process.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use bytes::Bytes;
use parking_lot::Mutex;
use rdlt_connector::{
    ConnectContext, ConnectorError, Emitter, Partition, PartitionId, Push, ReadMode, ReadRequest,
    ReadStream, Result, ResultExt as _, Source, SourceConnector, SourceEvent, SourceFactory,
    StreamName, StreamSpec, StreamState, Streams, partition_channel, source_factory,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::{Deserializer, Value, json};

use crate::env::{Env, InlineEnv};

static REPLAYS: LazyLock<Mutex<BTreeMap<String, Replayed>>> = LazyLock::new(Mutex::default);

/// The factory this process's replays connect through, made once: making one writes out its
/// configuration's schema, work no counted run should repeat.
static FACTORY: LazyLock<Box<dyn SourceFactory>> = LazyLock::new(replay_factory);

/// What a replay pushes, in order.
#[derive(Clone, Debug, PartialEq)]
pub enum Replayed {
    /// Arrow batches.
    Batches(Vec<RecordBatch>),
    /// JSON records, newline-delimited, a push each.
    Json(Vec<Bytes>),
    /// JSON records one after another, each push's records pushed as one call of
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

/// The pushes a source makes calling [`Emitter::rows`] once with the records of each of
/// `records`, a push of JSON objects one after another each.
///
/// # Panics
///
/// Panics where a push of `records` is not JSON values, which no corpus's is.
pub fn rows_pushed(records: Vec<Bytes>) -> Vec<Bytes> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime starts");
    runtime.block_on(async {
        register("rows", Replayed::Rows(records));
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
        read.expect("each push is JSON values");
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

/// `config`, the configuration of a replay source, following each partition as one that never
/// ends, read incrementally: its share pushed `pace` apart on the bench's clock, then nothing until
/// the read is stopped.
pub(super) fn following(mut config: Value, pace: Duration) -> Value {
    config["pace"] = json!(pace);
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
    let source = FACTORY
        .connect(config, ConnectContext::new())
        .await
        .expect("the replay is registered");
    Arc::from(source)
}

/// Names the registered replay a [`Replay`] pushes, the partitions it pushes from, the streams
/// it pushes, one of each where it names none, how many of them, the last, it cannot read again,
/// none where it names none, the pushes of a partition between its checkpoints, one where it
/// names none, and the wait between a partition's pushes where it follows its partitions.
#[derive(Debug, Deserialize, JsonSchema)]
pub(super) struct ReplayConfig {
    name: String,
    partitions: Option<NonZeroUsize>,
    streams: Option<NonZeroUsize>,
    unreplayable: Option<usize>,
    every: Option<NonZeroUsize>,
    pace: Option<Duration>,
}

/// The source a replay's configuration connects.
pub(super) struct Replay {
    replayed: Replayed,
    partitions: NonZeroUsize,
    streams: NonZeroUsize,
    unreplayable: usize,
    every: NonZeroUsize,
    pace: Option<Duration>,
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
            pace: config.pace,
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
                followed: self.pace.is_some(),
            })
        })
    }
}

/// The stream numbered `index` of a replay, named by [`stream_name`]: read in full where its
/// source can read it again and does not follow it, incrementally otherwise.
struct Events {
    index: usize,
    replayable: bool,
    followed: bool,
}

impl ReadStream<Replay> for Events {
    type Cursor = usize;

    fn spec(&self) -> StreamSpec {
        let spec = StreamSpec::new(stream_name(self.index));
        match (self.replayable, self.followed) {
            (true, false) => spec.with_read_modes([ReadMode::Full]),
            (true, true) => spec.with_read_modes([ReadMode::Incremental]),
            (false, _) => spec
                .with_read_modes([ReadMode::Incremental])
                .with_replayable(false),
        }
    }

    async fn partitions(&self, source: &Replay, _state: &StreamState) -> Result<Vec<Partition>> {
        let id = |index: usize| PartitionId::parse(index.to_string()).expect("a valid partition");
        let partitions = if source.partitions == NonZeroUsize::MIN {
            vec![Partition::single()]
        } else {
            (0..source.partitions.get())
                .map(|index| Partition::new(id(index)))
                .collect()
        };
        if self.followed {
            return Ok(partitions.into_iter().map(Partition::unbounded).collect());
        }
        Ok(partitions)
    }

    /// Pushes the partition's share from `next`, the index of the push after its last
    /// checkpoint, with a checkpoint after every `every`-th push of the share and after its last;
    /// where the replay follows, its pushes `pace` apart, then, where the engine asks the read to
    /// follow, nothing until it is stopped.
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
        let mut due = InlineEnv.instant();
        for index in next.max(share.start)..share.end {
            if let Some(pace) = source.pace {
                if !paced(out, due).await {
                    return Ok(());
                }
                due += pace;
            }
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
        if source.pace.is_some() && out.follows() {
            out.stopped().await;
        }
        Ok(())
    }
}

/// Waits until `due` on the bench's clock: whether the read goes on, rather than being stopped.
async fn paced(out: &Emitter<usize>, due: Instant) -> bool {
    let wait = due.saturating_duration_since(InlineEnv.instant());
    tokio::select! {
        biased;
        // A stop wins: the run is ending.
        () = out.stopped() => false,
        () = InlineEnv.sleep(wait) => true,
    }
}

/// The records of `push`, JSON values one after another, each as its text.
fn records(push: &[u8]) -> Result<Vec<&RawValue>> {
    let text = std::str::from_utf8(push).data("reading a replay's rows")?;
    let records = Deserializer::from_str(text).into_iter();
    records
        .collect::<std::result::Result<_, _>>()
        .data("reading a replay's rows")
}
