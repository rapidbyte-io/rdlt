//! A source of offset logs, as a message queue keeps them: each stream a set of partitions, each
//! partition a log of seeded messages that grows as time passes.

mod start;
#[cfg(test)]
pub(crate) mod tests;

use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::prelude::*;
use rdlt_connector::{Field, Partitioning};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::generator::mix;
use crate::kept::{Kept, Registry};
use crate::limits::{MAX_MESSAGE_ROWS, MAX_PARTITIONS, MAX_PER_SECOND, within};
use crate::positions::{keeper_name, keeper_path, unnamed};

/// Configuration of [`LogSource`].
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LogConfig {
    /// Every message derives from this seed.
    pub seed: u64,
    /// The streams.
    pub streams: Vec<LoggedStream>,
    /// The consumer group the source keeps its committed offsets in, by name, which every source
    /// of this process naming it shares, as a broker keeps a group's offsets; the default group
    /// where none is named.
    #[serde(default)]
    pub group: Option<String>,
    /// The file the group's offsets are kept in instead, which outlives the process as a
    /// broker keeps them; `group` names none then.
    ///
    /// A path to a file named `*.group`.
    #[serde(default)]
    pub group_path: Option<std::path::PathBuf>,
}

/// One stream: `partitions` offset logs of `messages` messages each, growing by `per_second`
/// messages a second.
#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LoggedStream {
    /// The stream's name.
    pub name: String,
    /// Partitions `p0..p{partitions - 1}`; with the ones the stream gains, at most 1024.
    pub partitions: u32,
    /// Messages each partition holds when the process first reads any log.
    pub messages: u64,
    /// Messages each partition gains a second after that, at most a thousand million.
    #[serde(default)]
    pub per_second: u64,
    /// Partitions the stream gains `later_after_ms` milliseconds after that, as a topic whose
    /// partitions are increased.
    #[serde(default)]
    pub partitions_later: u32,
    /// When the later partitions appear.
    #[serde(default)]
    pub later_after_ms: u64,
    /// Partitions, the last ones, the stream loses `retired_after_ms` milliseconds after the logs
    /// began, as a rebalance takes them from this reader.
    #[serde(default)]
    pub partitions_retired: u32,
    /// When the retired partitions go.
    #[serde(default)]
    pub retired_after_ms: u64,
    /// Whether each partition's read ends at its head, as a table read incrementally does, so a
    /// following run reads it again as it grows.
    #[serde(default)]
    pub bounded: bool,
    /// How many of its newest messages each partition keeps, every one where none is set.
    ///
    /// A read that would resume from a message it dropped fails with `retention_lost`.
    #[serde(default)]
    pub retention: Option<u64>,
    /// Whether the log serves again what its group committed; a queue that forgets it refuses
    /// to read from before its committed offset, and its source names its `group` or
    /// `group_path`.
    #[serde(default = "replayable")]
    pub replayable: bool,
    /// Messages per pushed batch, at most 100000.
    #[serde(default = "ten")]
    pub batch_rows: u64,
    /// Batches a checkpoint: one follows every so many batches, each batch where it is one.
    ///
    /// With more, a read that ends on a batch no checkpoint follows has no cursor past its last
    /// messages, as a table read to its end has none.
    #[serde(default = "one")]
    pub checkpoint_batches: u64,
}

fn replayable() -> bool {
    true
}

fn ten() -> u64 {
    10
}

fn one() -> u64 {
    1
}

/// The most partitions `stream` ever has: its first and the ones it gains.
fn most(stream: &LoggedStream) -> u64 {
    u64::from(stream.partitions) + u64::from(stream.partitions_later)
}

/// The least a following read waits for a message it does not hold yet, so that no rounding of
/// when the message arrives makes the read ask again at once.
const MIN_WAIT: Duration = Duration::from_millis(1);

/// Consumer groups by host and name, each for as long as a source holds it.
static GROUPS: Registry<u64> = Registry::new();

/// Reads offset logs of seeded messages `(partition, offset, value)`, keeping each partition's
/// committed offset in its consumer group.
///
/// Every read of an unbounded partition returns once caught up to the log's head as it stood when
/// the read started, unless asked to follow it, when it waits for the messages that follow until
/// asked to stop. The same seed always yields the same messages.
#[derive(Debug)]
pub struct LogSource {
    seed: u64,
    streams: Vec<LoggedStream>,
    group: Arc<Kept<u64>>,
    /// Whether the group is kept in a file, and so by the processes after this one.
    lasting: bool,
}

impl LogSource {
    /// How long ago the source's logs began.
    fn elapsed(&self) -> Duration {
        start::elapsed(&self.group)
    }
}

#[source(id = "io.rapidbyte.log", acknowledged)]
impl SourceConnector for LogSource {
    type Config = LogConfig;

    async fn connect(config: LogConfig, context: &ConnectContext) -> Result<Self> {
        for stream in &config.streams {
            StreamName::new(&stream.name).config(format!("stream name {:?}", stream.name))?;
            if stream.partitions == 0 || stream.batch_rows == 0 {
                return Err(ConnectorError::config(format!(
                    "stream {}: partitions and batch_rows must be at least 1",
                    stream.name
                )));
            }
            within(
                &stream.name,
                "per_second",
                stream.per_second,
                MAX_PER_SECOND,
            )?;
            within(&stream.name, "partitions", most(stream), MAX_PARTITIONS)?;
            within(
                &stream.name,
                "batch_rows",
                stream.batch_rows,
                MAX_MESSAGE_ROWS,
            )?;
        }
        keeper_name(config.group.as_deref(), "group")?;
        if let Some(path) = &config.group_path {
            keeper_path(path, "group")?;
        }
        let shared = config.group.is_none() && config.group_path.is_none();
        if let Some(forgets) = config.streams.iter().find(|stream| !stream.replayable)
            && shared
        {
            return Err(unnamed(&forgets.name, "group"));
        }
        let group = match &config.group_path {
            Some(path) => GROUPS
                .at(context.host(), path)
                .config(format!("group {}", path.display()))?,
            None => GROUPS.named(context.host(), config.group.as_deref()),
        };
        // The logs of groups kept nowhere begin with the first source connected.
        start::elapsed(&group);
        Ok(Self {
            seed: config.seed,
            streams: config.streams,
            lasting: config.group_path.is_some(),
            group,
        })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        self.streams.iter().fold(Streams::new(), |streams, stream| {
            streams.with(Logged(stream.clone()))
        })
    }
}

/// Where a partition's read resumes: the next offset.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Offset {
    /// The offset of the next message.
    pub next: u64,
}

struct Logged(LoggedStream);

impl Logged {
    /// How many partitions the stream has `elapsed` after the logs began.
    fn partitions(&self, elapsed: Duration) -> u32 {
        let since = |ms| elapsed >= Duration::from_millis(ms);
        let later = if since(self.0.later_after_ms) {
            self.0.partitions_later
        } else {
            0
        };
        let retired = if since(self.0.retired_after_ms) {
            self.0.partitions_retired
        } else {
            0
        };
        self.0
            .partitions
            .saturating_add(later)
            .saturating_sub(retired)
            .max(1)
    }

    /// Checks that `partition` is one the stream ever has: `p` and an index below its first
    /// partitions and the ones it gains, written the way the stream plans it.
    fn member(&self, partition: &PartitionId) -> Result<()> {
        let index = partition
            .as_str()
            .strip_prefix('p')
            .and_then(|index| index.parse::<u32>().ok())
            .filter(|index| u64::from(*index) < most(&self.0))
            .filter(|index| partition.as_str() == format!("p{index}"));
        match index {
            Some(_) => Ok(()),
            None => Err(ConnectorError::data(format!(
                "stream {} has no partition {partition}",
                self.0.name
            ))),
        }
    }

    /// The offset past the last message each partition holds `elapsed` after the logs began.
    fn head(&self, elapsed: Duration) -> u64 {
        let grown = u128::from(self.0.per_second) * elapsed.as_millis() / 1000;
        self.0
            .messages
            .saturating_add(u64::try_from(grown).unwrap_or(u64::MAX))
    }

    /// The earliest offset a partition whose head is `head` still holds: all of them, or the
    /// last `retention`.
    fn earliest(&self, head: u64) -> u64 {
        self.0
            .retention
            .map_or(0, |retention| head.saturating_sub(retention))
    }

    /// How long after `elapsed` the stream's partitions may next change; none once they never
    /// will.
    fn changes(&self, elapsed: Duration) -> Option<Duration> {
        [self.0.later_after_ms, self.0.retired_after_ms]
            .into_iter()
            .map(Duration::from_millis)
            .filter_map(|at| at.checked_sub(elapsed).filter(|left| !left.is_zero()))
            .min()
    }

    /// How long after `elapsed` the message at `offset` arrives, at least [`MIN_WAIT`]; none
    /// where it never does: the log never grows, or the offset is the last a number holds,
    /// which no head passes.
    fn arrives(&self, offset: u64, elapsed: Duration) -> Option<Duration> {
        if self.0.per_second == 0 || offset == u64::MAX {
            return None;
        }
        // In numbers twice as wide, no product of an offset and a thousand is cut short.
        let grown = u128::from(offset.saturating_sub(self.0.messages)) + 1;
        let at = (grown * 1000).div_ceil(u128::from(self.0.per_second));
        let at = Duration::from_millis(u64::try_from(at).unwrap_or(u64::MAX));
        Some(at.saturating_sub(elapsed).max(MIN_WAIT))
    }

    /// The messages at `offsets` of partition `partition`.
    fn messages(
        &self,
        seed: u64,
        partition: &PartitionId,
        offsets: std::ops::Range<u64>,
    ) -> Vec<serde_json::Value> {
        offsets
            .map(|offset| {
                json!({
                    "partition": partition.as_str(),
                    "offset": offset,
                    "value": message(seed, &self.0.name, partition, offset),
                })
            })
            .collect()
    }
}

/// The value of the message at `offset` of `partition` of `stream`, for `seed`.
pub fn message(seed: u64, stream: &str, partition: &PartitionId, offset: u64) -> String {
    // FNV-1a names each stream and partition apart.
    let named = format!("{stream}/{partition}")
        .bytes()
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
        });
    format!("m{:016x}", mix(seed ^ mix(named ^ mix(offset))))
}

impl ReadStream<LogSource> for Logged {
    type Cursor = Offset;

    fn spec(&self) -> StreamSpec {
        let schema = TableSchema::new(vec![
            Field::new("partition", LogicalType::Utf8, false),
            Field::new("offset", LogicalType::Int64, false),
            Field::new("value", LogicalType::Utf8, true),
        ])
        .expect("the log schema has distinct field names");
        StreamSpec::new(StreamName::new(&self.0.name).expect("connect validated stream names"))
            .with_schema(schema)
            .with_primary_key(["partition", "offset"])
            .with_read_modes([ReadMode::Incremental])
            .with_partitioning(Partitioning::Planned)
            .with_checkpointing(Checkpointing::Natural)
            .with_replayable(self.0.replayable)
    }

    async fn partitions(&self, source: &LogSource, _state: &StreamState) -> Result<Vec<Partition>> {
        (0..self.partitions(source.elapsed()))
            .map(|index| {
                let id = PartitionId::parse(format!("p{index}")).internal("partition id")?;
                Ok(if self.0.bounded {
                    Partition::new(id)
                } else {
                    Partition::new(id).unbounded()
                })
            })
            .collect()
    }

    async fn read(
        &self,
        source: &LogSource,
        partition: &Partition,
        cursor: Offset,
        out: &mut Emitter<Offset>,
    ) -> Result<()> {
        let id = partition.id();
        // A read that does not follow returns at the head as it stood when the read started.
        let head_at_start = self.head(source.elapsed());
        let mut next = self.accept(source, id, cursor, head_at_start)?;
        // A start the log has yet to reach is not one its host may report: the read sends
        // nothing before the head is there, and does not end cleanly before it.
        let reached = |head: u64| {
            if head < cursor.next {
                return Err(start::ahead(id, cursor.next, head));
            }
            Ok(())
        };
        let mut partitions = self.partitions(source.elapsed());
        let mut batches = 0_u64;
        loop {
            let head = if out.follows() {
                self.head(source.elapsed())
            } else {
                head_at_start
            };
            while next < head {
                let end = head.min(next.saturating_add(self.0.batch_rows));
                out.rows(&self.messages(source.seed, id, next..end)).await?;
                next = end;
                batches += 1;
                if batches.is_multiple_of(self.0.checkpoint_batches.max(1)) {
                    out.checkpoint(&Offset { next }).await?;
                }
                out.behind(self.head(source.elapsed()).saturating_sub(next))
                    .await?;
            }
            if self.0.bounded || !out.follows() {
                return reached(head);
            }
            // The first partition says when the stream's partitions change, as a consumer that
            // sees a topic's partitions increased does.
            let now = self.partitions(source.elapsed());
            if id.as_str() == "p0" && now != partitions {
                partitions = now;
                out.replan().await?;
            }
            let wake = [
                self.arrives(next, source.elapsed()),
                self.changes(source.elapsed()),
            ]
            .into_iter()
            .flatten()
            .min();
            match wake {
                None => {
                    out.stopped().await;
                    return reached(head);
                }
                Some(wait) => tokio::select! {
                    biased;
                    () = out.stopped() => return reached(self.head(source.elapsed())),
                    () = tokio::time::sleep(wait) => {}
                },
            }
        }
    }

    /// Commits each partition's offset in the source's consumer group.
    ///
    /// Nothing is committed where a partition is none the stream ever has.
    async fn committed(&self, source: &LogSource, cursors: &[(PartitionId, Offset)]) -> Result<()> {
        for (partition, _) in cursors {
            self.member(partition)?;
        }
        for (partition, offset) in cursors {
            let kept = source.group.advance(&self.0.name, partition, offset.next);
            kept.transient("keeping the group's offsets")?;
        }
        Ok(())
    }

    async fn acknowledged(
        &self,
        source: &LogSource,
        partition: &PartitionId,
    ) -> Result<Option<Offset>> {
        Ok(source
            .group
            .position(&self.0.name, partition)
            .map(|next| Offset { next }))
    }
}
