//! `S-ACK` against a change stream read in two phases, as a database's is: a snapshot, then
//! changes that never end and keep where they were acknowledged in a slot.

use std::time::Duration;

use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use super::super::super::{Outcome, certify_source};
use super::{keep, kept, outcome};
use crate::catalog::{Checkpointing, ReadMode, StreamSpec};
use crate::cursor::Cursor;
use crate::emitter::Emitter;
use crate::error::Result;
use crate::id::{PartitionId, StreamName};
use crate::sink::LogLevel;
use crate::source::{Partition, PartitionPlan, ReadStream, SourceConnector, Streams};
use crate::spec::ConnectContext;
use crate::state::{PartitionState, StreamState};

/// The phase the changes are read in.
const CHANGES: u16 = 1;

/// How many rows the snapshot holds.
const SNAPSHOT_ROWS: u64 = 3;

/// Where the changes start, past the position the snapshot captured.
const CAPTURED: u64 = 100;

#[derive(Default, Deserialize, JsonSchema)]
#[serde(default)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "each flag breaks or shapes one behavior"
)]
struct LogConfig {
    /// The name its slot is kept under, apart from every other test's.
    name: String,
    /// Moves the slot as the changes are read, before any commit.
    ack_on_read: bool,
    /// Never moves the slot for the changes.
    stuck: bool,
    /// Waits for changes that never come after the first it reads.
    idle: bool,
    /// Waits as `idle` does, logging as it waits.
    chatty: bool,
    /// Checkpoints the changes only when asked.
    on_demand: bool,
    /// Keeps the snapshot's position in the slot too.
    snapshot_keeps: bool,
    /// Reads the snapshot without checkpoints, so only its end moves the stream on.
    uncheckpointed: bool,
}

/// A table's snapshot, then its changes, with a slot that keeps where they were acknowledged.
struct Log {
    config: LogConfig,
}

impl SourceConnector for Log {
    const ID: &'static str = "io.test.log";
    const VERSION: &'static str = "0.0.1";
    const ACKNOWLEDGES: bool = true;
    type Config = LogConfig;

    async fn connect(config: LogConfig, _context: &ConnectContext) -> Result<Self> {
        Ok(Self { config })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new().with(Rows {
            on_demand: self.config.on_demand,
        })
    }
}

struct Rows {
    on_demand: bool,
}

fn snapshot() -> PartitionId {
    PartitionId::parse("snapshot").expect("a valid partition")
}

fn changes() -> PartitionId {
    PartitionId::parse("changes").expect("a valid partition")
}

impl Log {
    /// Whether the slot keeps `partition`'s position.
    fn keeps(&self, partition: &PartitionId) -> bool {
        partition == &changes() || self.config.snapshot_keeps
    }

    /// Reads the changes from `next` for as long as the read runs.
    async fn changes(&self, mut next: u64, out: &mut Emitter<u64>) -> Result<()> {
        loop {
            out.rows(&[json!({ "row": next })]).await?;
            next += 1;
            if self.config.ack_on_read {
                keep(&self.config.name, &changes(), next);
            }
            if !self.config.on_demand || out.checkpoint_due() {
                out.checkpoint(&next).await?;
            }
            if self.config.idle {
                std::future::pending::<()>().await;
            }
            if self.config.chatty {
                loop {
                    out.log(LogLevel::Info, "waiting for changes").await?;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
}

impl ReadStream<Log> for Rows {
    type Cursor = u64;

    fn spec(&self) -> StreamSpec {
        let checkpointing = if self.on_demand {
            Checkpointing::OnDemand
        } else {
            Checkpointing::Natural
        };
        StreamSpec::new(StreamName::new("rows").expect("a valid stream"))
            .with_read_modes([ReadMode::Cdc])
            .with_checkpointing(checkpointing)
    }

    async fn plan(&self, _source: &Log, state: &StreamState) -> Result<PartitionPlan> {
        let read = match state.partitions.get(&snapshot()) {
            Some(PartitionState::Done) => true,
            Some(PartitionState::Cursor(cursor)) => cursor
                .decode::<u64>(1)
                .is_ok_and(|read| read == SNAPSHOT_ROWS),
            _ => false,
        };
        if state.phase != CHANGES && !read {
            return Ok(PartitionPlan::new(vec![Partition::new(snapshot())]));
        }
        Ok(
            PartitionPlan::new(vec![Partition::new(changes()).unbounded()])
                .phase(CHANGES)
                .start(changes(), Cursor::encode(1, &CAPTURED)?),
        )
    }

    async fn read(
        &self,
        source: &Log,
        partition: &Partition,
        cursor: u64,
        out: &mut Emitter<u64>,
    ) -> Result<()> {
        if partition.id() != &snapshot() {
            return source.changes(cursor, out).await;
        }
        for row in cursor..SNAPSHOT_ROWS {
            out.rows(&[json!({ "row": row })]).await?;
            if !source.config.uncheckpointed {
                out.checkpoint(&(row + 1)).await?;
            }
        }
        Ok(())
    }

    async fn committed(&self, source: &Log, cursors: &[(PartitionId, u64)]) -> Result<()> {
        for (partition, cursor) in cursors {
            let stuck = partition == &changes() && source.config.stuck;
            if source.keeps(partition) && !stuck {
                keep(&source.config.name, partition, *cursor);
            }
        }
        Ok(())
    }

    async fn acknowledged(&self, source: &Log, partition: &PartitionId) -> Result<Option<u64>> {
        Ok(source
            .keeps(partition)
            .then(|| kept(&source.config.name, partition))
            .flatten())
    }
}

#[tokio::test(start_paused = true)]
async fn a_change_log_whose_slot_moves_only_when_committed_passes_s_ack() {
    let shapes = [
        json!({ "name": "log" }),
        json!({ "name": "log_idle", "idle": true }),
        json!({ "name": "log_chatty", "chatty": true }),
        json!({ "name": "log_on_demand", "on_demand": true }),
        json!({ "name": "log_snapshot_keeps", "snapshot_keeps": true }),
        json!({ "name": "log_uncheckpointed", "uncheckpointed": true }),
    ];
    for config in shapes {
        let report = certify_source::<Log>(config.clone()).await;
        assert_eq!(outcome(&report), &Outcome::Passed, "{config}: {report}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_change_log_whose_slot_moves_but_where_it_was_told_fails_s_ack() {
    let flawed = [
        json!({ "name": "log_acks_on_read", "ack_on_read": true }),
        json!({ "name": "log_stuck", "stuck": true }),
        json!({ "name": "log_stuck_behind_a_kept_snapshot", "stuck": true, "snapshot_keeps": true }),
        json!({ "name": "log_stuck_behind_a_whole_snapshot", "stuck": true, "uncheckpointed": true }),
    ];
    for config in flawed {
        let report = certify_source::<Log>(config.clone()).await;
        assert!(
            matches!(outcome(&report), Outcome::Failed(_)),
            "{config}: {:?}",
            outcome(&report)
        );
    }
}
