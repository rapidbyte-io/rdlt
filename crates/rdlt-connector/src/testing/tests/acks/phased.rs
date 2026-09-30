//! `S-ACK` against a change stream read in two phases, as a database's is: a snapshot that keeps
//! no position, then changes that never end and keep where they were acknowledged.

use std::sync::Mutex;

use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use super::super::super::{Outcome, certify_source};
use super::outcome;
use crate::catalog::{Checkpointing, ReadMode, StreamSpec};
use crate::cursor::Cursor;
use crate::emitter::Emitter;
use crate::error::Result;
use crate::id::{PartitionId, StreamName};
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
struct LogConfig {
    /// Moves the slot as the changes are read, before any commit.
    ack_on_read: bool,
    /// Never moves the slot.
    stuck: bool,
    /// Waits for changes that never come after the first it reads.
    idle: bool,
}

/// A table's snapshot, then its changes, with a slot that keeps where the changes were
/// acknowledged.
struct Log {
    config: LogConfig,
    slot: Mutex<Option<u64>>,
}

impl SourceConnector for Log {
    const ID: &'static str = "io.test.log";
    const VERSION: &'static str = "0.0.1";
    const ACKNOWLEDGES: bool = true;
    type Config = LogConfig;

    async fn connect(config: LogConfig, _context: &ConnectContext) -> Result<Self> {
        Ok(Self {
            config,
            slot: Mutex::new(None),
        })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new().with(Rows)
    }
}

struct Rows;

fn snapshot() -> PartitionId {
    PartitionId::parse("snapshot").expect("a valid partition")
}

fn changes() -> PartitionId {
    PartitionId::parse("changes").expect("a valid partition")
}

impl ReadStream<Log> for Rows {
    type Cursor = u64;

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(StreamName::new("rows").expect("a valid stream"))
            .with_read_modes([ReadMode::Cdc])
            .with_checkpointing(Checkpointing::Natural)
    }

    async fn plan(&self, _source: &Log, state: &StreamState) -> Result<PartitionPlan> {
        let read = matches!(
            state.partitions.get(&snapshot()),
            Some(PartitionState::Cursor(cursor))
                if cursor.decode::<u64>(1).is_ok_and(|read| read == SNAPSHOT_ROWS)
        );
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
        if partition.id() == &snapshot() {
            for row in cursor..SNAPSHOT_ROWS {
                out.rows(&[json!({ "row": row })]).await?;
                out.checkpoint(&(row + 1)).await?;
            }
            return Ok(());
        }
        // The changes never end: one follows another for as long as the read runs.
        let mut next = cursor;
        loop {
            out.rows(&[json!({ "row": next })]).await?;
            next += 1;
            if source.config.ack_on_read {
                *source.slot.lock().expect("unpoisoned") = Some(next);
            }
            out.checkpoint(&next).await?;
            if source.config.idle {
                std::future::pending::<()>().await;
            }
        }
    }

    async fn committed(&self, source: &Log, cursors: &[(PartitionId, u64)]) -> Result<()> {
        for (partition, cursor) in cursors {
            if partition == &changes() && !source.config.stuck {
                *source.slot.lock().expect("unpoisoned") = Some(*cursor);
            }
        }
        Ok(())
    }

    async fn acknowledged(&self, source: &Log, partition: &PartitionId) -> Result<Option<u64>> {
        Ok((partition == &changes())
            .then(|| *source.slot.lock().expect("unpoisoned"))
            .flatten())
    }
}

#[tokio::test]
async fn a_change_log_whose_slot_moves_only_when_committed_passes_s_ack() {
    let report = certify_source::<Log>(json!({})).await;
    assert_eq!(outcome(&report), &Outcome::Passed);
}

#[tokio::test]
async fn a_change_log_whose_slot_moves_as_it_is_read_or_not_at_all_fails_s_ack() {
    for flag in ["ack_on_read", "stuck"] {
        let report = certify_source::<Log>(json!({ flag: true })).await;
        assert!(
            matches!(outcome(&report), Outcome::Failed(_)),
            "{flag}: {:?}",
            outcome(&report)
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_change_log_waiting_for_changes_that_never_come_passes_s_ack() {
    let report = certify_source::<Log>(json!({ "idle": true })).await;
    assert_eq!(outcome(&report), &Outcome::Passed);
}
