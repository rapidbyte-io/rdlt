//! `S-ACK` against a queue that tells where it stands, correct unless a flag breaks it.

mod phased;

use std::sync::Mutex;

use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use super::super::{Outcome, Report, certify_source};
use crate::catalog::{Checkpointing, ReadMode, StreamSpec};
use crate::emitter::Emitter;
use crate::error::{ConnectorError, ConnectorErrorKind, Result};
use crate::id::{PartitionId, StreamName};
use crate::source::{Partition, ReadStream, SourceConnector, Streams};
use crate::spec::ConnectContext;
use crate::state::StreamState;

#[derive(Default, Deserialize, JsonSchema)]
#[serde(default)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "each flag breaks or shapes one behavior"
)]
struct QueueConfig {
    /// Moves the position as the queue is read, before any commit.
    ack_on_read: bool,
    /// Never moves the position.
    stuck: bool,
    /// Where an earlier load left the position.
    at: Option<u32>,
    /// Forgets what it acknowledged: a read from before the position fails, as the stream says.
    forgets: bool,
    /// Never says where it stands.
    silent: bool,
}

/// A queue of four messages, a checkpoint after each, that keeps where it was acknowledged.
struct Queue {
    config: QueueConfig,
    position: Mutex<Option<u32>>,
}

impl SourceConnector for Queue {
    const ID: &'static str = "io.test.queue";
    const VERSION: &'static str = "0.0.1";
    const ACKNOWLEDGES: bool = true;
    type Config = QueueConfig;

    async fn connect(config: QueueConfig, _context: &ConnectContext) -> Result<Self> {
        let at = config.at;
        Ok(Self {
            config,
            position: Mutex::new(at),
        })
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new().with(Messages {
            replayable: !self.config.forgets,
        })
    }
}

struct Messages {
    replayable: bool,
}

impl ReadStream<Queue> for Messages {
    type Cursor = u32;

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(StreamName::new("messages").expect("a valid stream"))
            .with_read_modes([ReadMode::Cdc])
            .with_checkpointing(Checkpointing::Natural)
            .with_replayable(self.replayable)
    }

    async fn partitions(&self, _source: &Queue, _state: &StreamState) -> Result<Vec<Partition>> {
        Ok(vec![Partition::single()])
    }

    async fn read(
        &self,
        source: &Queue,
        _partition: &Partition,
        cursor: u32,
        out: &mut Emitter<u32>,
    ) -> Result<()> {
        let position = *source.position.lock().expect("unpoisoned");
        if source.config.forgets && position.is_some_and(|position| cursor < position) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Transient,
                "the queue no longer holds what it acknowledged",
            ));
        }
        for message in cursor..4 {
            out.rows(&[json!({ "message": message })]).await?;
            // As a consumer committing on its own does: the rows it hands out are acknowledged.
            if source.config.ack_on_read {
                *source.position.lock().expect("unpoisoned") = Some(message + 1);
            }
            out.checkpoint(&(message + 1)).await?;
        }
        Ok(())
    }

    async fn committed(&self, source: &Queue, cursors: &[(PartitionId, u32)]) -> Result<()> {
        if !source.config.stuck {
            for (_, cursor) in cursors {
                *source.position.lock().expect("unpoisoned") = Some(*cursor);
            }
        }
        Ok(())
    }

    async fn acknowledged(&self, source: &Queue, _partition: &PartitionId) -> Result<Option<u32>> {
        if source.config.silent {
            std::future::pending::<()>().await;
        }
        Ok(*source.position.lock().expect("unpoisoned"))
    }
}

/// Well within the bound of a read, which a clause that fails must not wait out.
const PROMPTLY: std::time::Duration = std::time::Duration::from_secs(10);

pub(super) fn outcome(report: &Report) -> &Outcome {
    report.outcome("S-ACK").expect("S-ACK is certified")
}

#[tokio::test]
async fn a_queue_that_moves_only_when_committed_passes_s_ack() {
    let report = certify_source::<Queue>(json!({})).await;
    assert_eq!(outcome(&report), &Outcome::Passed);
}

#[tokio::test]
async fn a_queue_that_moves_as_it_is_read_or_not_at_all_fails_s_ack() {
    let flawed = [
        json!({ "ack_on_read": true }),
        json!({ "ack_on_read": true, "forgets": true }),
        json!({ "stuck": true }),
    ];
    for config in flawed {
        // A violation stops the read rather than waiting out the clause's bound.
        let certified = certify_source::<Queue>(config.clone());
        let report = tokio::time::timeout(PROMPTLY, certified)
            .await
            .expect("the clause ends promptly");
        assert!(
            matches!(outcome(&report), Outcome::Failed(_)),
            "{config}: {:?}",
            outcome(&report)
        );
    }
}

#[tokio::test]
async fn a_queue_that_forgets_what_it_acknowledged_passes_every_clause() {
    // The other clauses read it from the start, which it forgets once S-ACK commits.
    let report = certify_source::<Queue>(json!({ "forgets": true })).await;
    report.assert_passed();
    assert_eq!(outcome(&report), &Outcome::Passed);
}

#[tokio::test]
async fn a_queue_with_nothing_ahead_of_where_it_stands_skips_s_ack() {
    for forgets in [false, true] {
        let report = certify_source::<Queue>(json!({ "at": 4, "forgets": forgets })).await;
        assert!(
            matches!(outcome(&report), Outcome::Skipped(_)),
            "forgets {forgets}: {:?}",
            outcome(&report)
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_queue_that_never_says_where_it_stands_fails_s_ack_alone() {
    let report = certify_source::<Queue>(json!({ "silent": true })).await;
    assert!(matches!(outcome(&report), Outcome::Failed(_)), "{report}");
    assert_eq!(report.failures().count(), 1, "{report}");
}
