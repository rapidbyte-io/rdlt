//! `S-ACK` against a queue that tells where it stands, correct unless a flag breaks it.

mod phased;

use std::collections::BTreeMap;
use std::sync::{LazyLock, Mutex};

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

/// Positions kept outside any connection, as a broker keeps a group's offsets: by the name a
/// configuration gives, then partition.
static KEPT: LazyLock<Mutex<BTreeMap<(String, PartitionId), u64>>> = LazyLock::new(Mutex::default);

/// Where `name` keeps `partition`.
pub(super) fn kept(name: &str, partition: &PartitionId) -> Option<u64> {
    let kept = KEPT.lock().expect("unpoisoned");
    kept.get(&(name.to_owned(), partition.clone())).copied()
}

/// Keeps `partition` of `name` at `position`.
pub(super) fn keep(name: &str, partition: &PartitionId, position: u64) {
    let mut kept = KEPT.lock().expect("unpoisoned");
    kept.insert((name.to_owned(), partition.clone()), position);
}

#[derive(Default, Deserialize, JsonSchema)]
#[serde(default)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "each flag breaks or shapes one behavior"
)]
struct QueueConfig {
    /// The name its positions are kept under, apart from every other test's.
    name: String,
    /// How many partitions it has, at least one.
    partitions: u32,
    /// Moves the position as the queue is read, before any commit.
    ack_on_read: bool,
    /// Never moves the position.
    stuck: bool,
    /// Where an earlier load left each partition.
    at: Option<u64>,
    /// Forgets what it acknowledged: a read from before the position fails, as the stream says.
    forgets: bool,
    /// Never says where it stands.
    silent: bool,
    /// Keeps what it was told in the connection, not beyond it.
    per_connection: bool,
    /// Keeps every committed cursor as the first partition's.
    misroute: bool,
    /// Keeps each committed cursor one message on from where it was told.
    off_by_one: bool,
    /// How many seconds each answer to where a partition stands takes.
    slow: u64,
    /// How many messages a partition holds, when not four.
    messages: u64,
}

/// A queue of four messages a partition, a checkpoint after each, that keeps where it was
/// acknowledged.
struct Queue {
    config: QueueConfig,
    /// What it keeps where it keeps it in the connection.
    own: Mutex<BTreeMap<PartitionId, u64>>,
}

impl Queue {
    fn ids(&self) -> Vec<PartitionId> {
        (0..self.config.partitions.max(1))
            .map(|index| PartitionId::parse(format!("p{index}")).expect("a valid partition"))
            .collect()
    }

    fn position(&self, partition: &PartitionId) -> Option<u64> {
        if self.config.per_connection {
            self.own.lock().expect("unpoisoned").get(partition).copied()
        } else {
            kept(&self.config.name, partition)
        }
    }

    fn set(&self, partition: &PartitionId, position: u64) {
        if self.config.per_connection {
            let mut own = self.own.lock().expect("unpoisoned");
            own.insert(partition.clone(), position);
        } else {
            keep(&self.config.name, partition, position);
        }
    }
}

impl SourceConnector for Queue {
    const ID: &'static str = "io.test.queue";
    const VERSION: &'static str = "0.0.1";
    const ACKNOWLEDGES: bool = true;
    type Config = QueueConfig;

    async fn connect(config: QueueConfig, _context: &ConnectContext) -> Result<Self> {
        let queue = Self {
            config,
            own: Mutex::default(),
        };
        if let Some(at) = queue.config.at {
            for partition in queue.ids() {
                if queue.position(&partition).is_none() {
                    queue.set(&partition, at);
                }
            }
        }
        Ok(queue)
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
    type Cursor = u64;

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(StreamName::new("messages").expect("a valid stream"))
            .with_read_modes([ReadMode::Cdc])
            .with_checkpointing(Checkpointing::Natural)
            .with_replayable(self.replayable)
    }

    async fn partitions(&self, source: &Queue, _state: &StreamState) -> Result<Vec<Partition>> {
        Ok(source.ids().into_iter().map(Partition::new).collect())
    }

    async fn read(
        &self,
        source: &Queue,
        partition: &Partition,
        cursor: u64,
        out: &mut Emitter<u64>,
    ) -> Result<()> {
        let position = source.position(partition.id());
        if source.config.forgets && position.is_some_and(|position| cursor < position) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Transient,
                "the queue no longer holds what it acknowledged",
            ));
        }
        let messages = match source.config.messages {
            0 => 4,
            messages => messages,
        };
        for message in cursor..messages {
            out.rows(&[json!({ "message": message })]).await?;
            // As a consumer committing on its own does: the rows it hands out are acknowledged.
            if source.config.ack_on_read {
                source.set(partition.id(), message + 1);
            }
            out.checkpoint(&(message + 1)).await?;
        }
        Ok(())
    }

    async fn committed(&self, source: &Queue, cursors: &[(PartitionId, u64)]) -> Result<()> {
        let first = source.ids().swap_remove(0);
        for (partition, cursor) in cursors {
            let partition = if source.config.misroute {
                &first
            } else {
                partition
            };
            if !source.config.stuck {
                source.set(partition, cursor + u64::from(source.config.off_by_one));
            }
        }
        Ok(())
    }

    async fn acknowledged(&self, source: &Queue, partition: &PartitionId) -> Result<Option<u64>> {
        if source.config.silent {
            std::future::pending::<()>().await;
        }
        tokio::time::sleep(std::time::Duration::from_secs(source.config.slow)).await;
        Ok(source.position(partition))
    }
}

/// Well within the bound of a read, which a clause that fails must not wait out.
const PROMPTLY: std::time::Duration = std::time::Duration::from_secs(10);

pub(super) fn outcome(report: &Report) -> &Outcome {
    report.outcome("S-ACK").expect("S-ACK is certified")
}

#[tokio::test]
async fn a_queue_that_moves_only_when_committed_passes_s_ack() {
    for (name, partitions) in [("passing", 1), ("passing_twice", 2)] {
        let report =
            certify_source::<Queue>(json!({ "name": name, "partitions": partitions })).await;
        assert_eq!(outcome(&report), &Outcome::Passed, "{report}");
    }
}

#[tokio::test]
async fn a_queue_that_moves_but_where_it_was_told_fails_s_ack() {
    let flawed = [
        json!({ "name": "acks_on_read", "ack_on_read": true }),
        json!({ "name": "forgets_on_read", "ack_on_read": true, "forgets": true }),
        json!({ "name": "stuck", "stuck": true }),
        json!({ "name": "per_connection", "per_connection": true }),
        json!({ "name": "misroutes", "misroute": true, "partitions": 2 }),
        // One message ahead: a single commit, which lands elsewhere, decides.
        json!({ "name": "off_by_one", "off_by_one": true, "at": 3 }),
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
    let report = certify_source::<Queue>(json!({ "name": "forgets", "forgets": true })).await;
    report.assert_passed();
    assert_eq!(outcome(&report), &Outcome::Passed);
}

#[tokio::test]
async fn a_queue_with_nothing_ahead_of_where_it_stands_skips_s_ack() {
    for forgets in [false, true] {
        let name = format!("at_the_end_{forgets}");
        let config = json!({ "name": name, "at": 4, "forgets": forgets });
        let report = certify_source::<Queue>(config).await;
        assert!(
            matches!(outcome(&report), Outcome::Unobserved(_)),
            "forgets {forgets}: {:?}",
            outcome(&report)
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_queue_that_never_says_where_it_stands_fails_s_ack_alone() {
    let report = certify_source::<Queue>(json!({ "name": "silent", "silent": true })).await;
    assert!(matches!(outcome(&report), Outcome::Failed(_)), "{report}");
    assert_eq!(report.failures().count(), 1, "{report}");
}

#[tokio::test(start_paused = true)]
async fn asking_where_many_slow_partitions_stand_is_bounded_and_fails_s_ack_alone() {
    // Each answer comes within its own bound; a thousand of them take eight hours.
    let config = json!({ "name": "slow", "partitions": 1000, "slow": 29 });
    let began = tokio::time::Instant::now();
    let report = certify_source::<Queue>(config).await;
    // The question before the clauses and the clause itself each take their bound, at most.
    let bound = 2 * crate::testing::limits::CLAUSE_TIMEOUT + std::time::Duration::from_mins(1);
    assert!(began.elapsed() <= bound, "{:?}", began.elapsed());
    assert!(matches!(outcome(&report), Outcome::Failed(_)), "{report}");
    assert_eq!(report.failures().count(), 1, "{report}");
}

#[tokio::test(start_paused = true)]
async fn a_queue_that_checkpoints_more_than_a_clause_holds_leaves_s_ack_unobserved() {
    use crate::testing::limits::{HELD_BYTES, HELD_EVENT_BYTES};
    // More checkpoints than a clause holds, were each no more than what holds it.
    let messages = HELD_BYTES / HELD_EVENT_BYTES + 1;
    let config = json!({ "name": "long", "messages": messages });
    let report = certify_source::<Queue>(config).await;
    assert!(
        matches!(outcome(&report), Outcome::Unobserved(_)),
        "{report}"
    );
    assert_eq!(report.failures().count(), 0, "{report}");
}
