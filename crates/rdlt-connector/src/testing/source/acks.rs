//! `S-ACK`: a source's position outside the engine moves only when the engine tells it a cursor is
//! committed, and then to that cursor.
//!
//! The clause runs last, so its commits disturb no other clause's reads: it first checks that
//! those reads moved no partition from where it stood before them. It then reads a change stream's
//! phases as the engine does, each partition of a phase to its
//! end, then plans again from where they ended, so it reaches the changes a snapshot precedes. In
//! each partition it reads on from where the source says the partition stands, asking again at
//! every checkpoint, and tells the source the first checkpoints ahead are committed.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::catalog::{Catalog, ReadMode, StreamSpec};
use crate::cursor::Cursor;
use crate::id::PartitionId;
use crate::sink::{PartitionFeed, SourceEvent, partition_channel};
use crate::source::{AcknowledgedReader, Partition, PartitionPlan, ReadRequest, Source};
use crate::state::{PartitionState, StreamState};
use crate::testing::{Outcome, Violation, bounded, bounded_call, outcome};

use super::STOP_WINDOW;

/// How many phases of a stream the clause reads, as a snapshot and then its changes are two.
const PHASES: usize = 4;

/// How many checkpoints ahead of where a partition stands the clause tells it are committed.
const TOLD: usize = 2;

/// How many checkpoints a read of an unbounded partition sends before it is asked to stop.
const CHECKPOINTS: usize = 3;

/// How long a read of an unbounded partition may send nothing before it is asked to stop.
const QUIET: Duration = Duration::from_secs(1);

/// Where each partition of a change stream's first phase stands.
pub(super) type Standing = Vec<(PartitionId, Option<Cursor>)>;

/// The stream `S-ACK` reads: the first `catalog` reads as changes.
fn changes(catalog: &Catalog) -> Option<&StreamSpec> {
    catalog.iter().find(|stream| stream.supports(ReadMode::Cdc))
}

/// Where the partitions of `catalog`'s change stream stand before any clause reads them, as
/// `reader` tells.
pub(super) async fn standing(
    source: &dyn Source,
    reader: &dyn AcknowledgedReader,
    catalog: &Catalog,
) -> Result<Standing, Violation> {
    let Some(stream) = changes(catalog) else {
        return Ok(Vec::new());
    };
    let probed = Probed {
        source,
        reader,
        stream,
    };
    let mut standing = Vec::new();
    for partition in probed.plan(&StreamState::default()).await?.partitions {
        let position = probed.position(partition.id()).await?;
        standing.push((partition.id().clone(), position));
    }
    Ok(standing)
}

/// `S-ACK` against `source`, whose position `reader` tells, where it tells one, and `standing`,
/// where it stood before the other clauses.
pub(super) async fn acknowledged_only_when_committed(
    source: &dyn Source,
    told: Option<(&dyn AcknowledgedReader, Result<Standing, Violation>)>,
    catalog: &Catalog,
) -> Outcome {
    let Some((reader, standing)) = told else {
        return Outcome::Skipped("the source does not tell where it stands".to_owned());
    };
    let Some(stream) = changes(catalog) else {
        return Outcome::Skipped("the source reads no stream as changes".to_owned());
    };
    let probed = Probed {
        source,
        reader,
        stream,
    };
    let checked = async {
        probed.unmoved_since(standing?).await?;
        probed.walk().await
    };
    match checked.await {
        Err(violation) => outcome(Err(violation)),
        Ok(Tally { told: 0, .. }) => Outcome::Skipped(format!(
            "no partition of stream {} has anything ahead of where it stands to acknowledge, and \
             reading them moved nothing",
            stream.name()
        )),
        Ok(Tally { kept: 0, .. }) => Outcome::Failed(format!(
            "stream {}: told checkpoints are committed, no partition stands at one",
            stream.name()
        )),
        Ok(_) => Outcome::Passed,
    }
}

/// What the clause told the source, and where it answered.
#[derive(Default)]
struct Tally {
    /// Partitions told a checkpoint is committed.
    told: usize,
    /// Of those, the partitions that stand where they were told.
    kept: usize,
}

/// A change stream, read and asked where its partitions stand.
struct Probed<'a> {
    source: &'a dyn Source,
    reader: &'a dyn AcknowledgedReader,
    stream: &'a StreamSpec,
}

impl Probed<'_> {
    /// Probes the partitions of each phase, until planning names no new phase or a phase reads an
    /// unbounded partition, which never ends.
    async fn walk(&self) -> Result<Tally, Violation> {
        let mut tally = Tally::default();
        let mut state = StreamState::default();
        for walked in 0..PHASES {
            let planned = self.plan(&state).await?;
            let phase = planned.phase.unwrap_or(state.phase);
            if walked > 0 && phase == state.phase {
                break;
            }
            let mut ended = BTreeMap::new();
            for partition in &planned.partitions {
                let start = planned.starts.get(partition.id()).cloned();
                if let Some(end) = self.probe(partition, start, &mut tally).await? {
                    ended.insert(partition.id().clone(), PartitionState::Cursor(end));
                }
            }
            if planned.partitions.iter().any(Partition::is_unbounded) {
                break;
            }
            state = StreamState {
                phase,
                partitions: ended,
                ..StreamState::default()
            };
        }
        Ok(tally)
    }

    /// A violation unless each partition stands where `standing` says it stood.
    async fn unmoved_since(&self, standing: Standing) -> Result<(), Violation> {
        for (partition, stood) in standing {
            if self.position(&partition).await? != stood {
                return Err(self.violation(
                    &partition,
                    "the other clauses' reads moved where it stands, nothing committed",
                ));
            }
        }
        Ok(())
    }

    async fn plan(&self, state: &StreamState) -> Result<PartitionPlan, Violation> {
        let name = self.stream.name();
        bounded_call("plan", self.source.plan(name, state))
            .await
            .map_err(|Violation(reason)| Violation::from(format!("plan {name}: {reason}")))
    }

    /// Probes `partition`, which its phase starts at `start`, and returns where a read of it
    /// ended.
    async fn probe(
        &self,
        partition: &Partition,
        start: Option<Cursor>,
        tally: &mut Tally,
    ) -> Result<Option<Cursor>, Violation> {
        // Read on from where it stands, which an earlier load may have moved: each checkpoint
        // then lies ahead of it.
        let before = self.position(partition.id()).await?;
        let from = before.clone().or_else(|| start.clone());
        let ahead = self
            .watched(partition, from.clone(), before.as_ref())
            .await?;
        let Some(end) = ahead.last().cloned() else {
            // Nothing lies ahead: a read from where the phase starts must still move nothing,
            // where the stream can read it again.
            if before.is_some() && self.stream.is_replayable() {
                self.watched(partition, start, before.as_ref()).await?;
            }
            return Ok(from);
        };
        tally.told += 1;
        let mut standing = before;
        for (index, cursor) in ahead.iter().take(TOLD).enumerate() {
            let told = [(partition.id().clone(), cursor.clone())];
            bounded_call(
                "committed",
                self.source.committed(self.stream.name(), &told),
            )
            .await?;
            // A partition that keeps no position may keep none still; one that keeps one stands
            // where it was told.
            let now = self.position(partition.id()).await?;
            if now.as_ref() != Some(cursor) && !(now.is_none() && standing.is_none()) {
                return Err(self.violation(
                    partition.id(),
                    &format!(
                        "told checkpoint {} is committed, it stands elsewhere",
                        index + 1
                    ),
                ));
            }
            standing = now;
            // Reading on without committing moves nothing.
            self.watched(partition, Some(cursor.clone()), standing.as_ref())
                .await?;
        }
        if standing.is_some() {
            tally.kept += 1;
        }
        Ok(Some(end))
    }

    /// Where `partition` stands.
    async fn position(&self, partition: &PartitionId) -> Result<Option<Cursor>, Violation> {
        let asked = self.reader.acknowledged(self.stream.name(), partition);
        bounded_call("acknowledged", asked).await
    }

    /// The checkpoints of a read of `partition` from `cursor`, asking at each, and once it ends,
    /// that the partition still stands at `standing`.
    ///
    /// A read of an unbounded partition is asked to stop after a few checkpoints, or once it is
    /// quiet; one asked to stop that is still quiet a moment later waits for data, and is dropped.
    async fn watched(
        &self,
        partition: &Partition,
        cursor: Option<Cursor>,
        standing: Option<&Cursor>,
    ) -> Result<Vec<Cursor>, Violation> {
        // One event at a time, so the read waits at each while it is asked where it stands.
        let (sink, feed) = partition_channel(NonZeroUsize::MIN);
        let request = ReadRequest {
            stream: self.stream.name().clone(),
            partition: partition.clone(),
            cursor,
        };
        let stopped = CancellationToken::new();
        let read = async {
            tokio::select! {
                biased;
                read = self.source.read(request, sink) => Some(read),
                () = async {
                    stopped.cancelled().await;
                    tokio::time::sleep(STOP_WINDOW).await;
                } => None,
            }
        };
        let watch = async {
            let watched = self.watch(feed, partition, standing).await;
            stopped.cancel();
            watched
        };
        let what = format!(
            "reading {} partition {}",
            self.stream.name(),
            partition.id()
        );
        let (read, checkpoints) = bounded(&what, async { tokio::join!(read, watch) }).await?;
        let checkpoints = checkpoints?;
        if let Some(read) = read {
            read.map_err(|error| Violation::from(format!("{what}: {error}")))?;
        }
        self.unmoved(partition, standing).await?;
        Ok(checkpoints)
    }

    /// The checkpoints `feed` sends, asking at each whether `partition` still stands at
    /// `standing`; however the watch ends, the read is asked to stop.
    async fn watch(
        &self,
        mut feed: PartitionFeed,
        partition: &Partition,
        standing: Option<&Cursor>,
    ) -> Result<Vec<Cursor>, Violation> {
        let unbounded = partition.is_unbounded();
        let mut checkpoints = Vec::new();
        let watched = loop {
            if unbounded && checkpoints.len() >= CHECKPOINTS {
                break Ok(());
            }
            let event = if unbounded {
                match tokio::time::timeout(QUIET, feed.recv()).await {
                    Ok(event) => event,
                    Err(_) => break Ok(()),
                }
            } else {
                feed.recv().await
            };
            let Some(event) = event else {
                break Ok(());
            };
            if let SourceEvent::Checkpoint { cursor, .. } = event {
                checkpoints.push(cursor);
                if let Err(violation) = self.unmoved(partition, standing).await {
                    break Err(violation);
                }
            }
        };
        // A stop request wins over any send, so the read's next event ends it.
        feed.stop();
        watched.map(|()| checkpoints)
    }

    /// A violation unless `partition` stands at `standing`.
    async fn unmoved(
        &self,
        partition: &Partition,
        standing: Option<&Cursor>,
    ) -> Result<(), Violation> {
        if self.position(partition.id()).await?.as_ref() == standing {
            Ok(())
        } else {
            Err(self.violation(
                partition.id(),
                "reading it moved where it stands, nothing committed",
            ))
        }
    }

    fn violation(&self, partition: &PartitionId, what: &str) -> Violation {
        Violation::from(format!(
            "stream {} partition {partition}: {what}",
            self.stream.name(),
        ))
    }
}
