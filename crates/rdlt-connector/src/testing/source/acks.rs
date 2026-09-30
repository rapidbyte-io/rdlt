//! `S-ACK`: a source's position outside the engine moves only when the engine tells it a cursor is
//! committed, and then to that cursor.
//!
//! The clause runs last, so its commits disturb no other clause's reads: it first checks that
//! those reads moved no partition from where it stood before them. It then reads a change stream's
//! phases as the engine does, each partition of a phase to its end, then plans again from where
//! they ended, so it reaches the changes a snapshot precedes. In each partition it reads on from
//! where the source says the partition stands, asking at every checkpoint where every partition it
//! has seen stands, and tells the source the first checkpoints ahead are committed.
//!
//! The phase it reads last decides: each partition told a checkpoint there must stand at it. In
//! every phase, the partitions told checkpoints either all keep them or all keep none, as a
//! snapshot's partitions may.

mod watch;

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::catalog::{Catalog, ReadMode, StreamSpec};
use crate::cursor::Cursor;
use crate::id::PartitionId;
use crate::source::{AcknowledgedReader, Partition, PartitionPlan, Source};
use crate::state::{PartitionState, StreamState};
use crate::testing::{Outcome, Violation, bounded_call, outcome};

/// How many phases of a stream the clause reads, as a snapshot and then its changes are two.
const PHASES: usize = 4;

/// How many checkpoints ahead of where a partition stands the clause tells it are committed.
const TOLD: usize = 2;

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
    let probed = Probed::new(source, reader, stream);
    let mut standing = Vec::new();
    for partition in probed.plan(&StreamState::default()).await?.partitions {
        let position = probed.position(partition.id()).await?;
        standing.push((partition.id().clone(), position));
    }
    Ok(standing)
}

/// What tells where a source stands, and where it stood before the other clauses.
pub(super) type Told = (Arc<dyn AcknowledgedReader>, Standing);

/// `S-ACK` against `source`, where `told` says what tells where it stands, or why asking failed.
pub(super) async fn acknowledged_only_when_committed(
    source: &dyn Source,
    told: Option<Result<Told, Violation>>,
    catalog: &Catalog,
) -> Outcome {
    let Some(told) = told else {
        return Outcome::Skipped("the source does not tell where it stands".to_owned());
    };
    let (reader, standing) = match told {
        Ok(told) => told,
        Err(violation) => return outcome(Err(violation)),
    };
    let Some(stream) = changes(catalog) else {
        return Outcome::Skipped("the source reads no stream as changes".to_owned());
    };
    let mut probed = Probed::new(source, reader.as_ref(), stream);
    let checked = async {
        probed.unmoved_since(standing).await?;
        probed.walk().await
    };
    match checked.await {
        Err(violation) => outcome(Err(violation)),
        Ok(Last::Nothing) => Outcome::Skipped(format!(
            "no partition of the last phase of stream {} read has anything ahead of where it \
             stands to acknowledge, and reading them moved nothing",
            stream.name()
        )),
        Ok(Last::Unkept(partition)) => Outcome::Failed(format!(
            "stream {} partition {partition}: told checkpoints are committed, it keeps none",
            stream.name()
        )),
        Ok(Last::Kept) => Outcome::Passed,
    }
}

/// What the partitions of the last phase the clause read did with the checkpoints it told them.
enum Last {
    /// None had anything ahead to tell.
    Nothing,
    /// Each stands at the last it was told.
    Kept,
    /// None keeps a position, as this one shows.
    Unkept(PartitionId),
}

/// What probing a partition found.
struct Probe {
    /// Where the engine would record the partition's end, if anywhere.
    end: Option<PartitionState>,
    /// Whether it keeps the checkpoints it was told, where any lay ahead.
    keeps: Option<bool>,
}

/// A change stream, read and asked where its partitions stand.
struct Probed<'a> {
    source: &'a dyn Source,
    reader: &'a dyn AcknowledgedReader,
    stream: &'a StreamSpec,
    /// Where each partition the clause has seen stands, as it last found it.
    seen: BTreeMap<PartitionId, Option<Cursor>>,
}

impl<'a> Probed<'a> {
    fn new(
        source: &'a dyn Source,
        reader: &'a dyn AcknowledgedReader,
        stream: &'a StreamSpec,
    ) -> Self {
        Self {
            source,
            reader,
            stream,
            seen: BTreeMap::new(),
        }
    }

    /// A violation unless each partition stands where `standing` says it stood.
    async fn unmoved_since(&mut self, standing: Standing) -> Result<(), Violation> {
        for (partition, stood) in standing {
            if self.position(&partition).await? != stood {
                return Err(self.violation(
                    &partition,
                    "the other clauses' reads moved where it stands, nothing committed",
                ));
            }
            self.seen.insert(partition, stood);
        }
        Ok(())
    }

    /// Probes the partitions of each phase, until planning names no new phase or a phase reads an
    /// unbounded partition, which never ends; what the last phase's partitions kept.
    async fn walk(&mut self) -> Result<Last, Violation> {
        let mut state = StreamState::default();
        let mut last = Last::Nothing;
        for walked in 0..PHASES {
            let planned = self.plan(&state).await?;
            let phase = planned.phase.unwrap_or(state.phase);
            if walked > 0 && phase == state.phase {
                break;
            }
            let mut ended = BTreeMap::new();
            let mut told = Vec::new();
            for partition in &planned.partitions {
                let start = planned.starts.get(partition.id()).cloned();
                let probe = self.probe(partition, start).await?;
                if let Some(end) = probe.end {
                    ended.insert(partition.id().clone(), end);
                }
                if let Some(keeps) = probe.keeps {
                    told.push((partition.id().clone(), keeps));
                }
            }
            last = self.judged(&told)?;
            if planned.partitions.iter().any(Partition::is_unbounded) {
                break;
            }
            state = StreamState {
                phase,
                partitions: ended,
                ..StreamState::default()
            };
        }
        Ok(last)
    }

    /// What a phase's partitions, `told` checkpoints and whether each kept them, show.
    fn judged(&self, told: &[(PartitionId, bool)]) -> Result<Last, Violation> {
        let keeping = told.iter().find(|(_, keeps)| *keeps);
        let unkept = told.iter().find(|(_, keeps)| !*keeps);
        match (keeping, unkept) {
            (Some((keeping, _)), Some((unkept, _))) => Err(self.violation(
                unkept,
                &format!("keeps no position, though partition {keeping} of its phase does"),
            )),
            (Some(_), None) => Ok(Last::Kept),
            (None, Some((unkept, _))) => Ok(Last::Unkept(unkept.clone())),
            (None, None) => Ok(Last::Nothing),
        }
    }

    async fn plan(&self, state: &StreamState) -> Result<PartitionPlan, Violation> {
        let name = self.stream.name();
        bounded_call("plan", self.source.plan(name, state))
            .await
            .map_err(|Violation(reason)| Violation::from(format!("plan {name}: {reason}")))
    }

    /// Probes `partition`, which its phase starts at `start`.
    async fn probe(
        &mut self,
        partition: &Partition,
        start: Option<Cursor>,
    ) -> Result<Probe, Violation> {
        // Read on from where it stands, which an earlier load may have moved: each checkpoint
        // then lies ahead of it.
        let id = partition.id();
        let before = self.position(id).await?;
        self.seen.insert(id.clone(), before.clone());
        let from = before.clone().or_else(|| start.clone());
        let read = self.watched(partition, from.clone()).await?;
        let end = read.end(from.as_ref());
        if read.checkpoints.is_empty() {
            // Nothing lies ahead: a read from where the phase starts must still move nothing,
            // where the stream can read it again.
            if before.is_some() && self.stream.is_replayable() {
                self.watched(partition, start).await?;
            }
            return Ok(Probe { end, keeps: None });
        }
        let mut keeps = None;
        for (index, cursor) in read.checkpoints.iter().take(TOLD).enumerate() {
            let told = [(id.clone(), cursor.clone())];
            bounded_call(
                "committed",
                self.source.committed(self.stream.name(), &told),
            )
            .await?;
            // A partition that keeps no position may keep none still; one that keeps one stands
            // where it was told.
            let now = self.position(id).await?;
            let kept = match (&now, self.seen.get(id).cloned().flatten()) {
                (Some(now), _) if now == cursor => true,
                (None, None) => false,
                _ => {
                    let what = format!(
                        "told checkpoint {} is committed, it stands elsewhere",
                        index + 1
                    );
                    return Err(self.violation(id, &what));
                }
            };
            self.seen.insert(id.clone(), now);
            // No other partition moved with it, and reading on without committing moves nothing.
            self.unmoved().await?;
            self.watched(partition, Some(cursor.clone())).await?;
            keeps = Some(kept);
        }
        Ok(Probe { end, keeps })
    }

    /// Where `partition` stands.
    async fn position(&self, partition: &PartitionId) -> Result<Option<Cursor>, Violation> {
        let asked = self.reader.acknowledged(self.stream.name(), partition);
        bounded_call("acknowledged", asked).await
    }

    fn violation(&self, partition: &PartitionId, what: &str) -> Violation {
        Violation::from(format!(
            "stream {} partition {partition}: {what}",
            self.stream.name(),
        ))
    }
}
