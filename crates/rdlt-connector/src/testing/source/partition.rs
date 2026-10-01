//! `S-PARTITION`: a stream's planned partitions cover it exactly once, and those planned again
//! from where they stood cover what is left exactly once.
//!
//! The truth is one uninterrupted read of every partition the stream's first plan names. The
//! clause then reads each of those partitions to its first checkpoint, plans the stream again from
//! where they stood, as the engine does after a commit, and reads what that plan names from
//! there: the rows of the two reads must be the same, each as many times.

use std::collections::BTreeMap;

use super::json::Records;
use super::placed;
use super::recording::{Budget, Recording, record};
use crate::catalog::{Catalog, StreamSpec};
use crate::sink::Push;
use crate::source::{Partition, PartitionPlan, Source};
use crate::state::{PartitionState, StreamState};
use crate::testing::limits::{RENDERED_BYTES, YIELD_BYTES};
use crate::testing::render::Rendering;
use crate::testing::{Outcome, Violation, bounded_call, outcome};

/// `S-PARTITION` against every stream of `catalog` that it can check.
pub(super) async fn partitions_cover_exactly_once(
    source: &dyn Source,
    catalog: &Catalog,
    budget: &Budget,
) -> Outcome {
    let (mut checked, mut uncheckpointed) = (false, false);
    for stream in catalog.iter() {
        match covered(source, stream, budget).await {
            Ok(Covered::Checked) => checked = true,
            Ok(Covered::Uncheckpointed) => uncheckpointed = true,
            Ok(Covered::Unbounded) => {}
            Err(violation) => return outcome(Err(violation)),
        }
    }
    if checked {
        Outcome::Passed
    } else if uncheckpointed {
        Outcome::Unobserved("no stream's partitions checkpoint before their end".into())
    } else {
        Outcome::Inapplicable("every stream plans a partition that never ends".into())
    }
}

/// What checking a stream came to.
enum Covered {
    /// Its partitions cover it exactly once, however planned.
    Checked,
    /// It plans a partition that never ends, which no single read covers.
    Unbounded,
    /// None of its partitions checkpoints before its end, so nothing is planned again.
    Uncheckpointed,
}

/// Checks `stream`, where its partitions end and one checkpoints before its end.
async fn covered(
    source: &dyn Source,
    stream: &StreamSpec,
    budget: &Budget,
) -> Result<Covered, Violation> {
    let fresh = StreamState::default();
    let first = planned(source, stream, &fresh).await?;
    if first.partitions.iter().any(Partition::is_unbounded) {
        return Ok(Covered::Unbounded);
    }
    let (mut whole, mut stood) = (Vec::new(), BTreeMap::new());
    for (partition, cursor) in placed(&first, &fresh) {
        let recording = record(source, (stream, &partition), cursor, None, budget).await?;
        if let Some(cursor) = recording.checkpoints.first() {
            let state = PartitionState::Cursor(cursor.clone());
            stood.insert(partition.id().clone(), state);
        }
        whole.push(recording);
    }
    // Nothing is rendered of a stream the clause cannot check.
    if stood.is_empty() {
        return Ok(Covered::Uncheckpointed);
    }
    let state = StreamState {
        phase: first.phase.unwrap_or(fresh.phase),
        partitions: stood,
        ..StreamState::default()
    };
    let again = planned(source, stream, &state).await?;
    let mut rest = Vec::new();
    for (partition, cursor) in placed(&again, &state) {
        rest.push(record(source, (stream, &partition), cursor, None, budget).await?);
    }
    let mut rendering = Rendering::new(RENDERED_BYTES);
    let (mut once, mut read) = (Rows::default(), Rows::default());
    for recording in &whole {
        once.add(&mut rendering, pushes(recording)).await?;
        let first = recording
            .segments
            .first()
            .filter(|_| !recording.checkpoints.is_empty());
        read.add(&mut rendering, first.into_iter().flatten())
            .await?;
    }
    for recording in &rest {
        read.add(&mut rendering, pushes(recording)).await?;
    }
    match once.difference(&read) {
        None => Ok(Covered::Checked),
        Some((missing, extra)) => Err(Violation::from(format!(
            "stream {}: planned again from its first checkpoints, its partitions read {missing} \
             rows fewer and {extra} more than one read of the whole stream",
            stream.name()
        ))),
    }
}

/// Every push of `recording`, in order.
fn pushes(recording: &Recording) -> impl Iterator<Item = &Push> {
    recording.segments.iter().flatten().chain(&recording.tail)
}

async fn planned(
    source: &dyn Source,
    stream: &StreamSpec,
    state: &StreamState,
) -> Result<PartitionPlan, Violation> {
    let name = stream.name();
    bounded_call("plan", source.plan(name, state))
        .await
        .map_err(|violation| violation.of(format_args!("plan {name}")))
}

/// Rows, each as many times as it was read, rendered alike however they were pushed.
#[derive(Default, PartialEq, Eq)]
struct Rows(BTreeMap<String, usize>);

impl Rows {
    /// Counts each row of `pushes`, rendered within `rendering`'s limit: a JSON row as its
    /// text, an Arrow row as each column's name and value.
    async fn add<'a>(
        &mut self,
        rendering: &mut Rendering,
        pushes: impl Iterator<Item = &'a Push>,
    ) -> Result<(), Violation> {
        for push in pushes {
            // Rows that cannot be compared leave the clause unobserved: they break nothing.
            match push {
                Push::Json(text) => self.json(rendering, text).await?,
                Push::Arrow(batch) | Push::Changes(batch) => {
                    let rows = rendering.rows(batch, |_| true).await;
                    for row in rows.map_err(Violation::unobserved)? {
                        *self.0.entry(row).or_default() += 1;
                    }
                }
            }
            tokio::task::yield_now().await;
        }
        Ok(())
    }

    /// Counts each row of `text`, a JSON push made canonical: each record is its row's text,
    /// charged before it is kept.
    async fn json(&mut self, rendering: &mut Rendering, text: &[u8]) -> Result<(), Violation> {
        let mut yielded = 0;
        for record in Records::new(text) {
            let end = record.end;
            let row = String::from_utf8_lossy(&text[record]);
            rendering.charge(&row).map_err(Violation::unobserved)?;
            *self.0.entry(row.into_owned()).or_default() += 1;
            if end - yielded >= YIELD_BYTES {
                yielded = end;
                tokio::task::yield_now().await;
            }
        }
        Ok(())
    }

    /// How many rows `other` lacks and holds beyond these, where the two differ.
    fn difference(&self, other: &Self) -> Option<(usize, usize)> {
        let count = |rows: &Self, row: &String| rows.0.get(row).copied().unwrap_or_default();
        let missing = self
            .0
            .iter()
            .map(|(row, times)| times.saturating_sub(count(other, row)))
            .sum();
        let extra = other
            .0
            .iter()
            .map(|(row, times)| times.saturating_sub(count(self, row)))
            .sum();
        (missing + extra > 0).then_some((missing, extra))
    }
}
