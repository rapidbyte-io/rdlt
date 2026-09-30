//! `S-PARTITION`: a stream's planned partitions cover it exactly once, and those planned again
//! from where they stood cover what is left exactly once.
//!
//! The truth is one uninterrupted read of every partition the stream's first plan names. The
//! clause then reads each of those partitions to its first checkpoint, plans the stream again from
//! where they stood, as the engine does after a commit, and reads what that plan names from
//! there: the rows of the two reads must be the same, each as many times.

use std::collections::BTreeMap;

use super::{Recording, placed, record};
use crate::catalog::{Catalog, StreamSpec};
use crate::sink::Push;
use crate::source::{Partition, PartitionPlan, Source};
use crate::state::{PartitionState, StreamState};
use crate::testing::{Outcome, Violation, bounded_call, outcome};

/// `S-PARTITION` against every stream of `catalog` that it can check.
pub(super) async fn partitions_cover_exactly_once(
    source: &dyn Source,
    catalog: &Catalog,
) -> Outcome {
    let mut checked = false;
    for stream in catalog.iter() {
        match covered(source, stream).await {
            Ok(covered) => checked = checked || covered,
            Err(violation) => return outcome(Err(violation)),
        }
    }
    if checked {
        Outcome::Passed
    } else {
        Outcome::Skipped(
            "no stream has partitions that end and checkpoint before their end".to_owned(),
        )
    }
}

/// Checks `stream`, where its partitions end and one checkpoints before its end: whether it did.
async fn covered(source: &dyn Source, stream: &StreamSpec) -> Result<bool, Violation> {
    let fresh = StreamState::default();
    let first = planned(source, stream, &fresh).await?;
    if first.partitions.iter().any(Partition::is_unbounded) {
        return Ok(false);
    }
    let (mut whole, mut read, mut stood) = (Rows::default(), Rows::default(), BTreeMap::new());
    for (partition, cursor) in placed(&first, &fresh) {
        let recording = record(source, stream, &partition, cursor, None).await?;
        whole.add_all(&recording);
        if let (Some(cursor), Some(segment)) =
            (recording.checkpoints.first(), recording.segments.first())
        {
            read.add(segment);
            let state = PartitionState::Cursor(cursor.clone());
            stood.insert(partition.id().clone(), state);
        }
    }
    if stood.is_empty() {
        return Ok(false);
    }
    let state = StreamState {
        phase: first.phase.unwrap_or(fresh.phase),
        partitions: stood,
        ..StreamState::default()
    };
    let again = planned(source, stream, &state).await?;
    for (partition, cursor) in placed(&again, &state) {
        read.add_all(&record(source, stream, &partition, cursor, None).await?);
    }
    match whole.difference(&read) {
        None => Ok(true),
        Some((missing, extra)) => Err(Violation::from(format!(
            "stream {}: planned again from its first checkpoints, its partitions read {missing} \
             rows fewer and {extra} more than one read of the whole stream",
            stream.name()
        ))),
    }
}

async fn planned(
    source: &dyn Source,
    stream: &StreamSpec,
    state: &StreamState,
) -> Result<PartitionPlan, Violation> {
    let name = stream.name();
    bounded_call("plan", source.plan(name, state))
        .await
        .map_err(|Violation(reason)| Violation::from(format!("plan {name}: {reason}")))
}

/// Rows, each as many times as it was read, rendered alike however they were pushed.
#[derive(Default, PartialEq, Eq)]
struct Rows(BTreeMap<String, usize>);

impl Rows {
    fn add_all(&mut self, recording: &Recording) {
        for push in recording.segments.iter().flatten().chain(&recording.tail) {
            self.push(push);
        }
    }

    fn add(&mut self, segment: &[Push]) {
        for push in segment {
            self.push(push);
        }
    }

    fn push(&mut self, push: &Push) {
        for row in rendered(push) {
            *self.0.entry(row).or_default() += 1;
        }
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

/// Each row `push` holds, rendered: a JSON row as its text, an Arrow row as each column's name
/// and value.
fn rendered(push: &Push) -> Vec<String> {
    match push {
        Push::Json(bytes) => {
            let rows: Vec<serde_json::Value> = serde_json::from_slice(bytes).unwrap_or_default();
            rows.iter().map(ToString::to_string).collect()
        }
        Push::Arrow(batch) | Push::Changes(batch) => (0..batch.num_rows())
            .map(|row| {
                let schema = batch.schema();
                schema
                    .fields()
                    .iter()
                    .zip(batch.columns())
                    .map(|(field, column)| format!("{}={:?}", field.name(), column.slice(row, 1)))
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect(),
    }
}
