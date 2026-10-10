//! `S-RESUME`: a read resumed from a checkpoint yields exactly the data after it.

use rdlt_wire::limits::count;

use super::plan;
use super::recording::{Budget, record};
use crate::catalog::Catalog;
use crate::cost;
use crate::sink::Push;
use crate::source::Source;
use crate::testing::Violation;
use crate::testing::limits::{RENDERED_BYTES, RESUME_SAMPLES};

/// The checkpoints, of the `sent` a read sent, a resume is checked from: each of up to
/// [`RESUME_SAMPLES`], else that many spread from the first to the last.
pub(in crate::testing) fn sampled(sent: usize) -> Vec<usize> {
    if sent <= RESUME_SAMPLES {
        return (0..sent).collect();
    }
    (0..RESUME_SAMPLES)
        .map(|sample| sample * (sent - 1) / (RESUME_SAMPLES - 1))
        .collect()
}

/// `S-RESUME`: not observed of a source none of whose reads sends a checkpoint.
pub(super) async fn resumes_are_exact(
    source: &dyn Source,
    catalog: &Catalog,
    budget: &Budget,
) -> Result<(), Violation> {
    let mut observed = false;
    for stream in catalog.iter() {
        for (partition, start) in plan(source, stream.name()).await? {
            let read = (stream, &partition);
            let full = record(source, read, start, None, budget).await?;
            for index in sampled(full.checkpoints.len()) {
                observed = true;
                let cursor = &full.checkpoints[index];
                let resumed = record(source, read, Some(cursor.clone()), None, budget).await?;
                let expected: Vec<&Push> = full.segments[index + 1..]
                    .iter()
                    .flatten()
                    .chain(&full.tail)
                    .collect();
                let actual: Vec<&Push> = resumed
                    .segments
                    .iter()
                    .flatten()
                    .chain(&resumed.tail)
                    .collect();
                if !comparable(expected.iter().chain(&actual).copied()) {
                    return Err(Violation::unobserved(format_args!(
                        "stream {} partition {}: the pushes after checkpoint {} expand beyond the \
                         {RENDERED_BYTES} bytes a comparison holds",
                        stream.name(),
                        partition.id(),
                        index + 1
                    )));
                }
                if expected != actual {
                    return Err(format!(
                        "stream {} partition {}: resuming from checkpoint {} yielded {} pushes, expected {}",
                        stream.name(),
                        partition.id(),
                        index + 1,
                        actual.len(),
                        expected.len()
                    ).into());
                }
            }
        }
    }
    if observed {
        Ok(())
    } else {
        Err(Violation::unobserved(
            "no read sent a checkpoint to resume from",
        ))
    }
}

/// Whether `pushes` can be compared within what a comparison renders: their batches, all
/// together, expand to [`RENDERED_BYTES`] at most, as the cost model measures them.
///
/// Comparing two batches compares every value each of their rows names, so a batch that keeps
/// little alive can take far longer to compare than to hold; JSON is compared as its text.
fn comparable<'a>(pushes: impl IntoIterator<Item = &'a Push>) -> bool {
    let measuring = cost::Rendering::native();
    let mut room = count(RENDERED_BYTES);
    for push in pushes {
        let batch = match push {
            Push::Arrow(batch) | Push::Changes(batch) => batch,
            Push::Json(_) => continue,
        };
        let expanded = measuring.expanded(batch, 0..batch.num_rows(), room);
        let Some(left) = room.checked_sub(expanded) else {
            return false;
        };
        room = left;
    }
    true
}

#[cfg(test)]
mod tests;
