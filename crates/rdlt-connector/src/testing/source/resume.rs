//! `S-RESUME`: a read resumed from a checkpoint yields exactly the data after it.

use super::plan;
use super::recording::record;
use crate::catalog::Catalog;
use crate::sink::Push;
use crate::source::Source;
use crate::testing::Violation;
use crate::testing::limits::RESUME_SAMPLES;

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
) -> Result<(), Violation> {
    let mut observed = false;
    for stream in catalog.iter() {
        for (partition, start) in plan(source, stream.name()).await? {
            let full = record(source, stream, &partition, start, None).await?;
            for index in sampled(full.checkpoints.len()) {
                observed = true;
                let cursor = &full.checkpoints[index];
                let resumed =
                    record(source, stream, &partition, Some(cursor.clone()), None).await?;
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
