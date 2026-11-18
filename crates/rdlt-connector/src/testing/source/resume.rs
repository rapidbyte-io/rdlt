//! `S-RESUME`: a read resumed from a checkpoint yields exactly the data after it.
//!
//! The data is the rows, in order, whatever pushes carry them: a source may push what each poll
//! returned, so a resumed read's pushes need not end where the first read's did.

use arrow_array::RecordBatch;
use rdlt_wire::limits::count;

use super::json::Records;
use super::plan;
use super::recording::{Budget, record};
use crate::catalog::Catalog;
use crate::cost;
use crate::sink::Push;
use crate::source::Source;
use crate::testing::Violation;
use crate::testing::limits::{REASON_BYTES, RENDERED_BYTES, RESUME_SAMPLES};
use crate::testing::render::Rendering;

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
                if let Some(parted) = parted(&runs(expected), &runs(actual)) {
                    return Err(Violation::from(format!(
                        "stream {} partition {}: resumed from checkpoint {}, its row {} after it \
                         is {}, and the first read's {}",
                        stream.name(),
                        partition.id(),
                        index + 1,
                        parted.row + 1,
                        described(parted.actual).await,
                        described(parted.expected).await
                    )));
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

/// Rows of one push, from a row on: a batch's, of changes or not, or one JSON record.
#[derive(Clone, Copy, Debug)]
enum Run<'a> {
    Batch {
        changes: bool,
        batch: &'a RecordBatch,
        at: usize,
    },
    Record(&'a [u8]),
}

impl Run<'_> {
    /// What is left of the run once `rows` of it compared alike; none once nothing is.
    fn after(self, rows: usize) -> Option<Self> {
        match self {
            Self::Batch { changes, batch, at } if at + rows < batch.num_rows() => {
                Some(Self::Batch {
                    changes,
                    batch,
                    at: at + rows,
                })
            }
            Self::Batch { .. } | Self::Record(_) => None,
        }
    }
}

/// The rows of `pushes`, in order: each batch's as one run, and each JSON record, made
/// canonical, as a run of its own.
fn runs<'a>(pushes: impl IntoIterator<Item = &'a Push>) -> Vec<Run<'a>> {
    let mut runs = Vec::new();
    for push in pushes {
        let (changes, batch) = match push {
            Push::Arrow(batch) => (false, batch),
            Push::Changes(batch) => (true, batch),
            Push::Json(text) => {
                runs.extend(Records::new(text).map(|record| Run::Record(&text[record])));
                continue;
            }
        };
        if batch.num_rows() > 0 {
            runs.push(Run::Batch {
                changes,
                batch,
                at: 0,
            });
        }
    }
    runs
}

/// Where two reads' rows first part: the row, counted from 0, and where each read's rows go on
/// from it, none where they ended.
#[derive(Debug)]
struct Parted<'a> {
    row: usize,
    expected: Option<Run<'a>>,
    actual: Option<Run<'a>>,
}

/// Where the rows `actual` holds first part from those `expected` holds, both in order; none
/// where they are the same rows of the same kinds, each exactly equal.
fn parted<'a>(expected: &[Run<'a>], actual: &[Run<'a>]) -> Option<Parted<'a>> {
    let (mut left, mut right) = (expected.iter().copied(), actual.iter().copied());
    let (mut one, mut other) = (left.next(), right.next());
    let mut row = 0;
    loop {
        let alike = match (one, other) {
            (None, None) => return None,
            (
                Some(Run::Batch { changes, batch, at }),
                Some(Run::Batch {
                    changes: also,
                    batch: against,
                    at: from,
                }),
            ) if changes == also => {
                let rows = (batch.num_rows() - at).min(against.num_rows() - from);
                let same = |offset: usize, len: usize| {
                    batch.slice(at + offset, len) == against.slice(from + offset, len)
                };
                if !same(0, rows) {
                    // Rows of another schema differ one by one too, from the first.
                    let first = (0..rows).find(|offset| !same(*offset, 1)).unwrap_or(0);
                    let run = |batch, at| Some(Run::Batch { changes, batch, at });
                    return Some(Parted {
                        row: row + first,
                        expected: run(batch, at + first),
                        actual: run(against, from + first),
                    });
                }
                rows
            }
            (Some(Run::Record(text)), Some(Run::Record(against))) if text == against => 1,
            (expected, actual) => {
                return Some(Parted {
                    row,
                    expected,
                    actual,
                });
            }
        };
        row += alike;
        one = one.and_then(|run| run.after(alike)).or_else(|| left.next());
        other = other
            .and_then(|run| run.after(alike))
            .or_else(|| right.next());
    }
}

/// The first row of `run` as text, cut at [`REASON_BYTES`]; `nothing` where a read ended.
async fn described(run: Option<Run<'_>>) -> String {
    match run {
        None => "nothing".to_owned(),
        Some(Run::Record(text)) => {
            String::from_utf8_lossy(&text[..text.len().min(REASON_BYTES)]).into_owned()
        }
        Some(Run::Batch { changes, batch, at }) => {
            let mut rendering = Rendering::new(REASON_BYTES);
            let rows = rendering.rows(&batch.slice(at, 1), |_| true).await;
            let row = rows.ok().and_then(|rows| rows.into_iter().next());
            let row = row.unwrap_or_else(|| "a row longer than a reason holds".to_owned());
            if changes {
                format!("the change {{{row}}}")
            } else {
                format!("{{{row}}}")
            }
        }
    }
}

#[cfg(test)]
mod tests;
