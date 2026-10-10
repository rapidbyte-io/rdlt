//! One partition's read, recorded: what it pushed, split at its checkpoints.

#[cfg(test)]
mod tests;

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::json;
use crate::catalog::StreamSpec;
use crate::cost::push_charge;
use crate::cursor::Cursor;
use crate::sink::{Push, SourceEvent, partition_channel};
use crate::source::{Partition, ReadRequest, Source};
use crate::testing::limits::{HELD_BYTES, HELD_EVENT_BYTES, HELD_ROWS};
use crate::testing::{Violation, bounded};

/// What one clause may still hold of what its reads send: bytes and rows, all its reads
/// together, so neither the partitions nor the streams a source plans multiply it.
pub(in crate::testing) struct Budget {
    bytes: AtomicUsize,
    rows: AtomicUsize,
}

impl Budget {
    /// A clause's budget: [`HELD_BYTES`] and [`HELD_ROWS`].
    pub(in crate::testing) fn new() -> Self {
        Self::holding(HELD_BYTES)
    }

    /// A budget of `bytes` and [`HELD_ROWS`].
    pub(in crate::testing) fn holding(bytes: usize) -> Self {
        Self {
            bytes: AtomicUsize::new(bytes),
            rows: AtomicUsize::new(HELD_ROWS),
        }
    }

    /// Charges a push the clause holds, a JSON push for the records its text holds.
    async fn push(&self, push: &Push) -> Result<(), Violation> {
        let rows = match push {
            Push::Arrow(batch) | Push::Changes(batch) => batch.num_rows(),
            Push::Json(text) => json::counted(text).await?,
        };
        let bytes = push_charge(push);
        self.charge(usize::try_from(bytes).unwrap_or(usize::MAX), rows)
    }

    /// Charges a checkpoint's cursor the clause holds.
    pub(in crate::testing) fn cursor(&self, cursor: &Cursor) -> Result<(), Violation> {
        self.charge(cursor.bytes().len(), 0)
    }

    /// Charges an event of `bytes` and `rows`; a violation, of a clause not observed, once the
    /// source has sent more than the clause holds.
    fn charge(&self, bytes: usize, rows: usize) -> Result<(), Violation> {
        let bytes = bytes.saturating_add(HELD_EVENT_BYTES);
        let within = spend(&self.bytes, bytes) & spend(&self.rows, rows);
        if within {
            Ok(())
        } else {
            Err(Violation::unobserved(format_args!(
                "the source sends more than the {HELD_BYTES} bytes and {HELD_ROWS} rows a \
                 clause holds: certify it with less data"
            )))
        }
    }
}

/// Takes `spent` from what `left` holds; whether that much was left, none being left after.
fn spend(left: &AtomicUsize, spent: usize) -> bool {
    let took = left.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
        Some(left.saturating_sub(spent))
    });
    took.is_ok_and(|left| left >= spent)
}

/// Everything one partition read produced, split at checkpoints.
#[derive(Default)]
pub(super) struct Recording {
    /// Data sealed by each checkpoint, in order.
    pub(super) segments: Vec<Vec<Push>>,
    /// The checkpoint that sealed each segment.
    pub(super) checkpoints: Vec<Cursor>,
    /// Data after the last checkpoint.
    pub(super) tail: Vec<Push>,
    /// Barriers the checkpoints answered.
    pub(super) answered: Vec<u64>,
}

impl Recording {
    /// Holds `event`, charged to `budget` before it is held, or parsed.
    async fn hold(&mut self, event: SourceEvent, budget: &Budget) -> Result<(), Violation> {
        match event {
            SourceEvent::Push(push) => {
                budget.push(&push).await?;
                self.tail.push(normalize(push).await);
            }
            SourceEvent::Checkpoint { cursor, answers } => {
                budget.cursor(&cursor)?;
                self.segments.push(std::mem::take(&mut self.tail));
                self.checkpoints.push(cursor);
                self.answered.extend(answers);
            }
            SourceEvent::Log { .. }
            | SourceEvent::Metric { .. }
            | SourceEvent::Replan
            | SourceEvent::Behind { .. } => {}
        }
        Ok(())
    }
}

/// What a read of `partition` of `stream` from `cursor` sends, held within `budget`: a read
/// that sends more is stopped, and what it sends after is dropped.
pub(super) async fn record(
    source: &dyn Source,
    (stream, partition): (&StreamSpec, &Partition),
    cursor: Option<Cursor>,
    barrier: Option<u64>,
    budget: &Budget,
) -> Result<Recording, Violation> {
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(64).expect("64 is non-zero"));
    if let Some(barrier) = barrier {
        feed.request_checkpoint(barrier);
    }
    let request = ReadRequest::new(stream.name().clone(), partition.clone(), cursor);
    let collect = async {
        let mut recording = Recording::default();
        while let Some(event) = feed.recv().await {
            if let Err(beyond) = recording.hold(event, budget).await {
                feed.stop();
                while feed.recv().await.is_some() {}
                return Err(beyond);
            }
        }
        Ok(recording)
    };
    let what = format!("reading {} partition {}", stream.name(), partition.id());
    let (read, recording) = bounded(&what, async {
        tokio::join!(source.read(request, sink), collect)
    })
    .await?;
    // A read stopped for sending too much may end in an error of its own: the budget says why.
    let recording = recording?;
    read.map_err(|error| Violation::from(format!("{what}: {error}")))?;
    Ok(recording)
}

/// Rewrites a JSON push canonically, so equal rows compare equal whatever their formatting.
pub(in crate::testing) async fn normalize(push: Push) -> Push {
    match push {
        Push::Json(text) => Push::Json(json::canonical(&text).await),
        other => other,
    }
}
