//! `S-BARRIER`: an on-demand stream answers a barrier pending as its read starts, and one raised
//! during the read, with a checkpoint before the read's last push.

#[cfg(test)]
mod tests;

use std::num::NonZeroUsize;

use arrow_array::RecordBatch;
use rdlt_wire::{Limits, Weigher};

use super::plan;
use super::recording::{Budget, record_watched};
use crate::catalog::{Catalog, Checkpointing, StreamSpec};
use crate::cursor::Cursor;
use crate::sink::{Push, SourceEvent, partition_channel};
use crate::source::{Partition, Source};
use crate::testing::limits::BARRIER_SLACK;
use crate::testing::{Outcome, Violation, outcome};

/// Reads each partition of every on-demand stream with barrier 1 pending as it starts, which goes
/// with the read's start wherever the source runs, and barrier 2 raised once the read has
/// answered barrier 1 and pushed.
///
/// A read is evidence once it pushes after answering barrier 1; one that pushes nothing, or
/// pushes once and answers after it, is excused and shows nothing. Barrier 2 is judged against
/// the pushes that come more than [`BARRIER_SLACK`] events after it was raised, and a bound on
/// the pieces a source served under `limits` had its batches cut into ([`pieces`]), which the
/// source sent once it could see it: the read answers it before the last of them.
pub(super) async fn barriers_are_answered(
    source: &dyn Source,
    catalog: &Catalog,
    budget: &Budget,
    limits: Option<&Limits>,
) -> Outcome {
    let on_demand: Vec<&StreamSpec> = catalog
        .iter()
        .filter(|stream| stream.checkpointing() == Checkpointing::OnDemand)
        .collect();
    if on_demand.is_empty() {
        return Outcome::Inapplicable("no stream checkpoints on demand".into());
    }
    let check = async {
        let mut seen = false;
        for stream in on_demand {
            for (partition, start) in plan(source, stream.name()).await? {
                let read = read(source, (stream, &partition), start, budget).await?;
                let what = || format!("stream {} partition {}", stream.name(), partition.id());
                let cut = limits.map_or(0, |limits| pieces(limits, &read.after));
                match answered(&read, cut) {
                    Answered::Unseen => {}
                    Answered::InTime => seen = true,
                    Answered::Never => {
                        return Err(format!("{} never answered barrier 1", what()).into());
                    }
                    Answered::Late { barrier, pushes } => {
                        return Err(format!(
                            "{} answered barrier {barrier} only after the last of the {pushes} \
                             pushes it sent once the barrier was pending",
                            what()
                        )
                        .into());
                    }
                    Answered::Unanswered { pushes } => {
                        return Err(format!(
                            "{} never answered barrier 2 before the last of the {pushes} pushes \
                             it sent once the barrier was pending",
                            what()
                        )
                        .into());
                    }
                }
            }
        }
        if seen {
            Ok(())
        } else {
            Err(Violation::unobserved(
                "no read of an on-demand stream pushed after it answered a barrier, so none \
                 showed where it answers one",
            ))
        }
    };
    outcome(check.await)
}

/// What a read sent, in order: a push, or a checkpoint and the barrier it answered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Sent {
    Push,
    Checkpoint(Option<u64>),
}

/// A read with barrier 1 pending as it started: what it sent, how much of it had been sent when
/// barrier 2 was raised, and what the batches it pushed after that weigh.
#[derive(Debug, Default)]
struct Read {
    sent: Vec<Sent>,
    raised: Option<usize>,
    after: Weighed,
}

/// What batches weigh, as their receiver counts them: their rows, their values, and the bytes of
/// their frames and of their views; and the most bytes a frame of one of them takes beside what
/// its rows weigh.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Weighed {
    rows: u64,
    values: u64,
    frame_bytes: u64,
    view_bytes: u64,
    overhead: u64,
}

impl Weighed {
    /// Adds what `batch` weighs.
    fn add(&mut self, batch: &RecordBatch) {
        let mut weigher = Weigher::new(batch);
        weigher.begin();
        let weight = weigher.weigh_rows(0..batch.num_rows());
        let rows = u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
        self.rows = self.rows.saturating_add(rows);
        self.values = self.values.saturating_add(weight.values);
        self.frame_bytes = self.frame_bytes.saturating_add(weight.frame_bytes());
        self.view_bytes = self.view_bytes.saturating_add(weight.view_bytes);
        self.overhead = self.overhead.max(weigher.overhead());
    }
}

/// The most pushes beyond one a batch that a serving end cutting batches to `limits` may have
/// cut the batches weighing `after` into, those a read pushed once a barrier was raised.
///
/// Each piece but a batch's last is closed by a limit its next row would pass:
///
/// - A piece the row limit closes holds that many rows.
/// - A piece a limit of values or of a view's bytes closes holds, with the piece after it, more
///   than the limit, and one the frame limit closes more than the room a frame has for rows,
///   the limit less what a frame takes beside them: such pieces number fewer than twice what
///   the pieces weigh by the limit over it, half frames each.
/// - The serving end weighs a validity bit for each value of a column holding a null anywhere
///   in its batch, which a piece holding none comes without: each value weighs at most a bit
///   more in its frame than the pieces show.
///
/// Where a frame has no room for rows, each row may go alone and no weight bounds the pieces:
/// `usize::MAX`, which no read passes. A push of JSON is not weighed, since the serving end
/// sends each as one frame, and a read in this process is not cut: the clause is handed no
/// limits for it.
fn pieces(limits: &Limits, after: &Weighed) -> usize {
    let halves = |weighed: u64, limit: u64| {
        weighed
            .saturating_mul(2)
            .div_ceil(limit.max(1))
            .saturating_sub(1)
    };
    let room = limits.frame_bytes.saturating_sub(after.overhead);
    if room == 0 {
        return usize::MAX;
    }
    let framed = after.frame_bytes.saturating_add(after.values.div_ceil(8));
    let rows = after.rows / limits.batch_rows.max(1);
    let pieces = rows
        .saturating_add(halves(after.values, limits.batch_values))
        .saturating_add(halves(framed, room))
        .saturating_add(halves(after.view_bytes, limits.frame_bytes));
    usize::try_from(pieces).unwrap_or(usize::MAX)
}

impl Read {
    /// Where the read first answered `barrier`, or a later one, which answers every barrier
    /// before it.
    fn answered(&self, barrier: u64) -> Option<usize> {
        self.sent.iter().position(
            |sent| matches!(sent, Sent::Checkpoint(Some(answered)) if *answered >= barrier),
        )
    }

    /// How many pushes the read sent from `from` on.
    fn pushes(&self, from: usize) -> usize {
        let sent = self.sent.iter().skip(from);
        sent.filter(|sent| **sent == Sent::Push).count()
    }
}

/// How a read answered its barriers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Answered {
    /// It could show nothing: it pushed nothing, or pushed once and answered barrier 1 after.
    Unseen,
    /// Barrier 1 before its last push, and barrier 2 too where it pushed once it could see it.
    InTime,
    /// Barrier 1 with no checkpoint.
    Never,
    /// `barrier` only after the last of the `pushes` sent once it was pending.
    Late { barrier: u64, pushes: usize },
    /// Barrier 2 not at all, though `pushes` were sent once it was pending.
    Unanswered { pushes: usize },
}

/// How `read` answered its barriers, where the pieces its batches were cut into add at most
/// `cut` pushes.
fn answered(read: &Read, cut: usize) -> Answered {
    let Some(last) = read.sent.iter().rposition(|sent| *sent == Sent::Push) else {
        return Answered::Unseen;
    };
    let Some(answer) = read.answered(1) else {
        return Answered::Never;
    };
    if read.pushes(answer) == 0 {
        let pushes = read.pushes(0);
        return if pushes == 1 {
            Answered::Unseen
        } else {
            Answered::Late { barrier: 1, pushes }
        };
    }
    // What came within the slack of barrier 2 the source may have sent before it could see it.
    let slack = BARRIER_SLACK.saturating_add(cut);
    let Some(raised) = read
        .raised
        .filter(|raised| last >= raised.saturating_add(slack))
    else {
        return Answered::InTime;
    };
    let pushes = read.pushes(raised);
    match read.answered(2) {
        Some(at) if read.pushes(at) > 0 => Answered::InTime,
        Some(_) => Answered::Late { barrier: 2, pushes },
        None => Answered::Unanswered { pushes },
    }
}

/// A read of `partition` of `stream` from `cursor` with barrier 1 pending as it starts, held within
/// `budget`, through a one-event channel, which keeps the source at most an event ahead of the
/// clause: barrier 2 is raised once the read has answered barrier 1 and pushed, before anything
/// it sends next is taken.
async fn read(
    source: &dyn Source,
    read: (&StreamSpec, &Partition),
    cursor: Option<Cursor>,
    budget: &Budget,
) -> Result<Read, Violation> {
    let (sink, feed) = partition_channel(NonZeroUsize::MIN);
    feed.request_checkpoint(1);
    let mut seen = Read::default();
    record_watched(
        source,
        (read, cursor),
        (sink, feed),
        budget,
        |event, feed| {
            let sent = match event {
                SourceEvent::Push(push) => {
                    if let (Some(_), Push::Arrow(batch) | Push::Changes(batch)) =
                        (seen.raised, push)
                    {
                        seen.after.add(batch);
                    }
                    Sent::Push
                }
                SourceEvent::Checkpoint { answers, .. } => Sent::Checkpoint(*answers),
                SourceEvent::Replan | SourceEvent::Behind { .. } => return,
            };
            seen.sent.push(sent);
            if seen.raised.is_none() && sent == Sent::Push && seen.answered(1).is_some() {
                feed.request_checkpoint(2);
                seen.raised = Some(seen.sent.len());
            }
        },
    )
    .await?;
    Ok(seen)
}
