//! Where a read of a log may start, and when the log began.
//!
//! A host may report as committed the offset a read started from, once the source accepted the
//! read. So a start is accepted only where the log issued it, in this process or one before it:
//! an offset up to the log's head, or up to what the group has committed. The head must then not
//! fall back from one process to the next, or an offset one process issued would be refused by
//! the next for good.
//!
//! A group kept in a file keeps when it was first kept, in a file beside its offsets, so every
//! process counts the head from the same beginning, and an offset past the head is none the
//! log issued. A group kept nowhere begins with its process: a log of it that does not grow has the
//! head its configuration says, in every process, and one that grows cannot tell an offset an
//! earlier process issued from one nobody did. It accepts such a start, sends nothing before
//! its head is there, and until then ends the read as a failure to try again, so the start is
//! neither refused for good nor one its host is heard for.

#[cfg(test)]
mod tests;

use std::sync::OnceLock;
use std::time::Duration;

use rdlt_connector::prelude::*;
use tokio::time::Instant;

use super::{LogSource, Logged, Offset};
use crate::kept::Kept;

/// The code of an error for a read that starts at an offset the log has yet to reach.
pub(super) const CURSOR_AHEAD: &str = "cursor_ahead";

/// When the process first connected a log source: the logs of every group kept nowhere grow
/// from then, on tokio's clock, so tests on a paused clock see logs grow as they advance it.
///
/// A group kept nowhere is freed with its last source, so nothing of it can say when its logs
/// began; the process can, and no log of it falls back while the process runs.
fn first_connected() -> Instant {
    static FIRST: OnceLock<Instant> = OnceLock::new();
    *FIRST.get_or_init(Instant::now)
}

/// How long ago the logs of `group` began: for a group kept in a file, as long as the file
/// was kept before this process opened it and since; for any other, since the process first
/// connected a log source.
pub(super) fn elapsed(group: &Kept<u64>) -> Duration {
    let (now, first) = (Instant::now(), first_connected());
    match group.kept_for() {
        Some(before) => before.saturating_add(now.saturating_duration_since(group.opened().0)),
        None => now.saturating_duration_since(first),
    }
}

/// The error of a read of partition `id` from `next`, which the log, whose head is `head`, has
/// yet to reach: a failure to try again, sent before anything else.
pub(super) fn ahead(id: &PartitionId, next: u64, head: u64) -> ConnectorError {
    let message = format!("partition {id} holds offsets up to {head}, not yet {next}");
    ConnectorError::new(ConnectorErrorKind::Transient, message).with_code(CURSOR_AHEAD)
}

impl Logged {
    /// The greatest offset a read of the stream was ever sent to resume from, where its head is
    /// `head` and its group has committed `committed`.
    ///
    /// The head, where every process agrees on it: the group is kept in a file, or the log
    /// does not grow. A log that grows from each process's start may have issued any offset.
    fn issued(&self, lasting: bool, head: u64, committed: Option<u64>) -> u64 {
        let agreed = lasting || self.0.per_second == 0;
        let head = if agreed { head } else { u64::MAX };
        head.max(committed.unwrap_or(0))
    }

    /// Accepts a read of partition `id` from `cursor`, where the log's head is `head`: the
    /// offset the read starts at, the cursor's or the earliest the partition still holds.
    ///
    /// # Errors
    ///
    /// A partition the stream never has; the offsets before what a forgetting log's group
    /// committed, which are gone; an offset the log never issued, coded `cursor_unissued`; and
    /// one before the earliest it holds, which was dropped.
    pub(super) fn accept(
        &self,
        source: &LogSource,
        id: &PartitionId,
        cursor: Offset,
        head: u64,
    ) -> Result<u64> {
        self.member(id)?;
        let committed = source.group.position(&self.0.name, id);
        if !self.0.replayable && committed.is_some_and(|committed| cursor.next < committed) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Transient,
                format!("partition {id} no longer holds offsets before {committed:?}"),
            ));
        }
        // A host that read from an offset the log never issued could then report it committed.
        let issued = self.issued(source.lasting, head, committed);
        if cursor.next > issued {
            return Err(ConnectorError::cursor_unissued(format!(
                "partition {id} holds offsets up to {issued}, not {}",
                cursor.next
            )));
        }
        let earliest = self.earliest(head);
        // A read from the start reads from the earliest message the log holds; one that would
        // resume from before it finds its place dropped.
        if cursor.next > 0 && cursor.next < earliest {
            return Err(ConnectorError::retention_lost(format!(
                "partition {id} holds offsets from {earliest}, not {}",
                cursor.next
            )));
        }
        Ok(cursor.next.max(earliest))
    }
}
