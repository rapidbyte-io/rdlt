//! What a served read's host may later report committed, remembered as the read writes its
//! frames: see [`Sent`](crate::source::Sent).

use std::sync::Arc;

use crate::cursor::Cursor;
use crate::id::{PartitionId, StreamName};

/// What a served read reads: its stream, its partition, and the cursor it is asked to start from.
#[derive(Clone, Debug)]
pub(super) struct Read {
    pub(super) stream: StreamName,
    pub(super) partition: PartitionId,
    pub(super) start: Option<Cursor>,
}

/// What a frame written tells of the read.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum Note {
    /// A checkpoint at this cursor.
    Checkpoint(Cursor),
    /// Data of the partition, or its end.
    Data,
    /// Nothing the host may report.
    Nothing,
}

/// What a read's host may report, remembered as its frames are written: each checkpoint, and
/// where the read started once its source has accepted it.
pub(super) struct Noting {
    served: Arc<super::Served>,
    host: Option<Arc<str>>,
    read: Read,
    /// Whether the source has yet to send anything of the partition, or end the read.
    opening: bool,
}

impl Noting {
    /// The frames of `read`, written to `host`.
    pub(super) fn new(served: Arc<super::Served>, host: Option<Arc<str>>, read: Read) -> Self {
        Self {
            served,
            host,
            read,
            opening: true,
        }
    }

    /// Remembers that the read started from `start`.
    fn started(&self, start: &Cursor) {
        let (stream, partition) = (&self.read.stream, &self.read.partition);
        let sent = &self.served.sent;
        sent.started(self.host.as_deref(), stream, partition, start);
    }

    /// Remembers what a frame `note` tells of lets the host report.
    ///
    /// The source accepts the read by sending data or a checkpoint, or by ending it cleanly: a
    /// read it refuses fails before any of them, and leaves nothing to report. A checkpoint
    /// before any data is the source saying where it started, in place of where it was asked to.
    pub(super) fn note(&mut self, note: &Note) {
        match note {
            Note::Checkpoint(cursor) => {
                let (stream, partition) = (&self.read.stream, &self.read.partition);
                let sent = &self.served.sent;
                sent.note(self.host.as_deref(), stream, partition, cursor);
                if std::mem::take(&mut self.opening) {
                    self.started(cursor);
                }
            }
            Note::Data => {
                if std::mem::take(&mut self.opening)
                    && let Some(start) = &self.read.start
                {
                    self.started(start);
                }
            }
            Note::Nothing => {}
        }
    }
}
