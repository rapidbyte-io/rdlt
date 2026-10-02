//! A served read's frames, with what its host may later report committed remembered as they
//! pass: see [`Sent`](crate::source::Sent).

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use rdlt_wire::tonic::Status;
use rdlt_wire::tonic::codegen::tokio_stream::Stream;

use super::service::Answer;
use crate::cursor::Cursor;
use crate::id::{PartitionId, StreamName};
use crate::wire::v1;

/// What a served read reads: its stream, its partition, and the cursor it is asked to start from.
#[derive(Clone, Debug)]
pub(super) struct Read {
    pub(super) stream: StreamName,
    pub(super) partition: PartitionId,
    pub(super) start: Option<Cursor>,
}

/// A read's frames, with what its host may report remembered as they pass: each checkpoint, and
/// where the read started once its source has accepted it.
pub(super) struct Noted {
    frames: Answer<v1::ReadFrame>,
    served: Arc<super::Served>,
    host: Option<Arc<str>>,
    read: Read,
    /// Whether the source has yet to send anything of the partition, or end the read.
    opening: bool,
}

impl Noted {
    /// `frames` of `read`, served to `host`.
    pub(super) fn new(
        frames: Answer<v1::ReadFrame>,
        served: Arc<super::Served>,
        host: Option<Arc<str>>,
        read: Read,
    ) -> Self {
        Self {
            frames,
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

    /// Remembers what `frame` lets the host report.
    ///
    /// The source accepts the read by sending data or a checkpoint, or by ending it cleanly: a
    /// read it refuses fails before any of them, and leaves nothing to report. A checkpoint
    /// before any data is the source saying where it started, in place of where it was asked to.
    fn note(&mut self, frame: &v1::ReadFrame) {
        use v1::read_frame::Frame;
        match &frame.frame {
            Some(Frame::Checkpoint(checkpoint)) => {
                let Some(Ok(cursor)) = checkpoint.cursor.clone().map(Cursor::try_from) else {
                    return;
                };
                let (stream, partition) = (&self.read.stream, &self.read.partition);
                let sent = &self.served.sent;
                sent.note(self.host.as_deref(), stream, partition, &cursor);
                if std::mem::take(&mut self.opening) {
                    self.started(&cursor);
                }
            }
            Some(Frame::Schema(_) | Frame::Batch(_) | Frame::Json(_) | Frame::Done(_)) => {
                if std::mem::take(&mut self.opening)
                    && let Some(start) = &self.read.start
                {
                    self.started(start);
                }
            }
            _ => {}
        }
    }
}

impl Stream for Noted {
    type Item = Result<v1::ReadFrame, Status>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let frame = self.frames.as_mut().poll_next(context);
        if let Poll::Ready(Some(Ok(sent))) = &frame {
            self.note(sent);
        }
        frame
    }
}
