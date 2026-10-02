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

/// What a served read reads: its stream, its partition, and the cursor it starts from.
#[derive(Clone, Debug)]
pub(super) struct Read {
    pub(super) stream: StreamName,
    pub(super) partition: PartitionId,
    pub(super) start: Option<Cursor>,
}

/// A read's frames, each checkpoint among them remembered for the read's host as it passes.
pub(super) struct Noted {
    frames: Answer<v1::ReadFrame>,
    served: Arc<super::Served>,
    host: Option<Arc<str>>,
    read: Read,
}

impl Noted {
    /// `frames` of `read`, served to `host`; the cursor the read starts from is remembered at
    /// once.
    pub(super) fn new(
        frames: Answer<v1::ReadFrame>,
        served: Arc<super::Served>,
        host: Option<Arc<str>>,
        read: Read,
    ) -> Self {
        if let Some(start) = &read.start {
            served
                .sent
                .note(host.as_deref(), &read.stream, &read.partition, start);
        }
        Self {
            frames,
            served,
            host,
            read,
        }
    }
}

impl Stream for Noted {
    type Item = Result<v1::ReadFrame, Status>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let frame = self.frames.as_mut().poll_next(context);
        if let Poll::Ready(Some(Ok(v1::ReadFrame {
            frame: Some(v1::read_frame::Frame::Checkpoint(checkpoint)),
        }))) = &frame
            && let Some(Ok(cursor)) = checkpoint.cursor.clone().map(Cursor::try_from)
        {
            let (stream, partition) = (&self.read.stream, &self.read.partition);
            self.served
                .sent
                .note(self.host.as_deref(), stream, partition, &cursor);
        }
        frame
    }
}
