//! The connector's frames of a read, on a reader of their own: each reserved from what the
//! engine's pushes may take, once every schema and dictionary before it is decoded where it is a
//! batch, before its body is read into place.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rdlt_connector::cost::json_charge;
use rdlt_connector::wire::{Invalid, stream_error, v1};
use rdlt_connector::{Permit, Reserving};
use rdlt_wire::limits::MESSAGE_ROOM;
use rdlt_wire::stream::{Call, End, FrameReader, Head, Kind, Received};
use tokio::io::AsyncRead;
use tokio::sync::{Notify, mpsc};

use crate::remote::source::invalid;

/// Bytes: what a message frame's decoding may hold beside twice its length, the fixed sizes of the
/// messages a read's frames hold.
const MESSAGE_FORMS: u64 = 4_096;

/// Bytes: what a frame of `head` is reserved before its body is read, where a batch's push holds
/// `beside` bytes beside its frame: what the read's decoder keeps, and the batch's arrays.
pub(crate) fn reservation(head: &Head, beside: u64) -> u64 {
    let len = u64::from(head.len);
    match head.kind {
        Kind::Batch {
            dictionary: false, ..
        } => len.saturating_add(MESSAGE_ROOM).saturating_add(beside),
        Kind::Batch {
            dictionary: true, ..
        } => len.saturating_add(MESSAGE_ROOM),
        Kind::Json => json_charge(len),
        Kind::Message { .. } => len.saturating_mul(2).saturating_add(MESSAGE_FORMS),
    }
}

/// What a batch's push holds beside its frame, as the reader reserves batches by it, and the
/// schemas and dictionaries the reader passed on that the decoder has yet to take.
#[derive(Default)]
pub(super) struct Keeping {
    beside: AtomicU64,
    unsettled: AtomicU64,
    settled: Notify,
}

impl Keeping {
    /// The decoder took a schema or dictionary, and a batch's push holds `beside` bytes beside
    /// its frame since.
    pub(super) fn settled(&self, beside: u64) {
        self.beside.store(beside, Ordering::SeqCst);
        self.unsettled.fetch_sub(1, Ordering::SeqCst);
        self.settled.notify_one();
    }

    /// What a batch's push holds beside its frame, once the decoder has taken every schema and
    /// dictionary passed on.
    async fn beside(&self) -> u64 {
        loop {
            let settled = self.settled.notified();
            if self.unsettled.load(Ordering::SeqCst) == 0 {
                return self.beside.load(Ordering::SeqCst);
            }
            settled.await;
        }
    }
}

/// What a frame read holds.
pub(super) enum Payload {
    /// A batch or a push of JSON, as it arrived.
    Data(Received),
    /// A message, decoded.
    Message(v1::ReadFrame),
}

/// A frame read, the reservation it holds, and whether the decoder's taking it changes what it
/// keeps.
pub(super) struct Arrived {
    /// Declared first: dropped, it is freed before its reservation is released.
    pub(super) payload: Payload,
    pub(super) reserved: Option<Permit>,
    pub(super) settles: bool,
}

/// The connector's frames, each reserved before its body is read.
pub(super) struct Frames<R> {
    /// Declared first: what has arrived of a body is freed before its reservation is released.
    pub(super) reader: FrameReader<R>,
    /// The reservation of the body arriving.
    pub(super) reserved: Option<Permit>,
    pub(super) reserving: Option<Reserving>,
    pub(super) keeping: Arc<Keeping>,
}

impl<R: AsyncRead + Unpin> Frames<R> {
    /// The next frame whole and its reservation, or `None` once the connector ended the stream
    /// between frames.
    async fn next(&mut self) -> rdlt_connector::Result<Option<Arrived>> {
        let Some(head) = self
            .reader
            .head()
            .await
            .map_err(|error| stream_error(&error))?
        else {
            return Ok(None);
        };
        if self.reserved.is_none()
            && let Some(reserving) = &self.reserving
        {
            let beside = match head.kind {
                Kind::Batch {
                    dictionary: false, ..
                } => self.keeping.beside().await,
                _ => 0,
            };
            self.reserved = Some(reserving.reserve(reservation(&head, beside)).await?);
        }
        let frame = self
            .reader
            .body()
            .await
            .map_err(|error| stream_error(&error));
        let payload = frame.and_then(|frame| match frame.head.kind {
            Kind::Message { opens: None } => message(&frame).map(Payload::Message),
            Kind::Message { opens: Some(_) } => Err(invalid(&Invalid::Unknown("read frame"))),
            Kind::Batch { .. } | Kind::Json => Ok(Payload::Data(frame)),
        });
        let reserved = self.reserved.take();
        let payload = payload?;
        // The decoder's taking a schema or a dictionary is waited for before a batch is reserved.
        let settles = match &payload {
            Payload::Data(frame) => matches!(
                frame.head.kind,
                Kind::Batch {
                    dictionary: true,
                    ..
                }
            ),
            Payload::Message(message) => {
                matches!(message.frame, Some(v1::read_frame::Frame::Schema(_)))
            }
        };
        if settles {
            self.keeping.unsettled.fetch_add(1, Ordering::SeqCst);
        }
        Ok(Some(Arrived {
            payload,
            reserved,
            settles,
        }))
    }
}

/// The message a read's message `frame` holds, scanned within what it was reserved.
pub(super) fn message(frame: &Received) -> rdlt_connector::Result<v1::ReadFrame> {
    let form = Call::Read
        .form(End::Connector)
        .expect("a read's connector sends messages");
    let bound = usize::try_from(reservation(&frame.head, 0)).unwrap_or(usize::MAX);
    frame
        .message(form, bound)
        .map_err(|error| stream_error(&error))
}

/// Reads the connector's frames, each once its consumer has taken the frame before, until the
/// stream ends or fails.
pub(super) async fn read<R: AsyncRead + Unpin>(
    mut frames: Frames<R>,
    arrived: mpsc::Sender<rdlt_connector::Result<Arrived>>,
) {
    loop {
        // One frame beyond its consumer's: room first, then the frame.
        let Ok(slot) = arrived.reserve().await else {
            return;
        };
        match frames.next().await {
            Ok(Some(frame)) => slot.send(Ok(frame)),
            Ok(None) => return,
            Err(error) => {
                slot.send(Err(error));
                return;
            }
        }
    }
}

/// The read's reader, aborted once dropped: a read the engine leaves ends its reader with it.
pub(super) struct Reading(pub(super) tokio::task::JoinHandle<()>);

impl Drop for Reading {
    fn drop(&mut self) {
        self.0.abort();
    }
}
