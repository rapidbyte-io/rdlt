//! A data call's stream, served: its opening names the call and carries its start, which the
//! connection answers by opening the read, write or read-back it asks for, or with its refusal.

use std::sync::Arc;

use rdlt_wire::bounded::Window;
use rdlt_wire::limits::{Class, Limits};
use rdlt_wire::prost::Message;
use rdlt_wire::stream::{Bounds, Call, End, FrameReader, Kind, Outgoing, Received};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt as _};
use tokio::sync::OwnedSemaphorePermit;

use super::service::Service;
use super::{read, write};
use crate::error::{ConnectorError, ConnectorErrorKind};
use crate::wire::{INVALID_MESSAGE, stream_error, v1, wire_error};

/// A data stream's frames from its host, each body read once it has room in its connection's
/// window, which bounds what the connection holds of frames still arriving.
pub(super) struct Frames<R> {
    reader: FrameReader<R>,
    window: Window,
    /// The room the body arriving holds, until the frame is whole.
    room: Option<OwnedSemaphorePermit>,
    limits: Limits,
    /// The call the stream opened, once it has.
    call: Option<Call>,
}

impl<R: AsyncRead + Unpin> Frames<R> {
    /// The frames `io` carries from a host, held to `limits`, this end's, and to `window`.
    pub(super) fn new(io: R, limits: Limits, window: Window) -> Self {
        let opening = Bounds {
            batch: None,
            json: None,
            ..Call::Read.bounds(End::Host, &limits)
        };
        Self {
            reader: FrameReader::new(io, opening),
            window,
            room: None,
            limits,
            call: None,
        }
    }

    /// The next frame whole, or `None` once the host ended the stream between frames.
    ///
    /// # Errors
    ///
    /// The refusal of a frame beyond this end's limits or malformed, and a transport failure
    /// where the stream ended within a frame or failed.
    pub(super) async fn next(&mut self) -> Result<Option<Received>, ConnectorError> {
        let Some(head) = self
            .reader
            .head()
            .await
            .map_err(|error| stream_error(&error))?
        else {
            return Ok(None);
        };
        if self.room.is_none() {
            let len = usize::try_from(head.len).unwrap_or(usize::MAX);
            let room = self.window.take(len).await;
            self.room = Some(room.map_err(|_| closed_window())?);
        }
        let received = self
            .reader
            .body()
            .await
            .map_err(|error| stream_error(&error));
        self.room = None;
        received.map(Some)
    }

    /// Whether the host sent another frame, `false` once it ended the stream between frames; the
    /// frame stays for [`next`](Self::next) to read whole.
    ///
    /// # Errors
    ///
    /// As [`next`](Self::next) for the frame's head.
    pub(super) async fn more(&mut self) -> Result<bool, ConnectorError> {
        let head = self
            .reader
            .head()
            .await
            .map_err(|error| stream_error(&error))?;
        Ok(head.is_some())
    }

    /// The call the stream opens and its first frame, the call's start; `None` where the host
    /// ended it first.
    ///
    /// # Errors
    ///
    /// As [`next`](Self::next), and an `invalid_message` error where the first frame names no
    /// call.
    pub(super) async fn opening(&mut self) -> Result<Option<(Call, Received)>, ConnectorError> {
        let Some(first) = self.next().await? else {
            return Ok(None);
        };
        let Kind::Message { opens: Some(call) } = first.head.kind else {
            return Err(invalid("a data stream opened by no call"));
        };
        self.reader.set_bounds(call.bounds(End::Host, &self.limits));
        self.call = Some(call);
        Ok(Some((call, first)))
    }

    /// The call's start, the message `opening` holds.
    ///
    /// # Errors
    ///
    /// An `invalid_message` error where it is no start of the call, or would hold more decoded
    /// than its class may.
    pub(super) fn start<M: Message + Default>(
        &self,
        call: Call,
        opening: &Received,
    ) -> Result<M, ConnectorError> {
        let class = match call {
            Call::Read => Class::Cursor,
            Call::Write | Call::ReadPublished => Class::Control,
        };
        let bound = self.limits.decoded(class);
        opening
            .message(call.opening(), bound)
            .map_err(|error| stream_error(&error))
    }

    /// The message a later frame of the call, `received`, holds.
    ///
    /// # Errors
    ///
    /// As [`message`].
    pub(super) fn message<M: Message + Default>(
        &self,
        received: &Received,
    ) -> Result<M, ConnectorError> {
        let Some(call) = self.call else {
            return Err(invalid("a message frame before the call's opening"));
        };
        message(call, &self.limits, received)
    }
}

/// The message `received`, a later frame of `call` from its host, holds, within this end's
/// `limits`.
///
/// # Errors
///
/// An `invalid_message` error where it is no frame the call's host sends, or would hold more
/// decoded than its class may.
pub(super) fn message<M: Message + Default>(
    call: Call,
    limits: &Limits,
    received: &Received,
) -> Result<M, ConnectorError> {
    let Kind::Message { opens: None } = received.head.kind else {
        return Err(invalid("a message frame where the call takes none"));
    };
    let class = match call {
        Call::Write => Class::Schema,
        Call::Read | Call::ReadPublished => Class::Control,
    };
    let Some(form) = call.form(End::Host) else {
        return Err(invalid("a message the call's host sends none of"));
    };
    received
        .message(form, limits.decoded(class))
        .map_err(|error| stream_error(&error))
}

/// The error of a message a host sent that this end cannot read.
pub(super) fn invalid(message: &str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message).with_code(INVALID_MESSAGE)
}

/// The error of a connection whose window closed, which none does while it serves.
fn closed_window() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Internal,
        "the connection's window closed",
    )
}

/// Serves the data call `io` carries, opened as its first frame says, for `service`'s
/// connection, whose window holds what of its frames is still arriving, until the call or the
/// connection ends.
pub(super) async fn serve<IO>(service: Arc<Service>, io: IO)
where
    IO: AsyncRead + AsyncWrite + Send + 'static,
{
    // A connection that ended takes its calls with it, a writer waiting in its destination
    // among them, as a host that left can be told no other way.
    let ended = service.ended.clone();
    tokio::select! {
        biased;
        () = ended.cancelled() => {}
        () = served(service, io) => {}
    }
}

/// Serves the data call `io` carries, as [`serve`] does, to its end.
async fn served<IO>(service: Arc<Service>, io: IO)
where
    IO: AsyncRead + AsyncWrite + Send + 'static,
{
    let (read, mut write) = tokio::io::split(io);
    let mut frames = Frames::new(read, service.limits, service.window.clone());
    // A stream that names no call has no answer to refuse it in: it ends.
    let Ok(Some((call, opening))) = frames.opening().await else {
        return;
    };
    match call {
        Call::Read => read::serve(&service, &opening, frames, &mut write).await,
        Call::Write => write::serve(&service, &opening, frames, &mut write).await,
        Call::ReadPublished => read_back(&service, &opening, frames, &mut write).await,
    }
    // The answers are written: whatever the call left unread goes with the stream.
    write_end(&mut write).await.ok();
}

/// Serves the read-back `opening` asks for.
#[cfg(feature = "certify")]
async fn read_back<R, W>(service: &Service, opening: &Received, frames: Frames<R>, io: &mut W)
where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    super::published::serve(service, opening, frames, io).await;
}

/// Refuses the read-back `opening` asks for: a build without certification's probes serves none.
#[cfg(not(feature = "certify"))]
async fn read_back<R, W>(service: &Service, opening: &Received, frames: Frames<R>, io: &mut W)
where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    let asked = frames
        .start(Call::ReadPublished, opening)
        .and_then(|request| service.probes().read_published(request));
    if let Err(error) = asked {
        send(io, failed(Call::ReadPublished, &error)).await;
    }
}

/// The frame answering `call`'s opening: an empty message naming it.
pub(super) fn opened(call: Call) -> Outgoing {
    Outgoing::new(Kind::Message { opens: Some(call) }, Vec::new())
        .expect("an empty frame fits a head")
}

/// The frame carrying `error`, which ends `call`: a read's or a read-back's, or a write's.
pub(super) fn failed(call: Call, error: &ConnectorError) -> Outgoing {
    let error = Some(wire_error(error));
    let framed = match call {
        Call::Read | Call::ReadPublished => Outgoing::message(
            None,
            &v1::ReadFrame {
                frame: error.map(v1::read_frame::Frame::Error),
            },
        ),
        Call::Write => Outgoing::message(
            None,
            &v1::WriteAck {
                ack: error.map(v1::write_ack::Ack::Error),
            },
        ),
    };
    framed.expect("an error within the control string limit fits a head")
}

/// Writes `frame` to `io`; whether the host is still there to take it.
pub(super) async fn send<W: AsyncWrite + Unpin>(io: &mut W, mut frame: Outgoing) -> bool {
    frame.write(io).await.is_ok()
}

/// Ends what this end sends on `io`.
async fn write_end<W: AsyncWrite + Unpin>(io: &mut W) -> std::io::Result<()> {
    io.shutdown().await
}
