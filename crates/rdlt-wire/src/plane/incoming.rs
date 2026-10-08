//! The messages of a call's body, each decoded from the bytes its [`Bounded`] body passed on.

use std::marker::PhantomData;
use std::pin::Pin;

use bytes::Bytes;
use http_body::Body as _;
use prost::Message;
use tonic::codegen::http::{self, HeaderMap, StatusCode};
use tonic::{Code, Status};

use super::{PREFIX, status};
use crate::bounded::Bounded;

/// Which end reads the body, and so how it ends.
#[derive(Clone, Copy, Debug)]
enum Reading {
    /// A request's: its trailers say nothing, and its client's cancelling it ends it.
    Request,
    /// An answer's, of its HTTP status: its trailers say how it ended.
    Answer(StatusCode),
    /// An answer whose headers said it succeeded: its end says nothing more.
    Succeeded,
}

/// What reading the body has come to.
#[derive(Clone, Copy, Debug)]
enum State {
    Reading,
    Failed,
    Ended,
}

/// A call's messages of `M`, read from its body.
///
/// Each message is decoded from the whole, scanned and, where the body is charged, charged
/// bytes the body passes on: a `bytes` field of it is a slice of them, never a copy.
pub struct Incoming<M> {
    body: Bounded,
    reading: Reading,
    trailers: Option<HeaderMap>,
    state: State,
    message: PhantomData<fn() -> M>,
}

impl<M: Message + Default> Incoming<M> {
    /// The messages of a request's `body`.
    pub fn request(body: Bounded) -> Self {
        Self::reading(body, Reading::Request)
    }

    /// The messages of `answer`.
    ///
    /// # Errors
    ///
    /// The status its headers carry, where they say the call failed before any message, and
    /// `Unimplemented` where they name a compression.
    ///
    /// # Panics
    ///
    /// Where its headers carry status details that are not base64, as tonic's reading of a
    /// status does: whoever takes an answer from an untrusted peer drops such details first.
    pub fn answer(answer: http::Response<Bounded>) -> Result<Self, Status> {
        let (parts, body) = answer.into_parts();
        status::uncompressed(&parts.headers)?;
        let reading = match Status::from_header_map(&parts.headers) {
            Some(failed) if failed.code() != Code::Ok => return Err(failed),
            Some(_) => Reading::Succeeded,
            None => Reading::Answer(parts.status),
        };
        Ok(Self::reading(body, reading))
    }

    fn reading(body: Bounded, reading: Reading) -> Self {
        Self {
            body,
            reading,
            trailers: None,
            state: State::Reading,
            message: PhantomData,
        }
    }

    /// The next message, or `None` once the call has ended well; after a failure, `None`.
    ///
    /// # Errors
    ///
    /// The status the call failed with: one its peer sent, one its body failed with, or one of
    /// a message that does not decode.
    ///
    /// # Panics
    ///
    /// Where an answer's trailers carry status details that are not base64, as
    /// [`answer`](Self::answer) says of its headers.
    pub async fn message(&mut self) -> Result<Option<M>, Status> {
        while let State::Reading = self.state {
            let frame =
                std::future::poll_fn(|context| Pin::new(&mut self.body).poll_frame(context)).await;
            let failed = match frame {
                Some(Ok(frame)) => match frame.into_data() {
                    Ok(message) => match decoded(&message) {
                        Ok(message) => return Ok(Some(message)),
                        Err(status) => Some(status),
                    },
                    Err(frame) => {
                        self.trailers = frame.into_trailers().ok();
                        continue;
                    }
                },
                // A request its client cancelled ends where it was cut.
                Some(Err(status)) if self.cancelled(&status) => None,
                Some(Err(status)) => Some(status),
                None => self.ending(),
            };
            let Some(failed) = failed else {
                self.state = State::Ended;
                break;
            };
            self.state = State::Failed;
            return Err(failed);
        }
        Ok(None)
    }

    /// A unary request's message, its body read to its end.
    ///
    /// # Errors
    ///
    /// `Internal` where the request has no message, and the status reading it failed with.
    pub async fn unary(mut self) -> Result<M, Status> {
        self.message().await?;
        Err(Status::unimplemented("a unary request"))
    }

    /// Releases the charge of the message passed on last: it has been decoded.
    pub fn release(&self) {
        self.body.release();
    }

    /// Whether `status`, the error the body failed with, says its request's client cancelled it.
    fn cancelled(&self, status: &Status) -> bool {
        matches!(self.reading, Reading::Request) && status.code() == Code::Cancelled
    }

    /// How the call ended, once its body has: a failure, or `None`.
    fn ending(&mut self) -> Option<Status> {
        match self.reading {
            Reading::Answer(code) => status::ended(self.trailers.take().as_ref(), code),
            Reading::Request | Reading::Succeeded => None,
        }
    }
}

impl<M> std::fmt::Debug for Incoming<M> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Incoming")
            .field("body", &self.body)
            .field("reading", &self.reading)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

/// The message of `message`'s bytes, prefix and all, its `bytes` fields slices of them.
pub(super) fn decoded<M: Message + Default>(message: &Bytes) -> Result<M, Status> {
    let flag = message.first().copied().unwrap_or_default();
    status::flag(flag)?;
    M::decode(message.slice(PREFIX.min(message.len())..))
        .map_err(|error| Status::internal(format!("a message that does not decode: {error}")))
}
