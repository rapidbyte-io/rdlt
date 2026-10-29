//! A data call's stream on a raw client, for clients that speak the protocol themselves:
//! opened as a host opens it, or with an opening a host would never send.

use rdlt_connector::wire::{stream_error, v1};
use rdlt_connector::{ConnectorError, ConnectorErrorKind};
use rdlt_wire::Limits;
use rdlt_wire::prost::Message;
use rdlt_wire::prost::bytes::Bytes;
use rdlt_wire::stream::{Call, End, FrameReader, Kind, Outgoing, Received};
use tokio::io::{ReadHalf, WriteHalf};

use super::Client;
use crate::network::Stream;

/// A data call's stream on a raw client, its opening sent.
pub struct RawCall {
    reader: FrameReader<ReadHalf<Box<dyn Stream>>>,
    writer: WriteHalf<Box<dyn Stream>>,
    call: Call,
    limits: Limits,
}

impl std::fmt::Debug for RawCall {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RawCall")
            .field("call", &self.call)
            .finish_non_exhaustive()
    }
}

/// Bytes: what a message frame of a connector's may hold decoded beside twice its length.
const FORMS: usize = 4_096;

/// The error of a connector that answered otherwise than the protocol says.
fn answered(what: &str) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Internal,
        format!("the connector answered {what}"),
    )
}

impl RawCall {
    /// Opens `call` on `client`'s connection with `start`, as a host opens it, reading within
    /// `limits`: the stream once the connector opened the call, or the error it refused it with.
    ///
    /// # Errors
    ///
    /// Where no stream could be made, or the connector answered otherwise than the protocol says.
    pub async fn open(
        client: &Client,
        call: Call,
        start: &impl Message,
        limits: &Limits,
    ) -> Result<Result<Self, ConnectorError>, ConnectorError> {
        Self::opened(client, call, Bytes::from(start.encode_to_vec()), limits).await
    }

    /// Opens `call` as [`open`](Self::open) does, its opening's body `body` whatever it holds.
    ///
    /// # Errors
    ///
    /// As [`open`](Self::open).
    pub async fn opened(
        client: &Client,
        call: Call,
        body: Bytes,
        limits: &Limits,
    ) -> Result<Result<Self, ConnectorError>, ConnectorError> {
        let stream = client.stream().await?;
        let (read, writer) = tokio::io::split(stream);
        let mut streamed = Self {
            reader: FrameReader::new(read, call.answering(limits)),
            writer,
            call,
            limits: *limits,
        };
        let opening = Outgoing::new(Kind::Message { opens: Some(call) }, vec![body])
            .map_err(|error| answered(&error.to_string()))?;
        if !streamed.send(opening).await {
            return Err(answered("by closing the stream before its opening"));
        }
        let Some(first) = streamed.next().await? else {
            return Err(answered("by ending the call before answering it"));
        };
        match first.head.kind {
            Kind::Message {
                opens: Some(opened),
            } if opened == call && first.head.len == 0 => {
                streamed
                    .reader
                    .set_bounds(call.bounds(End::Connector, limits));
                Ok(Ok(streamed))
            }
            Kind::Message { opens: None } => match streamed.error(&first)? {
                Some(refused) => Ok(Err(refused)),
                None => Err(answered("its opening with a message that is no refusal")),
            },
            _ => Err(answered("its opening with a frame that is no answer")),
        }
    }

    /// Sends `frame`; whether the connector took it.
    pub async fn send(&mut self, mut frame: Outgoing) -> bool {
        frame.write(&mut self.writer).await.is_ok()
    }

    /// The connector's next frame, or `None` once it ended the stream.
    ///
    /// # Errors
    ///
    /// The refusal of a frame beyond this end's limits or malformed, or a failure of the stream.
    pub async fn next(&mut self) -> Result<Option<Received>, ConnectorError> {
        self.reader
            .next()
            .await
            .map_err(|error| stream_error(&error))
    }

    /// The message a message frame of the connector's holds: a read's or a read-back's frame.
    ///
    /// # Errors
    ///
    /// Where it does not decode as one.
    pub fn read_frame(&self, frame: &Received) -> Result<v1::ReadFrame, ConnectorError> {
        let form = self.answers()?;
        let bound = frame
            .body
            .bytes
            .len()
            .saturating_mul(2)
            .saturating_add(FORMS);
        frame
            .message(form, bound)
            .map_err(|error| stream_error(&error))
    }

    /// The answer a message frame of a write's connector holds.
    ///
    /// # Errors
    ///
    /// Where it does not decode as one.
    pub fn write_ack(&self, frame: &Received) -> Result<v1::WriteAck, ConnectorError> {
        let form = Call::Write
            .form(End::Connector)
            .ok_or_else(|| answered("on a call it does not answer"))?;
        let bound = frame
            .body
            .bytes
            .len()
            .saturating_mul(2)
            .saturating_add(FORMS);
        frame
            .message(form, bound)
            .map_err(|error| stream_error(&error))
    }

    /// The error a message frame of the connector's carries, where it carries one.
    ///
    /// # Errors
    ///
    /// Where it does not decode as the call's answer, or its error does not decode.
    pub fn error(&self, frame: &Received) -> Result<Option<ConnectorError>, ConnectorError> {
        let form = self.answers()?;
        let bound = frame
            .body
            .bytes
            .len()
            .saturating_mul(2)
            .saturating_add(FORMS);
        let carried = match self.call {
            Call::Read | Call::ReadPublished => {
                let read: v1::ReadFrame = frame
                    .message(form, bound)
                    .map_err(|error| stream_error(&error))?;
                match read.frame {
                    Some(v1::read_frame::Frame::Error(error)) => Some(error),
                    _ => None,
                }
            }
            Call::Write => {
                let ack: v1::WriteAck = frame
                    .message(form, bound)
                    .map_err(|error| stream_error(&error))?;
                match ack.ack {
                    Some(v1::write_ack::Ack::Error(error)) => Some(error),
                    _ => None,
                }
            }
        };
        carried
            .map(|error| {
                ConnectorError::try_from(error)
                    .map_err(|invalid| answered(&format!("with a malformed error: {invalid}")))
            })
            .transpose()
    }

    /// The error the call ends with, its answers read until one carries it; `None` where the
    /// connector answered with anything else first, stats or a frame of data.
    ///
    /// # Errors
    ///
    /// Where the stream fails, or the connector ends it, without an error.
    pub async fn refusal(&mut self) -> Result<Option<ConnectorError>, ConnectorError> {
        let Some(frame) = self.next().await? else {
            return Err(answered("by ending the call without an error"));
        };
        match frame.head.kind {
            Kind::Message { opens: None } => self.error(&frame),
            _ => Ok(None),
        }
    }

    /// The form of each answer of the call's connector.
    fn answers(&self) -> Result<&'static rdlt_wire::scan::Form, ConnectorError> {
        self.call
            .form(End::Connector)
            .ok_or_else(|| answered("on a call it does not answer"))
    }

    /// The limits this end reads within.
    pub fn limits(&self) -> &Limits {
        &self.limits
    }
}
