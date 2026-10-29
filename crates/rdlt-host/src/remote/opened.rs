//! A data call's stream, opened: passed to the connector over its opener, or carried by a `Data`
//! call of the control connection; its opening sent, and its first answer read within the call's
//! deadline for it.

use std::sync::Arc;

use rdlt_connector::wire::{stream_error, v1};
use rdlt_connector::{ConnectorError, ConnectorErrorKind};
use rdlt_wire::prost::Message;
use rdlt_wire::stream::{Call, End, FrameReader, Kind, Outgoing, pipe};
use tokio::io::{ReadHalf, WriteHalf};
use tonic::transport::Channel;

use super::source::invalid;
use super::{Connection, exceeded, lost_because, lost_error};
use crate::network::Stream;
use crate::opener::{Opener, Opening};

/// How a connection's data calls reach its connector.
#[derive(Clone)]
pub(crate) enum Carrier {
    /// Each a stream an opener passes the connector.
    Opened(Arc<dyn Opener>),
    /// Each a `Data` call of the control connection.
    Called(Channel),
}

impl std::fmt::Debug for Carrier {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Opened(_) => "Opened",
            Self::Called(_) => "Called",
        })
    }
}

/// A data call's stream, opened: what reads the connector's frames, and what the host writes.
pub(crate) struct Opened {
    pub(crate) reader: FrameReader<ReadHalf<Box<dyn Stream>>>,
    pub(crate) writer: WriteHalf<Box<dyn Stream>>,
}

impl Connection {
    /// Opens a stream for `call`, sends its opening carrying `start`, and reads the connector's
    /// first answer, all within the call's deadline for it: the stream once the connector opened
    /// the call.
    ///
    /// A read's opening has the connect deadline, as the start of a read; a write's, the
    /// write-ack deadline, as the connector may open its writer only once a schema change or a
    /// commit of the session lets it.
    ///
    /// # Errors
    ///
    /// The error the connector refused the call with; a transient error where no stream could be
    /// made or the connector was lost, coded `deadline_exceeded` where it did not answer within
    /// the deadline; an internal error where it answered otherwise than the protocol says.
    pub(crate) async fn open_call(
        &self,
        call: Call,
        start: &impl Message,
    ) -> Result<Opened, ConnectorError> {
        let deadline = match call {
            Call::Read | Call::ReadPublished => self.options.deadlines.connect,
            Call::Write => self.options.deadlines.write_ack,
        };
        let opening = async {
            let stream = self.carrier.stream().await?;
            let (read, mut writer) = tokio::io::split(stream);
            let mut opening = Outgoing::message(Some(call), start).map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::Internal, error.to_string())
            })?;
            opening
                .write(&mut writer)
                .await
                .map_err(|error| lost_because(format!("opening the call failed: {error}")))?;
            // The first answer opens the call or refuses it: no more than a control message.
            let limits = &self.options.limits;
            let bounds = call.bounds(End::Connector, limits);
            let mut reader = FrameReader::new(read, call.answering(limits));
            let answered = reader.next().await.map_err(|error| stream_error(&error))?;
            let Some(first) = answered else {
                return Err(lost_because(
                    "the connector ended the call unanswered".to_owned(),
                ));
            };
            match first.head.kind {
                Kind::Message {
                    opens: Some(opened),
                } if opened == call && first.head.len == 0 => {
                    reader.set_bounds(bounds);
                    Ok(Opened { reader, writer })
                }
                Kind::Message { opens: None } => Err(refused(call, &first)),
                _ => Err(invalid(&rdlt_connector::wire::Invalid::Missing(
                    "the call's opening answer",
                ))),
            }
        };
        tokio::select! {
            biased;
            () = self.lost.cancelled() => Err(lost_error()),
            opened = tokio::time::timeout(deadline, opening) => {
                opened.map_err(|_| exceeded("opening the call", deadline))?
            }
        }
    }
}

impl Carrier {
    /// A new stream to the connector, for a data call.
    pub(crate) async fn stream(&self) -> Result<Box<dyn Stream>, ConnectorError> {
        match self {
            Self::Opened(opener) => opener.open(Opening::Data).await.map_err(|error| {
                // A process out of descriptors may have one for the next call.
                ConnectorError::new(
                    ConnectorErrorKind::Transient,
                    format!("a stream for the call could not be made: {error}"),
                )
            }),
            Self::Called(channel) => called(channel.clone()).await,
        }
    }
}

/// The error a connector refused `call` with, the answer `first` carries.
fn refused(call: Call, first: &rdlt_wire::stream::Received) -> ConnectorError {
    let form = call
        .form(End::Connector)
        .expect("a connector answers every call");
    let bound = first
        .body
        .bytes
        .len()
        .saturating_mul(2)
        .saturating_add(4_096);
    let error = match call {
        Call::Read | Call::ReadPublished => first
            .message::<v1::ReadFrame>(form, bound)
            .ok()
            .and_then(|frame| match frame.frame {
                Some(v1::read_frame::Frame::Error(error)) => Some(error),
                _ => None,
            }),
        Call::Write => first
            .message::<v1::WriteAck>(form, bound)
            .ok()
            .and_then(|ack| match ack.ack {
                Some(v1::write_ack::Ack::Error(error)) => Some(error),
                _ => None,
            }),
    };
    match error.map(ConnectorError::try_from) {
        Some(Ok(error)) => error,
        Some(Err(error)) => invalid(&error),
        None => invalid(&rdlt_connector::wire::Invalid::Missing(
            "the call's opening answer",
        )),
    }
}

/// A `Data` call of the control connection over `channel`, as a stream: what it writes goes in
/// its request's body, and what it reads comes from its answer's.
async fn called(mut channel: Channel) -> Result<Box<dyn Stream>, ConnectorError> {
    use tonic::codegen::http;
    let (pipe, sent) = pipe();
    let mut request = http::Request::new(tonic::body::Body::new(sent));
    *request.method_mut() = http::Method::POST;
    *request.uri_mut() = http::Uri::from_static(rdlt_wire::stream::DATA);
    *request.version_mut() = http::Version::HTTP_2;
    let failed = |error: &dyn std::fmt::Display| lost_because(format!("the call failed: {error}"));
    let ready = tower::ServiceExt::ready(&mut channel)
        .await
        .map_err(|error| failed(&error))?;
    let answer = tower::Service::call(ready, request)
        .await
        .map_err(|error| failed(&error))?;
    if answer.status() != http::StatusCode::OK {
        return Err(failed(&answer.status()));
    }
    Ok(Box::new(pipe.reading(answer.into_body())))
}
