//! The host's transport, which drops status details a connector sent that do not decode.
//!
//! tonic decodes a status's details from base64 as it reads an answer's headers or trailers, and
//! panics on a value that is not base64; what a connector sends is untrusted. The transport drops
//! such a value before tonic reads it, so the status reads as one without details: a failure of
//! the transport.

#[cfg(test)]
mod tests;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use base64::Engine as _;
use base64::alphabet;
use base64::engine::DecodePaddingMode;
use base64::engine::general_purpose::{GeneralPurpose, GeneralPurposeConfig};
use http::HeaderMap;
use hyper::body::{Body, Bytes, Frame, SizeHint};
use tonic::transport::Channel;

/// The header that carries a status's details.
const DETAILS: &str = "grpc-status-details-bin";

/// The base64 tonic decodes details with.
const BASE64: GeneralPurpose = GeneralPurpose::new(
    &alphabet::STANDARD,
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

/// A channel whose answers carry only status details that decode.
#[derive(Clone, Debug)]
pub struct Checked(Channel);

impl Checked {
    pub(crate) fn new(channel: Channel) -> Self {
        Self(channel)
    }
}

type Answer = http::Response<tonic::body::Body>;

impl tower::Service<http::Request<tonic::body::Body>> for Checked {
    type Response = Answer;
    type Error = tonic::transport::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Answer, Self::Error>> + Send>>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.poll_ready(context)
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let answer = self.0.call(request);
        Box::pin(async move {
            let (mut parts, body) = answer.await?.into_parts();
            check(&mut parts.headers);
            let body = tonic::body::Body::new(CheckedBody(body));
            Ok(http::Response::from_parts(parts, body))
        })
    }
}

/// An answer's body, whose trailers carry only status details that decode.
struct CheckedBody(tonic::body::Body);

impl Body for CheckedBody {
    type Data = Bytes;
    type Error = tonic::Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        Pin::new(&mut self.0).poll_frame(context).map(|polled| {
            polled.map(|frame| {
                frame.map(|mut frame| {
                    if let Some(trailers) = frame.trailers_mut() {
                        check(trailers);
                    }
                    frame
                })
            })
        })
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.0.size_hint()
    }
}

/// Drops `headers`' status details where any value of them does not decode.
fn check(headers: &mut HeaderMap) {
    let undecodable = headers
        .get_all(DETAILS)
        .iter()
        .any(|value| BASE64.decode(value.as_bytes()).is_err());
    if undecodable {
        headers.remove(DETAILS);
    }
}
