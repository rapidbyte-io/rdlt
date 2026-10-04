//! The client an S3 log's requests go through, telling a refusal no retry changes from a failure
//! another attempt may not meet: a certificate refused, or a request the store answers as
//! malformed, which the log reports at once.

use std::error::Error;
use std::io;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt as _;
use http::StatusCode;
use object_store::client::{HttpError, HttpErrorKind, HttpRequest, HttpResponse, HttpService};
use rdlt_engine::StoreRefusal;

use crate::limits::REFUSAL_BYTES;

/// A client whose refusals no retry changes are marked as such.
#[derive(Debug)]
pub(crate) struct Refusing(pub(crate) reqwest::Client);

#[async_trait]
impl HttpService for Refusing {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        match HttpService::call(&self.0, request).await {
            Err(error) if tls(&error) => {
                let refusal = StoreRefusal::new("TLS refused the store", Some(Box::new(error)));
                Err(HttpError::new(HttpErrorKind::Connect, refusal))
            }
            Err(error) => Err(error),
            Ok(response) if refused(response.status()) => malformed(response).await,
            Ok(response) => Ok(response),
        }
    }
}

/// Whether TLS refused the connection: a certificate no trusted root signs or naming another
/// host, or a handshake the store would not complete.
fn tls(error: &HttpError) -> bool {
    among::<rustls::Error>(error)
}

/// Whether an `E` is among `error`'s causes, or those an I/O error among them holds.
fn among<E: Error + 'static>(error: &(dyn Error + 'static)) -> bool {
    if error.is::<E>() {
        return true;
    }
    let held = error
        .downcast_ref::<io::Error>()
        .and_then(io::Error::get_ref)
        .is_some_and(|inner| among::<E>(inner));
    held || error.source().is_some_and(among::<E>)
}

/// Whether `status` says the store will refuse the request however often it is sent: a request
/// it takes as malformed, a method or length it will not take, or one it does not implement.
fn refused(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::BAD_REQUEST
            | StatusCode::METHOD_NOT_ALLOWED
            | StatusCode::LENGTH_REQUIRED
            | StatusCode::PAYLOAD_TOO_LARGE
            | StatusCode::URI_TOO_LONG
            | StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE
            | StatusCode::NOT_IMPLEMENTED
            | StatusCode::HTTP_VERSION_NOT_SUPPORTED
    )
}

/// The answer to `response`, a refusal: for good, unless S3 says a request timed out or its
/// token expired, which another attempt may not meet; its body, the first [`REFUSAL_BYTES`],
/// says which.
async fn malformed(response: HttpResponse) -> Result<HttpResponse, HttpError> {
    let (parts, body) = response.into_parts();
    let mut stream = body.bytes_stream();
    let mut read = BytesMut::new();
    while read.len() < REFUSAL_BYTES
        && let Some(piece) = stream.next().await
    {
        read.extend_from_slice(&piece?);
    }
    read.truncate(REFUSAL_BYTES);
    let read = read.freeze();
    let passing = [
        b"<Code>RequestTimeout</Code>".as_slice(),
        b"<Code>ExpiredToken</Code>",
    ];
    if passing.iter().any(|code| contains(&read, code)) {
        return Ok(HttpResponse::from_parts(parts, read.into()));
    }
    let reason = format!("the store answered {}", parts.status);
    let refusal = StoreRefusal::new(reason, None);
    Err(HttpError::new(HttpErrorKind::Request, refusal))
}

fn contains(read: &Bytes, code: &[u8]) -> bool {
    read.windows(code.len()).any(|window| window == code)
}

#[cfg(test)]
mod tests;
