//! How a data-plane call ends, read from its headers and trailers as any gRPC peer reads them.

use tonic::codegen::http::{HeaderMap, StatusCode};
use tonic::{Code, Status};

/// The header naming a call's compression.
const ENCODING: &str = "grpc-encoding";

/// Refuses a call whose `headers` name a compression: the protocol has none.
pub(super) fn uncompressed(headers: &HeaderMap) -> Result<(), Status> {
    match headers.get(ENCODING) {
        None => Ok(()),
        Some(encoding) if encoding.as_bytes() == b"identity" => Ok(()),
        Some(encoding) => Err(Status::unimplemented(format!(
            "a message compressed as {encoding:?}, which is not supported"
        ))),
    }
}

/// Refuses a message whose prefix's flag says it is compressed, or is no flag at all.
pub(super) fn flag(flag: u8) -> Result<(), Status> {
    match flag {
        0 => Ok(()),
        1 => Err(Status::internal(
            "a message flagged compressed on a call that names no compression",
        )),
        other => Err(Status::internal(format!(
            "a message flagged {other}, neither compressed nor not"
        ))),
    }
}

/// How an answer of HTTP status `code` ends once its body has: the status its `trailers` carry,
/// or one its HTTP status implies where they carry none; `None` where it succeeded.
pub(super) fn ended(trailers: Option<&HeaderMap>, code: StatusCode) -> Option<Status> {
    if let Some(status) = trailers.and_then(Status::from_header_map) {
        return (status.code() != Code::Ok).then_some(status);
    }
    let implied = match code {
        StatusCode::BAD_REQUEST => Code::Internal,
        StatusCode::UNAUTHORIZED => Code::Unauthenticated,
        StatusCode::FORBIDDEN => Code::PermissionDenied,
        StatusCode::NOT_FOUND => Code::Unimplemented,
        StatusCode::TOO_MANY_REQUESTS
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT => Code::Unavailable,
        _ => Code::Unknown,
    };
    Some(Status::new(
        implied,
        format!("the answer ended with no status, its HTTP status {code}"),
    ))
}
