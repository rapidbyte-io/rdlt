//! Connector errors as gRPC statuses: the status's code follows the error's kind, and its details
//! carry the error whole, so the other end reads back the same error.

use rdlt_wire::limits::CONTROL_STRING_BYTES;
use rdlt_wire::prost::Message as _;
use rdlt_wire::tonic::{Code, Status};

use super::v1;
use crate::error::{ConnectorError, ConnectorErrorKind};

/// The code a transport failure with no error in its details goes by.
pub const TRANSPORT: &str = "transport";

/// `error` as a gRPC status carrying it whole, its message cut to the control string limit.
pub fn status(error: &ConnectorError) -> Status {
    let code = match error.kind() {
        ConnectorErrorKind::Config => Code::InvalidArgument,
        ConnectorErrorKind::Auth => Code::PermissionDenied,
        ConnectorErrorKind::Transient => Code::Unavailable,
        ConnectorErrorKind::RateLimited => Code::ResourceExhausted,
        ConnectorErrorKind::Data => Code::FailedPrecondition,
        ConnectorErrorKind::Unsupported => Code::Unimplemented,
        ConnectorErrorKind::Fenced => Code::Aborted,
        ConnectorErrorKind::Stopped => Code::Cancelled,
        ConnectorErrorKind::Internal => Code::Internal,
    };
    let mut carried = v1::Error::from(error);
    cut(&mut carried.message, CONTROL_STRING_BYTES);
    let mut message = error.to_string();
    cut(&mut message, STATUS_MESSAGE_BYTES);
    Status::with_details(code, message, carried.encode_to_vec().into())
}

/// Bytes: the status's own message, for a reader that does not decode its details.
const STATUS_MESSAGE_BYTES: u64 = 1024;

/// Cuts `text` to at most `bytes`, at a character's boundary.
fn cut(text: &mut String, bytes: u64) {
    let end = text.floor_char_boundary(usize::try_from(bytes).unwrap_or(usize::MAX));
    text.truncate(end);
}

/// The error `status` carries; one without an error in its details is a failure of the transport,
/// transient where retrying may help.
pub fn error(status: &Status) -> ConnectorError {
    let carried = v1::Error::decode(status.details())
        .ok()
        .filter(|_| !status.details().is_empty())
        .and_then(|error| ConnectorError::try_from(error).ok());
    if let Some(error) = carried {
        return error;
    }
    let kind = match status.code() {
        Code::Unavailable
        | Code::DeadlineExceeded
        | Code::ResourceExhausted
        | Code::Aborted
        | Code::Cancelled
        | Code::Unknown => ConnectorErrorKind::Transient,
        _ => ConnectorErrorKind::Internal,
    };
    let message = format!(
        "the connection failed ({:?}): {}",
        status.code(),
        status.message()
    );
    ConnectorError::new(kind, message).with_code(TRANSPORT)
}

/// The code of an error for a frame the other end sent malformed.
pub const MALFORMED_FRAME: &str = "malformed_frame";

/// A frame the codec refused, as the connector error the end that received it reports: a limit's
/// refusal, or a frame malformed by a faulty peer, which retrying cannot help.
pub fn frame_error(error: &rdlt_wire::WireError) -> ConnectorError {
    match error {
        rdlt_wire::WireError::Refused(refusal) => {
            let limit = crate::error::LimitExceeded {
                name: rdlt_wire::limits::FIELDS
                    .iter()
                    .find(|name| **name == refusal.field)
                    .copied()
                    .unwrap_or("limit"),
                limit: refusal.limit,
                actual: refusal.actual,
            };
            ConnectorError::exceeds(limit)
        }
        other => ConnectorError::new(ConnectorErrorKind::Internal, other.to_string())
            .with_code(MALFORMED_FRAME),
    }
}
