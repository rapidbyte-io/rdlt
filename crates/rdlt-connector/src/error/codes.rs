//! The codes the protocol gives its own failures, public through `wire` where a build has it.

use super::DEADLINE_EXCEEDED;

/// The code an error goes by in place of the code its connector chose where that is not one it
/// may use, or holds what its host shows nothing of.
pub const INVALID_CODE: &str = "invalid_code";

/// The code of the error for a message the protocol does not allow where it came, from a
/// connector or a host.
pub const INVALID_MESSAGE: &str = "invalid_message";

/// The code of the error for a handshake at a major version of the protocol the other end does
/// not speak.
pub const PROTOCOL_VERSION: &str = "protocol_version";

/// The code of the error for a configuration of a connection already configured.
pub const CONFIGURE_REPEATED: &str = "configure_repeated";

/// The code a transport failure with no error in its details goes by.
pub const TRANSPORT: &str = "transport";

/// The code of an error for a frame the other end sent malformed.
pub const MALFORMED_FRAME: &str = "malformed_frame";

/// The code of an error for a batch of a type the protocol cannot carry.
pub const UNSENDABLE_TYPE: &str = "unsendable_type";

/// The code of an error for a batch its own sender could not encode.
pub const UNENCODABLE_BATCH: &str = "unencodable_batch";

/// The codes a host gives its own findings about a connection, which no connector may claim.
pub const HOST_CODES: &[&str] = &["connector_lost", DEADLINE_EXCEEDED, "tls", TRANSPORT];
