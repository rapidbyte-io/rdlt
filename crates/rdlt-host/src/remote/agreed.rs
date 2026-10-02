//! What a connector's handshake answer must say before the host takes it.
//!
//! A connector of another protocol would be driven with messages it reads otherwise, so the host
//! refuses it itself rather than trust it to refuse. Its spec's id and version are machine
//! strings the host shows and compares: an id that does not parse, or a version that is not one,
//! is refused before anything shows it. A feature it accepts must be one the host offered.

#[cfg(test)]
mod tests;

use rdlt_connector::wire::{Invalid, v1};
use rdlt_connector::{ConnectorError, ConnectorErrorKind};
use rdlt_wire::PROTOCOL_MAJOR;

use super::source::invalid;

/// Bytes: the longest version a connector may give.
const MAX_VERSION_BYTES: usize = 64;

/// Checks `answer`, a handshake answer to an offer of `offered` features.
///
/// # Errors
///
/// An `Unsupported` error coded `protocol_version` for another major; an internal error coded
/// `invalid_message` for a spec whose id or version is malformed, or a feature not offered.
pub(super) fn agreed(
    answer: &v1::HandshakeResponse,
    offered: &[String],
) -> Result<(), ConnectorError> {
    if answer.protocol_major != PROTOCOL_MAJOR {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            format!(
                "the connector speaks protocol {}, not this host's {PROTOCOL_MAJOR}",
                answer.protocol_major
            ),
        )
        .with_code("protocol_version"));
    }
    if let Some(feature) = answer
        .accepted_features
        .iter()
        .find(|feature| !offered.contains(feature))
    {
        let shown = rdlt_connector::text::shown(feature, MAX_VERSION_BYTES);
        return Err(invalid(&Invalid::rejected(
            "accepted features",
            Unoffered(shown),
        )));
    }
    let spec = answer
        .spec
        .as_ref()
        .ok_or_else(|| invalid(&Invalid::Missing("spec")))?;
    rdlt_connector::ConnectorId::parse(&spec.id)
        .map_err(|error| invalid(&Invalid::rejected("connector id", error)))?;
    if !version(&spec.version) {
        return Err(invalid(&Invalid::rejected("connector version", Malformed)));
    }
    Ok(())
}

/// Whether `text` is a version: letters, digits, `.`, `+` and `-`, within the longest.
fn version(text: &str) -> bool {
    let allowed = |c: char| c.is_ascii_alphanumeric() || ".+-".contains(c);
    (1..=MAX_VERSION_BYTES).contains(&text.len()) && text.chars().all(allowed)
}

/// A feature accepted that the host did not offer.
#[derive(Debug, thiserror::Error)]
#[error("`{0}` was not offered")]
struct Unoffered(String);

/// A version that is not one.
#[derive(Debug, thiserror::Error)]
#[error("it is not a version")]
struct Malformed;
