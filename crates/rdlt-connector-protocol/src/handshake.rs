//! The stdout handshake line: the one thing a spawned connector process
//! writes before a provider dials it over gRPC, carried as plain text
//! because the provider needs it before any codec exists — WHERE to
//! dial (`socket_path`) and WHICH protocol versions the connector will
//! accept (`protocol_min..=protocol_max`).
//!
//! FROZEN format, one line, five pipe-separated fields:
//! `rdlt-connector|1|<protocol_min>|<protocol_max>|<socket_path>`.
//! The parse splits on the first four pipes only, so the socket path is
//! the one field that is never re-split — a path may itself contain
//! `|`, and splitting on every pipe would shred it.

use std::path::PathBuf;

use thiserror::Error;

use rdlt_core::inventory;

/// This line format's own version — distinct from [`crate::PROTOCOL_VERSION`].
const LINE_FORMAT_VERSION: u32 = 1;

const LEADING_TOKEN: &str = "rdlt-connector";

/// Linux `sockaddr_un.sun_path` holds 108 bytes including the terminating NUL.
/// The project currently supports pathname sockets, not the Linux abstract
/// namespace, so 107 UTF-8 bytes is the portable wire ceiling used here.
const MAX_SOCKET_PATH_BYTES: usize = 107;

/// A parsed (or about-to-be-rendered) handshake line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Line {
    pub socket_path: PathBuf,
    pub protocol_min: u32,
    pub protocol_max: u32,
}

impl Line {
    /// Renders the frozen spelling. `to_string_lossy` is deliberate: a
    /// socket path is presented, not round-tripped through raw bytes — the
    /// UDS filesystem namespace is overwhelmingly UTF-8 in practice, and a
    /// lossy render is still a usable diagnostic if it somehow isn't.
    pub fn render(&self) -> String {
        format!(
            "{LEADING_TOKEN}|{LINE_FORMAT_VERSION}|{}|{}|{}",
            self.protocol_min,
            self.protocol_max,
            self.socket_path.to_string_lossy(),
        )
    }

    /// Parses one handshake line.
    ///
    /// Splits on the FIRST FOUR pipes only (`splitn(5, '|')`): the leading
    /// token, the line-format version, `protocol_min`, `protocol_max` —
    /// and whatever remains, untouched, is the socket path. A path may
    /// itself contain `|` if an operator fights the naming convention;
    /// splitting on every pipe would shred it, so the path is deliberately
    /// the one field that is never re-split.
    pub fn parse(line: &str) -> Result<Line, Error> {
        let mut fields = line.splitn(5, '|');

        let token = fields.next().unwrap_or_default();
        if token != LEADING_TOKEN {
            return Err(Error::NotAConnectorLine);
        }

        let format_version = fields
            .next()
            .ok_or_else(|| Error::Malformed("missing line-format version field".to_string()))?
            .parse::<u32>()
            .map_err(|_| Error::Malformed("line-format version is not a u32".to_string()))?;
        if format_version != LINE_FORMAT_VERSION {
            return Err(Error::UnsupportedFormatVersion(format_version));
        }

        let protocol_min = fields
            .next()
            .ok_or_else(|| Error::Malformed("missing protocol_min field".to_string()))?
            .parse::<u32>()
            .map_err(|_| Error::Malformed("protocol_min is not a u32".to_string()))?;
        let protocol_max = fields
            .next()
            .ok_or_else(|| Error::Malformed("missing protocol_max field".to_string()))?
            .parse::<u32>()
            .map_err(|_| Error::Malformed("protocol_max is not a u32".to_string()))?;
        if protocol_min > protocol_max {
            // An inverted range can never be satisfied — refused where
            // it is parsed, so no consumer re-derives the sanity check.
            return Err(Error::Malformed(format!(
                "protocol_min {protocol_min} exceeds protocol_max {protocol_max}"
            )));
        }
        let socket_path = fields
            .next()
            .ok_or_else(|| Error::Malformed("missing socket_path field".to_string()))?;
        if socket_path.is_empty() {
            return Err(Error::Malformed("socket_path is empty".to_string()));
        }
        if !std::path::Path::new(socket_path).is_absolute() {
            return Err(Error::Malformed("socket_path is not absolute".to_string()));
        }
        if socket_path.len() > MAX_SOCKET_PATH_BYTES {
            return Err(Error::Malformed(format!(
                "socket_path is {} bytes, over the {MAX_SOCKET_PATH_BYTES}-byte Unix socket cap",
                socket_path.len()
            )));
        }
        // The full shared inventory, joiners included: a socket path is
        // filesystem material, not human orthography, so nothing
        // invisible has a legitimate seat in it — and the same table
        // gating the client's identifier seats gates the path here, so
        // the two sides of the wire cannot drift.
        if socket_path.chars().any(inventory::is_control_or_invisible) {
            return Err(Error::Malformed(
                "socket_path contains control or invisible formatting characters".to_string(),
            ));
        }

        Ok(Line {
            socket_path: PathBuf::from(socket_path),
            protocol_min,
            protocol_max,
        })
    }
}

/// Typed reasons `Line::parse` refuses a line.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum Error {
    #[error("not an rdlt-connector handshake line")]
    NotAConnectorLine,
    #[error("unsupported handshake line-format version {0}")]
    UnsupportedFormatVersion(u32),
    #[error("malformed handshake line: {0}")]
    Malformed(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_line_round_trips_and_freezes_its_spelling() {
        let line = Line {
            socket_path: "/tmp/x.sock".into(),
            protocol_min: 0,
            protocol_max: 0,
        };
        assert_eq!(line.render(), "rdlt-connector|1|0|0|/tmp/x.sock");
        assert_eq!(Line::parse(&line.render()).expect("parses"), line);
    }

    #[test]
    fn a_path_containing_pipes_survives() {
        let line = Line {
            socket_path: "/tmp/odd|dir/x.sock".into(),
            protocol_min: 0,
            protocol_max: 0,
        };
        assert_eq!(Line::parse(&line.render()).expect("parses"), line);
    }

    /// An inverted range can never be satisfied — refused where it is
    /// parsed, so no consumer has to re-derive the sanity check.
    #[test]
    fn an_inverted_protocol_range_refuses_at_parse() {
        assert_eq!(
            Line::parse("rdlt-connector|1|3|1|/x"),
            Err(Error::Malformed(
                "protocol_min 3 exceeds protocol_max 1".to_string()
            ))
        );
    }

    #[test]
    fn foreign_lines_refuse_typed() {
        assert!(Line::parse("not-a-connector-line").is_err());
        assert!(
            Line::parse("rdlt-connector|9|0|0|/x").is_err(),
            "unknown line-format version refuses"
        );
    }

    #[test]
    fn socket_paths_are_absolute_bounded_and_control_free() {
        for line in [
            "rdlt-connector|1|0|0|relative.sock".to_string(),
            format!(
                "rdlt-connector|1|0|0|/{}",
                "x".repeat(MAX_SOCKET_PATH_BYTES)
            ),
            "rdlt-connector|1|0|0|/tmp/evil\u{1b}.sock".to_string(),
            // The invisible set is refused too, by the same shared
            // inventory the client applies — a right-to-left override
            // or zero-width space in a path is spoofing material even
            // where the render escapes it.
            "rdlt-connector|1|0|0|/tmp/evil\u{202e}.sock".to_string(),
            "rdlt-connector|1|0|0|/tmp/evil\u{200b}.sock".to_string(),
            "rdlt-connector|1|0|0|/tmp/evil\u{200d}.sock".to_string(),
        ] {
            assert!(Line::parse(&line).is_err(), "must refuse {line:?}");
        }
        let longest = format!(
            "rdlt-connector|1|0|0|/{}",
            "x".repeat(MAX_SOCKET_PATH_BYTES - 1)
        );
        assert!(Line::parse(&longest).is_ok(), "107-byte path remains valid");
    }
}
