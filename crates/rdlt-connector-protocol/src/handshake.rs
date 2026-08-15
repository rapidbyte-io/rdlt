//! The stdout handshake line: the one thing a spawned connector process
//! writes before a provider dials it over gRPC. It carries no protobuf —
//! the provider needs it before any codec exists, to learn WHERE to dial
//! (`socket_path`) and WHICH protocol versions the connector will accept
//! (`proto_min..=proto_max`, negotiated inside `HandshakeRequest` once the
//! channel is up).
//!
//! FROZEN format, one line, five pipe-separated fields:
//! `rdlt-connector|1|<proto_min>|<proto_max>|<socket_path>`
//!
//! The leading token names the line kind; `1` is this line FORMAT's own
//! version (never `PROTOCOL_VERSION` — the two evolve independently: the
//! line format could reach `2` while the RPC protocol stays at `0`).

use std::path::PathBuf;

use thiserror::Error;

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
    pub proto_min: u32,
    pub proto_max: u32,
}

impl Line {
    /// Renders the frozen spelling. `to_string_lossy` is deliberate: a
    /// socket path is presented, not round-tripped through raw bytes — the
    /// UDS filesystem namespace is overwhelmingly UTF-8 in practice, and a
    /// lossy render is still a usable diagnostic if it somehow isn't.
    pub fn render(&self) -> String {
        format!(
            "{LEADING_TOKEN}|{LINE_FORMAT_VERSION}|{}|{}|{}",
            self.proto_min,
            self.proto_max,
            self.socket_path.to_string_lossy(),
        )
    }

    /// Parses one handshake line.
    ///
    /// Splits on the FIRST FOUR pipes only (`splitn(5, '|')`): the leading
    /// token, the line-format version, `proto_min`, `proto_max` — and
    /// whatever remains, untouched, is the socket path. A path may itself
    /// contain `|` if an operator fights the naming convention; splitting
    /// on every pipe would shred it, so the path is deliberately the one
    /// field that is never re-split.
    pub fn parse(line: &str) -> Result<Line, LineError> {
        let mut fields = line.splitn(5, '|');

        let token = fields.next().unwrap_or_default();
        if token != LEADING_TOKEN {
            return Err(LineError::NotAConnectorLine);
        }

        let format_version = fields
            .next()
            .ok_or_else(|| LineError::Malformed("missing line-format version field".to_string()))?
            .parse::<u32>()
            .map_err(|_| LineError::Malformed("line-format version is not a u32".to_string()))?;
        if format_version != LINE_FORMAT_VERSION {
            return Err(LineError::UnsupportedLineVersion(format_version));
        }

        let proto_min = fields
            .next()
            .ok_or_else(|| LineError::Malformed("missing proto_min field".to_string()))?
            .parse::<u32>()
            .map_err(|_| LineError::Malformed("proto_min is not a u32".to_string()))?;
        let proto_max = fields
            .next()
            .ok_or_else(|| LineError::Malformed("missing proto_max field".to_string()))?
            .parse::<u32>()
            .map_err(|_| LineError::Malformed("proto_max is not a u32".to_string()))?;
        if proto_min > proto_max {
            // An inverted range can never be satisfied — refused where
            // it is parsed, so no consumer re-derives the sanity check.
            return Err(LineError::Malformed(format!(
                "proto_min {proto_min} exceeds proto_max {proto_max}"
            )));
        }
        let socket_path = fields
            .next()
            .ok_or_else(|| LineError::Malformed("missing socket_path field".to_string()))?;
        if socket_path.is_empty() {
            return Err(LineError::Malformed("socket_path is empty".to_string()));
        }
        if !std::path::Path::new(socket_path).is_absolute() {
            return Err(LineError::Malformed(
                "socket_path is not absolute".to_string(),
            ));
        }
        if socket_path.len() > MAX_SOCKET_PATH_BYTES {
            return Err(LineError::Malformed(format!(
                "socket_path is {} bytes, over the {MAX_SOCKET_PATH_BYTES}-byte Unix socket cap",
                socket_path.len()
            )));
        }
        if socket_path.chars().any(char::is_control) {
            return Err(LineError::Malformed(
                "socket_path contains control characters".to_string(),
            ));
        }

        Ok(Line {
            socket_path: PathBuf::from(socket_path),
            proto_min,
            proto_max,
        })
    }
}

/// Typed reasons `Line::parse` refuses a line.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum LineError {
    #[error("not an rdlt-connector handshake line")]
    NotAConnectorLine,
    #[error("unsupported handshake line-format version {0}")]
    UnsupportedLineVersion(u32),
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
            proto_min: 0,
            proto_max: 0,
        };
        assert_eq!(line.render(), "rdlt-connector|1|0|0|/tmp/x.sock");
        assert_eq!(Line::parse(&line.render()).expect("parses"), line);
    }

    #[test]
    fn a_path_containing_pipes_survives() {
        let line = Line {
            socket_path: "/tmp/odd|dir/x.sock".into(),
            proto_min: 0,
            proto_max: 0,
        };
        assert_eq!(Line::parse(&line.render()).expect("parses"), line);
    }

    /// An inverted range can never be satisfied — refused where it is
    /// parsed, so no consumer has to re-derive the sanity check (045
    /// external findings, GROK 7).
    #[test]
    fn an_inverted_protocol_range_refuses_at_parse() {
        assert_eq!(
            Line::parse("rdlt-connector|1|3|1|/x"),
            Err(LineError::Malformed(
                "proto_min 3 exceeds proto_max 1".to_string()
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
