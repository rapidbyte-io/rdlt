//! Cursors: a partition's resume position, opaque to the engine.

#[cfg(test)]
mod tests;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::{ConnectorError, LimitExceeded, Result, ResultExt};
use crate::limits::MAX_CURSOR_BYTES;

/// A partition's resume position: versioned bytes only the connector interprets.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Cursor {
    version: u16,
    bytes: Bytes,
}

impl Cursor {
    /// A cursor of `bytes` in the connector's format `version`, at most
    /// [`MAX_CURSOR_BYTES`](crate::limits::MAX_CURSOR_BYTES).
    pub fn new(version: u16, bytes: Bytes) -> Result<Self> {
        let actual = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if actual > MAX_CURSOR_BYTES {
            return Err(ConnectorError::exceeds(LimitExceeded {
                name: "cursor bytes",
                limit: MAX_CURSOR_BYTES,
                actual,
            }));
        }
        Ok(Self { version, bytes })
    }

    /// Encodes `value` as JSON in format `version`.
    pub fn encode<T: Serialize>(version: u16, value: &T) -> Result<Self> {
        let json = serde_json::to_vec(value).internal("encoding a cursor")?;
        Self::new(version, Bytes::from(json))
    }

    /// Decodes a cursor written by [`Cursor::encode`] in format `version`.
    ///
    /// A cursor in another format is a [`Config`](crate::ConnectorErrorKind::Config) error with
    /// code `cursor_version`, never a silent restart.
    pub fn decode<T: DeserializeOwned>(&self, version: u16) -> Result<T> {
        if self.version != version {
            let message = format!(
                "cursor is format {}; this connector reads format {version}",
                self.version
            );
            return Err(ConnectorError::config(message).with_code("cursor_version"));
        }
        serde_json::from_slice(&self.bytes).data("decoding a cursor")
    }

    /// The connector's format version.
    pub fn version(&self) -> u16 {
        self.version
    }

    /// The encoded position.
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }
}

#[derive(Serialize, Deserialize)]
struct EncodedCursor {
    version: u16,
    base64: String,
}

impl Serialize for Cursor {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        EncodedCursor {
            version: self.version,
            base64: STANDARD.encode(&self.bytes),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Cursor {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let encoded = EncodedCursor::deserialize(deserializer)?;
        let bytes = STANDARD
            .decode(encoded.base64)
            .map_err(serde::de::Error::custom)?;
        Self::new(encoded.version, Bytes::from(bytes)).map_err(serde::de::Error::custom)
    }
}
