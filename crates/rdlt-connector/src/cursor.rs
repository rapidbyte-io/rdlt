//! Cursors: a partition's resume position, opaque to the engine.

#[cfg(test)]
mod tests;

use base64::Engine as _;
use base64::display::Base64Display;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::{ConnectorError, Result, ResultExt};
use rdlt_wire::limits::{CURSOR_BYTES, count};

/// A partition's resume position: versioned bytes only the connector interprets.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Cursor {
    version: u16,
    bytes: Bytes,
}

impl Cursor {
    /// A cursor of `bytes` in the connector's format `version`, at most
    /// [`CURSOR_BYTES`](rdlt_wire::limits::CURSOR_BYTES).
    ///
    /// The bytes are copied into an allocation of their own: a cursor waits for its commit, and
    /// must not keep the message or buffer it was cut from alive meanwhile.
    pub fn new(version: u16, bytes: &[u8]) -> Result<Self> {
        admit(bytes.len())?;
        Ok(Self {
            version,
            bytes: Bytes::copy_from_slice(bytes),
        })
    }

    /// Encodes `value` as JSON in format `version`.
    pub fn encode<T: Serialize>(version: u16, value: &T) -> Result<Self> {
        let json = serde_json::to_vec(value).internal("encoding a cursor")?;
        Self::new(version, &json)
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

/// A cursor as JSON is written: its version, then its bytes in base64.
#[derive(Serialize)]
struct WrittenCursor<'a> {
    version: u16,
    base64: Base64<'a>,
}

/// A cursor as JSON is read: its version and its decoded bytes, and no other member.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EncodedCursor {
    version: u16,
    base64: Decoded,
}

/// A cursor's bytes, written as base64 into the writer of the JSON that holds them.
struct Base64<'a>(&'a [u8]);

impl Serialize for Base64<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(&Base64Display::new(self.0, &STANDARD))
    }
}

/// A cursor's bytes, decoded from the JSON text that holds them in base64.
struct Decoded(Vec<u8>);

impl<'de> Deserialize<'de> for Decoded {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        deserializer.deserialize_str(DecodedVisitor)
    }
}

struct DecodedVisitor;

impl serde::de::Visitor<'_> for DecodedVisitor {
    type Value = Decoded;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a cursor's bytes in base64")
    }

    fn visit_str<E: serde::de::Error>(self, text: &str) -> std::result::Result<Decoded, E> {
        STANDARD.decode(text).map(Decoded).map_err(E::custom)
    }
}

impl Serialize for Cursor {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        WrittenCursor {
            version: self.version,
            base64: Base64(&self.bytes),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Cursor {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let EncodedCursor {
            version,
            base64: Decoded(bytes),
        } = EncodedCursor::deserialize(deserializer)?;
        admit(bytes.len()).map_err(serde::de::Error::custom)?;
        // The decoded bytes are an allocation of the cursor's own, as `new` makes one.
        Ok(Self {
            version,
            bytes: Bytes::from(bytes),
        })
    }
}

/// Admits a cursor of `len` bytes, at most [`CURSOR_BYTES`].
fn admit(len: usize) -> Result<()> {
    rdlt_wire::Limits::admit(rdlt_wire::Limit::CursorBytes, CURSOR_BYTES, count(len))?;
    Ok(())
}
