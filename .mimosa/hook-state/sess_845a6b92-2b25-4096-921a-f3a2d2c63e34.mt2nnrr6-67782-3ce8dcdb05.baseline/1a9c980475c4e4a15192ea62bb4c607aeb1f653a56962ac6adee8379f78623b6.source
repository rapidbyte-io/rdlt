//! Identifier newtypes: no bare `String` or hash bytes cross a seam.

use std::fmt;

use serde::{Deserialize, Serialize};

macro_rules! string_id {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Wrap a value as this identifier. No validation: an identifier is
            /// whatever the host calls it, and normalization happens later, at
            /// the destination seam where the rules are known.
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            /// The underlying string.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self::new(value)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self::new(value)
            }
        }
    };
}

string_id!(
    /// A named, repeatable pipeline; key for state lookup in the destination.
    PipelineId
);
string_id!(
    /// One run (execution) of a pipeline; stamped on every row as `_rdlt_load_id`.
    LoadId
);
string_id!(
    /// A source stream name (normalized); maps 1:1 to a root table.
    StreamName
);
string_id!(
    /// A physical destination table (root or child), after naming/normalization.
    TableName
);

/// Content hash of one `TableSchema` version.
///
/// Serializes as 64 lowercase hex digits — the rendering persisted in state
/// documents and reported in schema deltas.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaHash([u8; 32]);

impl SchemaHash {
    /// Wrap 32 raw hash bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The raw hash bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Lowercase hex; the portable rendering used in persisted formats.
    pub fn to_hex(&self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(64);
        for byte in &self.0 {
            out.push(HEX[(byte >> 4) as usize] as char);
            out.push(HEX[(byte & 0x0f) as usize] as char);
        }
        out
    }

    /// Parse the hex rendering, either case. Rejects anything that is not
    /// exactly 64 hex digits, so a truncated or mangled persisted value is a
    /// typed error rather than a silently different hash.
    pub fn from_hex(hex: &str) -> Result<Self, InvalidHexId> {
        let bytes = hex.as_bytes();
        if bytes.len() != 64 {
            return Err(InvalidHexId);
        }
        let mut out = [0u8; 32];
        for (i, chunk) in bytes.chunks_exact(2).enumerate() {
            let hi = hex_nibble(chunk[0]).ok_or(InvalidHexId)?;
            let lo = hex_nibble(chunk[1]).ok_or(InvalidHexId)?;
            out[i] = (hi << 4) | lo;
        }
        Ok(Self(out))
    }
}

impl fmt::Display for SchemaHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl Serialize for SchemaHash {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for SchemaHash {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let hex = String::deserialize(d)?;
        Self::from_hex(&hex).map_err(serde::de::Error::custom)
    }
}

/// A string was not a 64-character hex encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("expected a 64-character hex string")]
pub struct InvalidHexId;

fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_ids_serde_transparent() {
        let table = TableName::new("users");
        assert_eq!(serde_json::to_string(&table).unwrap(), "\"users\"");
    }

    #[test]
    fn hex_round_trips_and_accepts_uppercase() {
        let hash = SchemaHash::from_bytes([0xAB; 32]);
        let hex = hash.to_hex();
        assert_eq!(hex, "ab".repeat(32));
        assert_eq!(SchemaHash::from_hex(&hex).expect("lowercase"), hash);
        assert_eq!(
            SchemaHash::from_hex(&hex.to_uppercase()).expect("uppercase"),
            hash
        );
        // Every nibble value survives the round trip exactly.
        let mut bytes = [0u8; 32];
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(17).wrapping_add(9);
        }
        let hash = SchemaHash::from_bytes(bytes);
        assert_eq!(
            SchemaHash::from_hex(&hash.to_hex()).expect("round trip"),
            hash
        );
    }

    #[test]
    fn invalid_hex_is_rejected() {
        assert!(
            SchemaHash::from_hex(&"g".repeat(64)).is_err(),
            "non-hex digit"
        );
        assert!(SchemaHash::from_hex("ab").is_err(), "wrong length");
    }

    /// The serde form IS the hex rendering: a hash in a state document or a
    /// schema delta is a 64-digit string, never a byte array.
    #[test]
    fn serde_form_is_hex_text() {
        let hash = SchemaHash::from_bytes([0x01; 32]);
        let json = serde_json::to_string(&hash).unwrap();
        assert_eq!(json, format!("\"{}\"", "01".repeat(32)));
        let back: SchemaHash = serde_json::from_str(&json).unwrap();
        assert_eq!(back, hash);
    }
}
