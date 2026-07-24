//! Identifier newtypes. No bare `String`/`u64` crosses a seam.

use std::fmt;

use serde::{Deserialize, Serialize};

macro_rules! string_id {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

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

macro_rules! hash_id {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name([u8; 32]);

        impl $name {
            pub fn from_bytes(bytes: [u8; 32]) -> Self {
                Self(bytes)
            }

            pub fn as_bytes(&self) -> &[u8; 32] {
                &self.0
            }

            /// Lowercase hex; the portable rendering used in destination columns
            /// and persisted formats. Table-driven — this runs three times per
            /// shredded row (id/parent/root); the `write!("{:02x}")` form it
            /// replaced dominated shred-time instruction counts under profiling.
            pub fn to_hex(&self) -> String {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                let mut out = vec![0u8; 64];
                for (i, byte) in self.0.iter().enumerate() {
                    out[i * 2] = HEX[(byte >> 4) as usize];
                    out[i * 2 + 1] = HEX[(byte & 0x0f) as usize];
                }
                String::from_utf8(out).expect("hex digits are ASCII")
            }

            /// Allocation-free hex into a stack buffer (hot path: three of
            /// these per shredded row land in Arrow builders).
            pub fn write_hex(&self, out: &mut [u8; 64]) {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                for (i, byte) in self.0.iter().enumerate() {
                    out[i * 2] = HEX[(byte >> 4) as usize];
                    out[i * 2 + 1] = HEX[(byte & 0x0f) as usize];
                }
            }

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

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.to_hex())
            }
        }

        impl Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_str(&self.to_hex())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let hex = String::deserialize(d)?;
                Self::from_hex(&hex).map_err(serde::de::Error::custom)
            }
        }
    };
}

hash_id!(
    /// Content hash of one `TableSchema` version.
    SchemaHash
);
hash_id!(
    /// Deterministic row identity (`_rdlt_id`): content hash (keyless) or key hash (keyed).
    RowId
);

/// A string was not a 64-char lowercase/uppercase hex encoding.
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
    fn hex_round_trip() {
        let id = RowId::from_bytes([0xab; 32]);
        assert_eq!(RowId::from_hex(&id.to_hex()).unwrap(), id);
    }

    #[test]
    fn string_ids_serde_transparent() {
        let table = TableName::new("users");
        assert_eq!(serde_json::to_string(&table).unwrap(), "\"users\"");
    }
}

#[cfg(test)]
mod hex_tests {
    // Mutation-report closure: hex ENCODING is used everywhere; DECODING
    // (hex_nibble arms) had no direct tests.
    use super::*;

    #[test]
    fn hex_round_trips_and_accepts_uppercase() {
        let id = RowId::from_bytes([0xAB; 32]);
        let hex = id.to_hex();
        assert_eq!(hex, "ab".repeat(32));
        assert_eq!(RowId::from_hex(&hex).expect("lowercase"), id);
        assert_eq!(RowId::from_hex(&hex.to_uppercase()).expect("uppercase"), id);
        // Every nibble value survives the round trip exactly.
        let mut bytes = [0u8; 32];
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(17).wrapping_add(9);
        }
        let id = RowId::from_bytes(bytes);
        assert_eq!(RowId::from_hex(&id.to_hex()).expect("round trip"), id);
    }

    #[test]
    fn invalid_hex_is_rejected() {
        assert!(RowId::from_hex(&"g".repeat(64)).is_err(), "non-hex digit");
        assert!(RowId::from_hex("ab").is_err(), "wrong length");
    }
}
