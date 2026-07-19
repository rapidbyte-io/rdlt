//! Identifier newtypes. No bare `String`/`u64` crosses a seam (data-model.md §1).

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
            /// and persisted formats.
            pub fn to_hex(&self) -> String {
                let mut out = String::with_capacity(64);
                for byte in self.0 {
                    use fmt::Write;
                    write!(out, "{byte:02x}").expect("writing hex to String cannot fail");
                }
                out
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
    /// Content hash of one `TableSchema` version (contracts/persisted-formats.md §5).
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
