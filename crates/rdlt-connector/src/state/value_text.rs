//! A state record's value as base64 text, for the formats that write a record as JSON.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use serde::{Deserialize, Deserializer, Serializer};

pub(super) fn serialize<S: Serializer>(value: &Bytes, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&STANDARD.encode(value))
}

pub(super) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Bytes, D::Error> {
    let text = String::deserialize(deserializer)?;
    STANDARD
        .decode(text)
        .map(Bytes::from)
        .map_err(serde::de::Error::custom)
}
