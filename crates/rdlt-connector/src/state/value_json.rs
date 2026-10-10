//! A state record's value as the JSON text it is, for the formats that write a record as JSON:
//! written as it lies, read back byte for byte.

use bytes::Bytes;
use serde::ser::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;

pub(super) fn serialize<S: Serializer>(value: &Bytes, serializer: S) -> Result<S::Ok, S::Error> {
    let text = std::str::from_utf8(value).map_err(S::Error::custom)?;
    let raw: &RawValue = serde_json::from_str(text).map_err(S::Error::custom)?;
    // Whitespace around the value would not be written, and it would read back as other bytes.
    if raw.get().len() != text.len() {
        return Err(S::Error::custom(
            "a state value is JSON text with nothing around it",
        ));
    }
    raw.serialize(serializer)
}

pub(super) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Bytes, D::Error> {
    let raw = Box::<RawValue>::deserialize(deserializer)?;
    Ok(Bytes::from(Box::<str>::from(raw).into_boxed_bytes()))
}
