//! The persisted per-stream resume state, and scalar extraction from
//! decoded batches.
//!
//! The JSON shape is FROZEN — state written by the first-generation crate
//! must decode here unchanged: `{"watermark": {"t": <tag>, "v": …},
//! "boundary_keys": […]}` with the tag vocabulary pinned below. The Rust
//! value vocabulary is [`Scalar`]; this module owns ONLY its persistence.

use rdlt_connector_sdk::spi::{Cursor, SourceError};
use serde::{Deserialize, Serialize};

use crate::source::errors::{self, Phase};
use crate::types::{Kind, Scalar};

/// The persisted per-stream state: the watermark plus the keys of the rows
/// that share it. Empty keys ⇒ strict `>` resume; non-empty ⇒ `>=` + dedup.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct State {
    pub(crate) watermark: Scalar,
    pub(crate) boundary_keys: Vec<String>,
}

impl State {
    pub(crate) fn encode(&self) -> Cursor {
        Cursor::new(serde_json::to_value(Persisted::from(self)).expect("cursor state serializes"))
    }

    pub(crate) fn decode(cursor: &Cursor, table: &str) -> Result<Self, SourceError> {
        let persisted: Persisted =
            serde_json::from_value(cursor.as_value().clone()).map_err(|e| {
                errors::fatal(
                    Phase::Reflect,
                    Some(table),
                    format!("stored cursor state does not decode (state corruption?): {e}"),
                )
            })?;
        Ok(persisted.into())
    }
}

/// The FROZEN wire form. A mirror rather than serde on [`Scalar`] itself so
/// the persisted format cannot change as a side effect of evolving the
/// in-memory vocabulary.
#[derive(Serialize, Deserialize)]
struct Persisted {
    watermark: Watermark,
    #[serde(default)]
    boundary_keys: Vec<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "t", content = "v", rename_all = "snake_case")]
enum Watermark {
    Int(i64),
    Decimal {
        #[serde(with = "i128_text")]
        scaled: i128,
        scale: u8,
    },
    Text(String),
    Uuid(String),
    TimestampTz(i64),
    TimestampNaive(i64),
    Date(i32),
    Time(i64),
}

/// i128 (JSON can't carry it) ↔ decimal-string serde for the Decimal scaled
/// field.
mod i128_text {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &i128, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i128, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

impl From<&State> for Persisted {
    fn from(state: &State) -> Self {
        Persisted {
            watermark: match &state.watermark {
                Scalar::Int(value) => Watermark::Int(*value),
                Scalar::Decimal { scaled, scale } => Watermark::Decimal {
                    scaled: *scaled,
                    scale: *scale,
                },
                Scalar::Text(value) => Watermark::Text(value.clone()),
                Scalar::Uuid(value) => Watermark::Uuid(value.clone()),
                Scalar::TimestampTz(value) => Watermark::TimestampTz(*value),
                Scalar::TimestampNaive(value) => Watermark::TimestampNaive(*value),
                Scalar::Date(value) => Watermark::Date(*value),
                Scalar::Time(value) => Watermark::Time(*value),
            },
            boundary_keys: state.boundary_keys.clone(),
        }
    }
}

impl From<Persisted> for State {
    fn from(persisted: Persisted) -> Self {
        State {
            watermark: match persisted.watermark {
                Watermark::Int(value) => Scalar::Int(value),
                Watermark::Decimal { scaled, scale } => Scalar::Decimal { scaled, scale },
                Watermark::Text(value) => Scalar::Text(value),
                Watermark::Uuid(value) => Scalar::Uuid(value),
                Watermark::TimestampTz(value) => Scalar::TimestampTz(value),
                Watermark::TimestampNaive(value) => Scalar::TimestampNaive(value),
                Watermark::Date(value) => Scalar::Date(value),
                Watermark::Time(value) => Scalar::Time(value),
            },
            boundary_keys: persisted.boundary_keys,
        }
    }
}

/// Extract a typed scalar from a decoded column at `row` (None = NULL, or a
/// kind that never carries a cursor).
pub(crate) fn scalar_at(column: &arrow_array::ArrayRef, kind: Kind, row: usize) -> Option<Scalar> {
    use arrow_array::{
        Array, Date32Array, Decimal128Array, Int64Array, StringArray, Time64MicrosecondArray,
        TimestampMicrosecondArray,
    };
    if column.is_null(row) {
        return None;
    }
    let any = column.as_any();
    match kind {
        Kind::Int16 | Kind::Int32 | Kind::Int64 => {
            Some(Scalar::Int(any.downcast_ref::<Int64Array>()?.value(row)))
        }
        Kind::Decimal { scale, .. } => Some(Scalar::Decimal {
            scaled: any.downcast_ref::<Decimal128Array>()?.value(row),
            scale,
        }),
        Kind::Text => Some(Scalar::Text(
            any.downcast_ref::<StringArray>()?.value(row).to_owned(),
        )),
        Kind::Uuid => Some(Scalar::Uuid(
            any.downcast_ref::<StringArray>()?.value(row).to_owned(),
        )),
        Kind::TimestampTz => Some(Scalar::TimestampTz(
            any.downcast_ref::<TimestampMicrosecondArray>()?.value(row),
        )),
        Kind::TimestampNaive => Some(Scalar::TimestampNaive(
            any.downcast_ref::<TimestampMicrosecondArray>()?.value(row),
        )),
        Kind::Date => Some(Scalar::Date(any.downcast_ref::<Date32Array>()?.value(row))),
        Kind::Time => Some(Scalar::Time(
            any.downcast_ref::<Time64MicrosecondArray>()?.value(row),
        )),
        Kind::Bool | Kind::Float32 | Kind::Float64 | Kind::Jsonb | Kind::Bytea => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn persisted_json_shape_is_frozen() {
        // The exact wire form the first-generation crate wrote — decoding
        // and re-encoding must both hold.
        let json = serde_json::json!({
            "watermark": {"t": "timestamp_tz", "v": 1_700_000_000_000_000i64},
            "boundary_keys": ["7|a"]
        });
        let state = State::decode(&Cursor::new(json.clone()), "t").expect("frozen shape decodes");
        assert_eq!(state.watermark, Scalar::TimestampTz(1_700_000_000_000_000));
        assert_eq!(state.boundary_keys, ["7|a"]);
        assert_eq!(state.encode().as_value(), &json);
        // Decimal carries i128 as TEXT, and boundary_keys defaults.
        let decimal = serde_json::json!({
            "watermark": {"t": "decimal", "v": {"scaled": "170141183460469231731687303715884105727", "scale": 2}}
        });
        let state = State::decode(&Cursor::new(decimal), "t").expect("decimal decodes");
        assert_eq!(
            state.watermark,
            Scalar::Decimal {
                scaled: i128::MAX,
                scale: 2
            }
        );
        assert!(state.boundary_keys.is_empty());
        // Tag vocabulary, one per variant.
        for (scalar, tag) in [
            (Scalar::Int(1), "int"),
            (Scalar::Text("x".into()), "text"),
            (Scalar::Uuid("u".into()), "uuid"),
            (Scalar::TimestampTz(1), "timestamp_tz"),
            (Scalar::TimestampNaive(1), "timestamp_naive"),
            (Scalar::Date(1), "date"),
            (Scalar::Time(1), "time"),
        ] {
            let state = State {
                watermark: scalar,
                boundary_keys: vec![],
            };
            assert_eq!(state.encode().as_value()["watermark"]["t"], tag);
        }
    }

    fn any_scalar() -> impl Strategy<Value = Scalar> {
        prop_oneof![
            any::<i64>().prop_map(Scalar::Int),
            (any::<i128>(), 0u8..=38).prop_map(|(scaled, scale)| Scalar::Decimal { scaled, scale }),
            ".*".prop_map(Scalar::Text),
            "[0-9a-f-]{1,36}".prop_map(Scalar::Uuid),
            any::<i64>().prop_map(Scalar::TimestampTz),
            any::<i64>().prop_map(Scalar::TimestampNaive),
            any::<i32>().prop_map(Scalar::Date),
            any::<i64>().prop_map(Scalar::Time),
        ]
    }

    proptest! {
        /// decode(encode(state)) is the identity — resume state survives any
        /// round trip losslessly.
        #[test]
        fn state_round_trips(watermark in any_scalar(), keys in proptest::collection::vec(".*", 0..4)) {
            let state = State { watermark, boundary_keys: keys };
            let decoded = State::decode(&state.encode(), "t").expect("round-trip decodes");
            prop_assert_eq!(decoded, state);
        }
    }
}
