//! Binary-COPY wire encoding: arrow column → Postgres wire type + cell values.

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
    StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use rdlt_connector::DestError;
use tokio_postgres::types::{ToSql, Type};

use rdlt_connector::core::{ColumnType, LogicalType};

use super::fatal;

/// The wire decision for one column: LOGICAL type first (T6 — Utf8-as-text
/// vs Utf8-as-json/uuid can never confuse), arrow representation second.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ColumnWire {
    Bool,
    Int8,
    Float8,
    Text,
    Bytea,
    TimestampTz,
    Timestamp,
    Date,
    Time,
    Numeric { scale: u8 },
    Jsonb,
    Uuid,
}

/// Decide the wire encoding from the schema's logical type + the batch's
/// arrow type. `logical: None` (a column outside the ensured schema, e.g.
/// engine system columns in edge orders) falls back to the arrow type.
pub(super) fn column_wire(
    logical: Option<&ColumnType>,
    dt: &DataType,
) -> Result<ColumnWire, DestError> {
    if let Some(ColumnType::Scalar { scalar }) = logical {
        match (scalar, dt) {
            (LogicalType::Decimal { scale, .. }, DataType::Decimal128(_, _)) => {
                return Ok(ColumnWire::Numeric { scale: *scale });
            }
            (LogicalType::Json, DataType::Utf8) => return Ok(ColumnWire::Jsonb),
            (LogicalType::Uuid, DataType::Utf8) => return Ok(ColumnWire::Uuid),
            (LogicalType::Time, DataType::Time64(TimeUnit::Microsecond)) => {
                return Ok(ColumnWire::Time);
            }
            _ => {} // representation-driven below
        }
    }
    Ok(match dt {
        DataType::Boolean => ColumnWire::Bool,
        DataType::Int64 => ColumnWire::Int8,
        DataType::Float64 => ColumnWire::Float8,
        DataType::Utf8 => ColumnWire::Text,
        DataType::Binary => ColumnWire::Bytea,
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => ColumnWire::TimestampTz,
        DataType::Timestamp(TimeUnit::Microsecond, None) => ColumnWire::Timestamp,
        DataType::Date32 => ColumnWire::Date,
        DataType::Time64(TimeUnit::Microsecond) => ColumnWire::Time,
        DataType::Decimal128(_, scale) => ColumnWire::Numeric {
            scale: u8::try_from(*scale)
                .map_err(|_| fatal(format!("negative decimal scale {scale} unsupported")))?,
        },
        other => return Err(fatal(format!("unsupported arrow type for COPY: {other}"))),
    })
}

/// Postgres wire type per column decision.
pub(super) fn wire_type(wire: ColumnWire) -> Type {
    match wire {
        ColumnWire::Bool => Type::BOOL,
        ColumnWire::Int8 => Type::INT8,
        ColumnWire::Float8 => Type::FLOAT8,
        ColumnWire::Text => Type::TEXT,
        ColumnWire::Bytea => Type::BYTEA,
        ColumnWire::TimestampTz => Type::TIMESTAMPTZ,
        ColumnWire::Timestamp => Type::TIMESTAMP,
        ColumnWire::Date => Type::DATE,
        ColumnWire::Time => Type::TIME,
        ColumnWire::Numeric { .. } => Type::NUMERIC,
        ColumnWire::Jsonb => Type::JSONB,
        ColumnWire::Uuid => Type::UUID,
    }
}

/// One cell as an owned ToSql value for binary COPY. `column` names the
/// column in typed errors.
pub(super) fn cell_value(
    wire: ColumnWire,
    array: &dyn Array,
    row: usize,
    column: &str,
) -> Result<Box<dyn ToSql + Sync + Send>, DestError> {
    macro_rules! cast {
        ($ty:ty) => {
            array
                .as_any()
                .downcast_ref::<$ty>()
                .ok_or_else(|| fatal(format!("column `{column}`: array type mismatch")))?
        };
    }
    if array.is_null(row) {
        // Binary COPY checks the ToSql type against the column's wire type — a NULL
        // must still be typed correctly.
        return Ok(match wire {
            ColumnWire::Bool => Box::new(Option::<bool>::None),
            ColumnWire::Int8 => Box::new(Option::<i64>::None),
            ColumnWire::Float8 => Box::new(Option::<f64>::None),
            ColumnWire::Text => Box::new(Option::<String>::None),
            ColumnWire::Bytea => Box::new(Option::<Vec<u8>>::None),
            ColumnWire::TimestampTz => Box::new(Option::<chrono::DateTime<chrono::Utc>>::None),
            ColumnWire::Timestamp => Box::new(Option::<chrono::NaiveDateTime>::None),
            ColumnWire::Date => Box::new(Option::<chrono::NaiveDate>::None),
            ColumnWire::Time => Box::new(Option::<chrono::NaiveTime>::None),
            ColumnWire::Numeric { .. } => Box::new(Option::<NumericWire>::None),
            ColumnWire::Jsonb => Box::new(Option::<JsonbWire>::None),
            ColumnWire::Uuid => Box::new(Option::<UuidWire>::None),
        });
    }
    Ok(match wire {
        ColumnWire::Bool => Box::new(cast!(BooleanArray).value(row)),
        ColumnWire::Int8 => Box::new(cast!(Int64Array).value(row)),
        ColumnWire::Float8 => Box::new(cast!(Float64Array).value(row)),
        ColumnWire::Text => Box::new(cast!(StringArray).value(row).to_owned()),
        ColumnWire::Bytea => Box::new(cast!(BinaryArray).value(row).to_owned()),
        ColumnWire::TimestampTz => {
            let micros = cast!(TimestampMicrosecondArray).value(row);
            Box::new(
                chrono::DateTime::<chrono::Utc>::from_timestamp_micros(micros)
                    .ok_or_else(|| fatal(format!("column `{column}`: timestamp out of range")))?,
            )
        }
        ColumnWire::Timestamp => {
            let micros = cast!(TimestampMicrosecondArray).value(row);
            Box::new(
                chrono::DateTime::from_timestamp_micros(micros)
                    .ok_or_else(|| fatal(format!("column `{column}`: timestamp out of range")))?
                    .naive_utc(),
            )
        }
        ColumnWire::Date => {
            let days = cast!(Date32Array).value(row);
            Box::new(
                chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163)
                    .ok_or_else(|| fatal(format!("column `{column}`: date out of range")))?,
            )
        }
        ColumnWire::Time => {
            let micros = cast!(Time64MicrosecondArray).value(row) as u64;
            Box::new(
                chrono::NaiveTime::from_num_seconds_from_midnight_opt(
                    (micros / 1_000_000) as u32,
                    ((micros % 1_000_000) * 1_000) as u32,
                )
                .ok_or_else(|| fatal(format!("column `{column}`: time out of range")))?,
            )
        }
        ColumnWire::Numeric { scale } => Box::new(NumericWire {
            value: cast!(Decimal128Array).value(row),
            scale,
        }),
        ColumnWire::Jsonb => Box::new(JsonbWire(cast!(StringArray).value(row).to_owned())),
        ColumnWire::Uuid => {
            let text = cast!(StringArray).value(row);
            Box::new(UuidWire(parse_uuid_text(text).ok_or_else(|| {
                fatal(format!(
                    "column `{column}`: `{text}` is not a canonical uuid"
                ))
            })?))
        }
    })
}

// ---- Native wire encoders ----
//
// Hand-rolled mirrors of the SOURCE's binary-COPY decoders: no tokio-postgres
// type features, no new dependencies. The source decoder
// is the round-trip oracle in the tests below.

use bytes::{BufMut, BytesMut};
use tokio_postgres::types::{IsNull, to_sql_checked};

/// Postgres binary `numeric`: i128 at a declared scale → ndigits/weight/
/// sign/dscale + base-10000 digit groups (canonical: leading/trailing zero
/// groups stripped).
#[derive(Debug, Clone, Copy)]
pub(super) struct NumericWire {
    pub value: i128,
    pub scale: u8,
}

pub(super) fn numeric_wire_bytes(value: i128, scale: u8) -> Vec<u8> {
    let negative = value < 0;
    // Overflow-free: a 38-digit i128 times 10^pad exceeds
    // u128::MAX, so grouping happens in the DECIMAL-STRING domain — append
    // the pad zeros textually, then read base-10000 groups off the digits.
    let pad = (4 - (scale as usize % 4)) % 4;
    let mut text = value.unsigned_abs().to_string();
    text.extend(std::iter::repeat_n('0', pad));
    let frac_groups = (scale as usize + pad) / 4;
    // Left-pad to a whole number of 4-digit groups.
    let rem = text.len() % 4;
    if rem != 0 {
        text.insert_str(0, &"0".repeat(4 - rem));
    }
    let mut digits: Vec<u16> = text
        .as_bytes()
        .chunks(4)
        .map(|group| {
            std::str::from_utf8(group)
                .expect("decimal digits are ASCII")
                .parse::<u16>()
                .expect("4 decimal digits fit u16")
        })
        .collect();
    if digits.iter().all(|d| *d == 0) {
        // Canonical zero: no digits, weight 0.
        let mut out = Vec::with_capacity(8);
        out.extend_from_slice(&0i16.to_be_bytes()); // ndigits
        out.extend_from_slice(&0i16.to_be_bytes()); // weight
        out.extend_from_slice(&0u16.to_be_bytes()); // sign (+)
        out.extend_from_slice(&(scale as i16).to_be_bytes()); // dscale
        return out;
    }
    // weight = index of the most significant group relative to the decimal
    // point (units group = 0).
    let mut weight = digits.len() as i32 - 1 - frac_groups as i32;
    // Canonical form: strip trailing zero groups…
    while digits.last() == Some(&0) {
        digits.pop();
    }
    // …and leading zero groups (adjusting weight).
    while digits.first() == Some(&0) {
        digits.remove(0);
        weight -= 1;
    }
    let mut out = Vec::with_capacity(8 + digits.len() * 2);
    out.extend_from_slice(&(digits.len() as i16).to_be_bytes());
    out.extend_from_slice(&(weight as i16).to_be_bytes());
    out.extend_from_slice(&if negative { 0x4000u16 } else { 0x0000u16 }.to_be_bytes());
    out.extend_from_slice(&(scale as i16).to_be_bytes());
    for d in digits {
        out.extend_from_slice(&d.to_be_bytes());
    }
    out
}

impl ToSql for NumericWire {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.put_slice(&numeric_wire_bytes(self.value, self.scale));
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::NUMERIC
    }

    to_sql_checked!();
}

/// Postgres binary `jsonb`: version byte 1 + the UTF-8 document (the exact
/// byte the source's `Decode::JsonbText` strips).
#[derive(Debug, Clone)]
pub(super) struct JsonbWire(pub String);

impl ToSql for JsonbWire {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.put_u8(1);
        out.put_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::JSONB
    }

    to_sql_checked!();
}

/// Postgres binary `uuid`: 16 raw bytes, parsed from the canonical text form
/// the engine ships (LogicalType::Uuid arrives as Utf8).
#[derive(Debug, Clone, Copy)]
pub(super) struct UuidWire(pub [u8; 16]);

pub(super) fn parse_uuid_text(text: &str) -> Option<[u8; 16]> {
    // Accept the same textual forms the SERVER's uuid input accepts:
    // optional urn:uuid: prefix, optional braces, hyphenated or bare hex.
    let text = text
        .strip_prefix("urn:uuid:")
        .or_else(|| text.strip_prefix("URN:UUID:"))
        .unwrap_or(text);
    let text = text
        .strip_prefix('{')
        .and_then(|t| t.strip_suffix('}'))
        .unwrap_or(text);
    let mut bytes = [0u8; 16];
    let mut idx = 0;
    let mut hi: Option<u8> = None;
    let mut hex_seen = 0usize;
    for ch in text.bytes() {
        if ch == b'-' {
            // Hyphens only at group boundaries (8-4-4-4-12), matching the
            // server's grouping rule.
            if !matches!(hex_seen, 8 | 12 | 16 | 20) {
                return None;
            }
            continue;
        }
        hex_seen += 1;
        let nibble = (ch as char).to_digit(16)? as u8;
        match hi {
            None => hi = Some(nibble),
            Some(h) => {
                if idx >= 16 {
                    return None;
                }
                bytes[idx] = (h << 4) | nibble;
                idx += 1;
                hi = None;
            }
        }
    }
    (idx == 16 && hi.is_none()).then_some(bytes)
}

impl ToSql for UuidWire {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.put_slice(&self.0);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::UUID
    }

    to_sql_checked!();
}

#[cfg(all(test, feature = "source"))]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::source::copy_decode::decode_numeric;

    #[test]
    fn numeric_edges_round_trip_through_the_source_decoder() {
        // Zero at every scale, negatives, exact scale boundaries, and values
        // whose base-10000 alignment needs padding.
        for &(value, scale) in &[
            (0i128, 0u8),
            (0, 4),
            (0, 7),
            (1, 0),
            (-1, 0),
            (1, 4),      // 0.0001
            (-1, 4),     // -0.0001
            (12_345, 2), // 123.45
            (-12_345, 2),
            (10_000, 4), // 1.0000
            (99_999_999, 4),
            (1, 38),
            (i128::from(i64::MAX), 0),
            (i128::from(i64::MIN) + 1, 0),
            (123_456_789_012_345_678_901_234_567i128, 9),
            (-123_456_789_012_345_678_901_234_567i128, 9),
            (1_000_000_000_000i128, 12), // exactly 1.000000000000
        ] {
            let wire = numeric_wire_bytes(value, scale);
            let decoded = decode_numeric(&wire, scale)
                .unwrap_or_else(|e| panic!("({value}, {scale}): {}", e.0));
            assert_eq!(decoded, value, "({value}, {scale})");
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 2048, ..ProptestConfig::default() })]

        /// Encode → (source) decode is the identity everywhere the
        /// DECODER can represent the padded accumulation (its own documented
        /// boundary: it accumulates wire digits into i128 before rescaling,
        /// so |v|·10^pad must fit — a pre-existing source limit). The
        /// encoder itself is verified beyond that range structurally below
        /// and against a live server in dest_conformance.
        #[test]
        fn numeric_round_trips(value in any::<i128>(), scale in 0u8..=38) {
            let pad = (4 - (scale as u32 % 4)) % 4;
            let limit = i128::MAX / 10i128.pow(pad);
            let value = value.clamp(-limit, limit);
            let wire = numeric_wire_bytes(value, scale);
            prop_assert_eq!(decode_numeric(&wire, scale).unwrap(), value);
        }
    }

    /// F1 regression: the exact overflow shape — 39-digit magnitudes at
    /// pad-requiring scales. Verified STRUCTURALLY (group-by-group against
    /// the decimal string), since the decoder's own i128 accumulation
    /// cannot represent these.
    #[test]
    fn numeric_wire_is_exact_at_i128_extremes() {
        for &(value, scale) in &[
            (i128::MAX, 3u8),
            (i128::MIN, 3),
            (i128::MAX, 37),
            (i128::MIN + 1, 1),
        ] {
            let wire = numeric_wire_bytes(value, scale);
            let ndigits = i16::from_be_bytes([wire[0], wire[1]]) as usize;
            let weight = i16::from_be_bytes([wire[2], wire[3]]) as i32;
            let sign = u16::from_be_bytes([wire[4], wire[5]]);
            assert_eq!(sign, if value < 0 { 0x4000 } else { 0x0000 });
            // Reconstruct the decimal string from the digit groups and
            // compare against the ground truth rendering of value/10^scale.
            let mut digits = String::new();
            for i in 0..ndigits {
                let d = u16::from_be_bytes([wire[8 + i * 2], wire[9 + i * 2]]);
                digits.push_str(&format!("{d:04}"));
            }
            // The wire value is digits × 10000^(weight − ndigits + 1);
            // rescale to `scale` and compare with the input integer.
            // Decoder identity: value = wire_digits × 10^exp10 (its rescale
            // step) ⇒ compare digits·10^exp10 against |value| as strings.
            let exp10 = 4 * (weight - ndigits as i32 + 1) + scale as i32;
            let mut expected = value.unsigned_abs().to_string();
            match exp10.cmp(&0) {
                std::cmp::Ordering::Greater => {
                    // wire × 10^exp10 == value ⇒ pad the WIRE side.
                    digits.push_str(&"0".repeat(exp10 as usize));
                }
                std::cmp::Ordering::Less => {
                    // value × 10^-exp10 == wire ⇒ pad the VALUE side.
                    expected.push_str(&"0".repeat((-exp10) as usize));
                }
                std::cmp::Ordering::Equal => {}
            }
            let digits_trimmed = digits.trim_start_matches('0');
            let expected_trimmed = expected.trim_start_matches('0');
            assert_eq!(digits_trimmed, expected_trimmed, "({value}, {scale})");
        }
    }

    #[test]
    fn uuid_parses_canonical_and_rejects_garbage() {
        let bytes = parse_uuid_text("550e8400-e29b-41d4-a716-446655440000").expect("canonical");
        assert_eq!(bytes[0], 0x55);
        assert_eq!(bytes[15], 0x00);
        // Uppercase accepted (hex digits case-insensitive).
        assert!(parse_uuid_text("550E8400-E29B-41D4-A716-446655440000").is_some());
        for bad in [
            "",
            "550e8400e29b41d4a716446655440000zz",
            "550e8400-e29b-41d4-a716-44665544000",   // short
            "550e8400-e29b-41d4-a716-4466554400000", // long
            "550e-8400e29b-41d4-a716-446655440000",  // wrong hyphens
            "not-a-uuid-at-all",
        ] {
            assert!(parse_uuid_text(bad).is_none(), "{bad}");
        }
        // Hyphen-less form: also canonical-rejected? PG accepts it, our engine
        // ships hyphenated canonical text — but accept it anyway (32 hex).
        assert!(parse_uuid_text("550e8400e29b41d4a716446655440000").is_some());
        // Server-accepted forms: urn prefix and braces.
        assert!(parse_uuid_text("urn:uuid:550e8400-e29b-41d4-a716-446655440000").is_some());
        assert!(parse_uuid_text("{550e8400-e29b-41d4-a716-446655440000}").is_some());
        assert!(parse_uuid_text("{550e8400e29b41d4a716446655440000}").is_some());
        assert!(parse_uuid_text("urn:uuid:{nope}").is_none());
        assert!(parse_uuid_text("{550e8400-e29b-41d4-a716-446655440000").is_none());
    }
}
