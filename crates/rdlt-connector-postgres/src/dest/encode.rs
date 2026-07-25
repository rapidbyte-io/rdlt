//! Binary-COPY wire encoding: arrow column → Postgres wire type + cell values.

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
    StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use rdlt_connector::DestinationError;
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
    TimestampNaive,
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
) -> Result<ColumnWire, DestinationError> {
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
        DataType::Timestamp(TimeUnit::Microsecond, None) => ColumnWire::TimestampNaive,
        DataType::Date32 => ColumnWire::Date,
        DataType::Time64(TimeUnit::Microsecond) => ColumnWire::Time,
        DataType::Decimal128(_, scale) => ColumnWire::Numeric {
            scale: u8::try_from(*scale)
                .map_err(|_| fatal(format!("negative decimal scale {scale} unsupported")))?,
        },
        other => return Err(fatal(format!("unsupported arrow type for COPY: {other}"))),
    })
}

/// One column of a batch, already downcast, ready to encode any row.
///
/// Built ONCE per column per batch. The enum arm *is* the encode decision, so
/// the per-cell path has no `downcast_ref`, no trait object, and no
/// allocation — the three costs the previous per-cell `Box<dyn ToSql>` design
/// paid on every one of the ~12M cells a 1M-row load encodes.
pub(super) enum ColumnEncoder<'a> {
    Bool(&'a BooleanArray),
    Int8(&'a Int64Array),
    Float8(&'a Float64Array),
    Text(&'a StringArray),
    Bytea(&'a BinaryArray),
    TimestampTz(&'a TimestampMicrosecondArray),
    TimestampNaive(&'a TimestampMicrosecondArray),
    Date(&'a Date32Array),
    Time(&'a Time64MicrosecondArray),
    Numeric {
        array: &'a Decimal128Array,
        scale: u8,
    },
    Jsonb(&'a StringArray),
    Uuid(&'a StringArray),
}

impl<'a> ColumnEncoder<'a> {
    /// Downcast once for the whole column. `column` names the column in typed
    /// errors.
    pub(super) fn new(
        wire: ColumnWire,
        array: &'a dyn Array,
        column: &str,
    ) -> Result<Self, DestinationError> {
        macro_rules! cast {
            ($ty:ty) => {
                array
                    .as_any()
                    .downcast_ref::<$ty>()
                    .ok_or_else(|| fatal(format!("column `{column}`: array type mismatch")))?
            };
        }
        Ok(match wire {
            ColumnWire::Bool => Self::Bool(cast!(BooleanArray)),
            ColumnWire::Int8 => Self::Int8(cast!(Int64Array)),
            ColumnWire::Float8 => Self::Float8(cast!(Float64Array)),
            ColumnWire::Text => Self::Text(cast!(StringArray)),
            ColumnWire::Bytea => Self::Bytea(cast!(BinaryArray)),
            ColumnWire::TimestampTz => Self::TimestampTz(cast!(TimestampMicrosecondArray)),
            ColumnWire::TimestampNaive => Self::TimestampNaive(cast!(TimestampMicrosecondArray)),
            ColumnWire::Date => Self::Date(cast!(Date32Array)),
            ColumnWire::Time => Self::Time(cast!(Time64MicrosecondArray)),
            ColumnWire::Numeric { scale } => Self::Numeric {
                array: cast!(Decimal128Array),
                scale,
            },
            ColumnWire::Jsonb => Self::Jsonb(cast!(StringArray)),
            ColumnWire::Uuid => Self::Uuid(cast!(StringArray)),
        })
    }

    /// Append one cell as a complete binary-COPY *field*: an `i32` byte
    /// length followed by the value bytes, or `-1` alone for NULL.
    ///
    /// Field shape lives here; TUPLE and STREAM framing (field count, header,
    /// trailer) lives in the caller — the encoder owns how a value looks, the
    /// session owns how the stream is assembled.
    ///
    /// The old design needed a typed NULL (`Option::<i64>::None` and friends)
    /// because `to_sql_checked` validates the value's type against the
    /// column's wire type. Writing the field ourselves, a NULL is a bare `-1`
    /// with no type to get wrong.
    #[inline]
    pub(super) fn encode_field(
        &self,
        row: usize,
        column: &str,
        out: &mut BytesMut,
    ) -> Result<(), DestinationError> {
        /// A length-prefixed value whose prefix is backfilled once the value's
        /// own length is known. NULL never reaches here — the macro emits its
        /// bare `-1` and returns — so there is exactly one place that decides
        /// what a NULL looks like on the wire. Generic over the writer, not
        /// `dyn`: each arm monomorphizes, so the value encoding inlines into
        /// this frame instead of paying an indirect call on every cell.
        #[inline(always)]
        fn field<W>(out: &mut BytesMut, column: &str, write: W) -> Result<(), DestinationError>
        where
            W: FnOnce(&mut BytesMut) -> Result<(), DestinationError>,
        {
            let start = out.len();
            out.put_i32(0); // length placeholder, backfilled below
            write(out)?;
            let len = i32::try_from(out.len() - start - 4)
                .map_err(|_| fatal(format!("column `{column}`: value exceeds 2 GiB")))?;
            out[start..start + 4].copy_from_slice(&len.to_be_bytes());
            Ok(())
        }

        macro_rules! field {
            ($array:expr, $write:expr) => {{
                if $array.is_null(row) {
                    out.put_i32(-1);
                    return Ok(());
                }
                field(out, column, $write)
            }};
        }

        /// `ToSql::to_sql` on a CONCRETE type: monomorphic, inlinable, and
        /// the same bytes the driver would have written. Errors are mapped
        /// through the typed constructor, never matched on.
        fn sql<T: ToSql>(
            value: &T,
            ty: &Type,
            column: &str,
            out: &mut BytesMut,
        ) -> Result<(), DestinationError> {
            value
                .to_sql(ty, out)
                .map(|_| ())
                .map_err(|e| fatal(format!("column `{column}`: {e}")))
        }

        match *self {
            Self::Bool(a) => field!(a, |o: &mut BytesMut| sql(
                &a.value(row),
                &Type::BOOL,
                column,
                o
            )),
            Self::Int8(a) => field!(a, |o: &mut BytesMut| sql(
                &a.value(row),
                &Type::INT8,
                column,
                o
            )),
            Self::Float8(a) => field!(a, |o: &mut BytesMut| sql(
                &a.value(row),
                &Type::FLOAT8,
                column,
                o
            )),
            // Borrowed: no `String::to_owned` per cell (`&str: ToSql` writes
            // the same UTF-8 bytes an owned String would).
            Self::Text(a) => field!(a, |o: &mut BytesMut| sql(
                &a.value(row),
                &Type::TEXT,
                column,
                o
            )),
            Self::Bytea(a) => field!(a, |o: &mut BytesMut| sql(
                &a.value(row),
                &Type::BYTEA,
                column,
                o
            )),
            Self::TimestampTz(a) => field!(a, |o: &mut BytesMut| {
                let micros = a.value(row);
                let ts = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(micros)
                    .ok_or_else(|| fatal(format!("column `{column}`: timestamp out of range")))?;
                sql(&ts, &Type::TIMESTAMPTZ, column, o)
            }),
            Self::TimestampNaive(a) => field!(a, |o: &mut BytesMut| {
                let micros = a.value(row);
                let ts = chrono::DateTime::from_timestamp_micros(micros)
                    .ok_or_else(|| fatal(format!("column `{column}`: timestamp out of range")))?
                    .naive_utc();
                sql(&ts, &Type::TIMESTAMP, column, o)
            }),
            Self::Date(a) => field!(a, |o: &mut BytesMut| {
                let days = a.value(row);
                let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163)
                    .ok_or_else(|| fatal(format!("column `{column}`: date out of range")))?;
                sql(&date, &Type::DATE, column, o)
            }),
            Self::Time(a) => field!(a, |o: &mut BytesMut| {
                let micros = a.value(row) as u64;
                let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(
                    (micros / 1_000_000) as u32,
                    ((micros % 1_000_000) * 1_000) as u32,
                )
                .ok_or_else(|| fatal(format!("column `{column}`: time out of range")))?;
                sql(&time, &Type::TIME, column, o)
            }),
            Self::Numeric { array, scale } => field!(array, |o: &mut BytesMut| {
                write_numeric(array.value(row), scale, o);
                Ok(())
            }),
            Self::Jsonb(a) => field!(a, |o: &mut BytesMut| {
                // Version byte 1 + the UTF-8 document — the exact byte the
                // source's `Decode::JsonbText` strips.
                o.put_u8(1);
                o.put_slice(a.value(row).as_bytes());
                Ok(())
            }),
            Self::Uuid(a) => field!(a, |o: &mut BytesMut| {
                let text = a.value(row);
                let bytes = parse_uuid_text(text).ok_or_else(|| {
                    fatal(format!(
                        "column `{column}`: `{text}` is not a canonical uuid"
                    ))
                })?;
                o.put_slice(&bytes);
                Ok(())
            }),
        }
    }
}

// ---- Native wire encoders ----
//
// Hand-rolled mirrors of the SOURCE's binary-COPY decoders: no new
// dependencies. The source decoder is the round-trip oracle in the tests
// below, and `tests/fixtures/pg_copy_values.hex` pins the bytes literally.

use bytes::{BufMut, BytesMut};

/// Postgres binary `numeric`: i128 at a declared scale → ndigits/weight/
/// sign/dscale + base-10⁴ digit groups (canonical: leading/trailing zero
/// groups stripped), appended in place.
///
/// Hand-written deliberately: `postgres-protocol` exposes no `numeric_to_sql`;
/// `rust_decimal`'s 96-bit mantissa cannot hold `Decimal128`'s 38 digits; and
/// `bigdecimal` allocates per value, which is the cost this exists to remove.
///
/// Grouping is by integer divmod, NOT via a decimal string. The obvious
/// route — multiply by 10^pad to align the fraction to a group boundary,
/// then take `% 10000` — overflows: a 39-digit i128 times 10³ exceeds
/// u128::MAX. Instead the pad is folded into the FIRST group only
/// (`(v % 10^(4−pad)) × 10^pad`, both factors bounded so the product stays
/// under 10⁴), after which dividing by `10^(4−pad)` leaves a value whose
/// remaining groups are plain `% 10000` steps. Same digits, no wide
/// intermediate, no allocation.
pub(super) fn write_numeric(value: i128, scale: u8, out: &mut BytesMut) {
    let pad = u32::from(scale) % 4;
    let pad = (4 - pad) % 4;
    let low_div = 10u128.pow(4 - pad);

    // Base-10⁴ groups, least-significant first. 16 is ample: an i128
    // magnitude is at most 39 digits, plus 3 pad digits, is 11 groups.
    let mut groups = [0u16; 16];
    let mut count = 0usize;
    let mut v = value.unsigned_abs();

    let first = (v % low_div) * 10u128.pow(pad);
    groups[count] = u16::try_from(first).expect("a base-10^4 group is < 10000");
    count += 1;
    v /= low_div;
    while v > 0 {
        groups[count] = u16::try_from(v % 10_000).expect("a base-10^4 group is < 10000");
        count += 1;
        v /= 10_000;
    }

    let scale16 = i16::try_from(scale).unwrap_or(i16::MAX);
    if count == 1 && groups[0] == 0 {
        // Canonical zero: no digits, weight 0.
        out.put_i16(0); // ndigits
        out.put_i16(0); // weight
        out.put_u16(0); // sign (+)
        out.put_i16(scale16); // dscale
        return;
    }

    // weight = index of the most significant group relative to the decimal
    // point (units group = 0).
    let frac_groups = (i32::from(scale) + pad as i32) / 4;
    let weight = count as i32 - 1 - frac_groups;

    // Canonical form strips trailing zero groups — the LEAST significant end,
    // which is the FRONT of this array. Leading zero groups cannot occur: the
    // loop stops as soon as `v` is exhausted, so the last group written is
    // always non-zero (the all-zero case returned above).
    let mut lo = 0usize;
    while lo < count && groups[lo] == 0 {
        lo += 1;
    }
    let ndigits = count - lo;

    out.put_i16(i16::try_from(ndigits).expect("at most 11 groups"));
    out.put_i16(i16::try_from(weight).unwrap_or(i16::MAX));
    out.put_u16(if value < 0 { 0x4000 } else { 0x0000 });
    out.put_i16(scale16);
    for idx in (lo..count).rev() {
        out.put_u16(groups[idx]);
    }
}

/// Postgres binary `uuid`: 16 raw bytes, parsed from the canonical text form
/// the engine ships (LogicalType::Uuid arrives as Utf8).
///
/// The `uuid` crate is deliberately NOT adopted: it appears nowhere in the
/// measured profile, so the "measured-better" test for taking a dependency is
/// unmet, and `Uuid::try_parse` accepts a strictly narrower set of texts than
/// both this parser and PostgreSQL's own `uuid_in` — swapping it in would
/// silently start rejecting rows the server would have accepted.
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
    let mut hyphen_at: Option<usize> = None;
    for ch in text.bytes() {
        if ch == b'-' {
            // Hyphens only at group boundaries (8-4-4-4-12), matching the
            // server's grouping rule — and at most ONE per boundary, so
            // "550e8400--e29b-…" is rejected the way the server rejects it.
            // Without the second guard, `hex_seen` alone still reads as a
            // boundary for the repeat.
            if !matches!(hex_seen, 8 | 12 | 16 | 20) || hyphen_at == Some(hex_seen) {
                return None;
            }
            hyphen_at = Some(hex_seen);
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
            let mut wire = BytesMut::new();
            write_numeric(value, scale, &mut wire);
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
            let mut wire = BytesMut::new();
            write_numeric(value, scale, &mut wire);
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
            let mut wire = BytesMut::new();
            write_numeric(value, scale, &mut wire);
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
        // A REPEATED hyphen at one boundary: `hex_seen` alone still reads as
        // a legal boundary for the second one, so a boundary consumed twice
        // has to be tracked. The server rejects these; so must we.
        for repeated in [
            "550e8400--e29b-41d4-a716-446655440000",
            "550e8400-e29b--41d4-a716-446655440000",
            "550e8400-e29b-41d4-a716--446655440000",
            "550e8400---e29b-41d4-a716-446655440000",
        ] {
            assert!(parse_uuid_text(repeated).is_none(), "{repeated}");
        }
        // …while one hyphen per boundary stays accepted, including partial
        // hyphenation, which the server also accepts.
        assert!(parse_uuid_text("550e8400-e29b41d4a716446655440000").is_some());
        assert!(parse_uuid_text("550e8400e29b41d4-a716-446655440000").is_some());
        assert!(parse_uuid_text("{550e8400-e29b-41d4-a716-446655440000").is_none());
    }
}

/// Encoder tests that need no source decoder, so they run even when the crate
/// is built with `--no-default-features --features dest`.
#[cfg(test)]
mod encoder {
    use super::*;
    use arrow_array::{Int64Array, StringArray, TimestampMicrosecondArray};

    fn field_bytes(encoder: &ColumnEncoder<'_>, row: usize) -> Result<BytesMut, DestinationError> {
        let mut out = BytesMut::new();
        encoder.encode_field(row, "c", &mut out)?;
        Ok(out)
    }

    #[test]
    fn null_is_a_bare_minus_one_with_no_value_bytes() {
        let array = Int64Array::from(vec![None, Some(7)]);
        let encoder = ColumnEncoder::new(ColumnWire::Int8, &array, "c").expect("encoder");
        assert_eq!(&field_bytes(&encoder, 0).unwrap()[..], &(-1i32).to_be_bytes());
        // …and a present value carries its length, so NULL and a zero-length
        // value can never be confused on the wire.
        let present = field_bytes(&encoder, 1).unwrap();
        assert_eq!(&present[..4], &8i32.to_be_bytes());
        assert_eq!(&present[4..], &7i64.to_be_bytes());
    }

    #[test]
    fn an_empty_string_is_length_zero_not_null() {
        let array = StringArray::from(vec![Some(""), None]);
        let encoder = ColumnEncoder::new(ColumnWire::Text, &array, "c").expect("encoder");
        assert_eq!(&field_bytes(&encoder, 0).unwrap()[..], &0i32.to_be_bytes());
        assert_eq!(&field_bytes(&encoder, 1).unwrap()[..], &(-1i32).to_be_bytes());
    }

    /// FR-021: an unrepresentable value returns a FATAL typed error. The
    /// caller then drops the `CopyInSink` without `finish()`, which is
    /// tokio-postgres' abort protocol — so a partially written buffer is
    /// never anything the server sees.
    #[test]
    fn unrepresentable_values_are_fatal_and_name_the_column() {
        let bad_uuid = StringArray::from(vec![Some("not-a-uuid")]);
        let encoder = ColumnEncoder::new(ColumnWire::Uuid, &bad_uuid, "c").expect("encoder");
        // Asserted on the VARIANT, never on the rendered text (Principle V).
        assert!(matches!(
            field_bytes(&encoder, 0),
            Err(DestinationError::Fatal(_))
        ));

        let far_future = TimestampMicrosecondArray::from(vec![Some(i64::MAX)]);
        let encoder =
            ColumnEncoder::new(ColumnWire::TimestampTz, &far_future, "c").expect("encoder");
        assert!(matches!(
            field_bytes(&encoder, 0),
            Err(DestinationError::Fatal(_))
        ));
    }

    /// The array a column was downcast from must match the wire decision.
    /// Caught ONCE per column now instead of once per cell — same error,
    /// raised earlier.
    #[test]
    fn a_column_type_mismatch_is_caught_at_construction() {
        let array = Int64Array::from(vec![Some(1)]);
        assert!(matches!(
            ColumnEncoder::new(ColumnWire::Text, &array, "c").map(|_| ()),
            Err(DestinationError::Fatal(_))
        ));
    }
}
