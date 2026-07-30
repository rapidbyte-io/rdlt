//! Arrow columnar building: drained rows → `RecordBatch` per the resolved
//! schema. Values land directly in typed arrays; the `Json` column type stores the
//! verbatim serialized subtree (never dropped, never exploded).
//!
//! Generic over [`JsonView`]: the tree and streaming paths
//! build through the SAME code — identical arrays, bit for bit.

use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder,
        Int64Builder, ListArray, StringBuilder, StructArray, Time64MicrosecondBuilder,
        TimestampMicrosecondBuilder,
    },
    buffer::{NullBuffer, OffsetBuffer},
    datatypes::{DataType, Field, Fields, Schema, TimeUnit},
    error::ArrowError,
    record_batch::RecordBatch,
};
use rdlt_core::{
    ColumnDef, ColumnType, LoadId, LogicalType, RowId, TableSchema, schema::system_columns,
};

use super::{
    DrainRow,
    canon::parse_timestamp_tz,
    view::{JsonView, ValueKind},
};

/// Arrow physical type for a logical type.
fn arrow_scalar_type(ty: LogicalType) -> DataType {
    match ty {
        LogicalType::Bool => DataType::Boolean,
        LogicalType::Int64 => DataType::Int64,
        LogicalType::Float64 => DataType::Float64,
        LogicalType::Decimal { precision, scale } => DataType::Decimal128(precision, scale as i8),
        LogicalType::Utf8 | LogicalType::Uuid | LogicalType::Json => DataType::Utf8,
        LogicalType::Binary => DataType::Binary,
        LogicalType::TimestampTz => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
        }
        LogicalType::TimestampNaive => DataType::Timestamp(TimeUnit::Microsecond, None),
        LogicalType::Date => DataType::Date32,
        LogicalType::Time => DataType::Time64(TimeUnit::Microsecond),
    }
}

pub(crate) fn arrow_column_type(ty: &ColumnType) -> DataType {
    match ty {
        ColumnType::Scalar { scalar } => arrow_scalar_type(*scalar),
        ColumnType::Struct { fields } => DataType::Struct(arrow_fields(fields)),
        ColumnType::ScalarList { item } => {
            DataType::List(Arc::new(Field::new("item", arrow_scalar_type(*item), true)))
        }
    }
}

fn arrow_fields(columns: &[ColumnDef]) -> Fields {
    columns
        .iter()
        .map(|c| Field::new(&c.name, arrow_column_type(&c.column_type), c.nullable))
        .collect()
}

pub(crate) fn arrow_schema(schema: &TableSchema) -> Schema {
    Schema::new(arrow_fields(&schema.columns))
}

/// Build one table's batch. `source_to_normalized` maps source keys to normalized column names
/// (the schema speaks normalized; the drained rows speak source).
///
/// Returns the batch and the number of MISFITS: cells where a present, non-null
/// input produced a NULL output because the value could not be represented under
/// the column's type. Counting them is what keeps the crate's "counted, never
/// silent" rule true for declared (pinned) columns, whose values are never
/// observed and so can never reach the policy layer.
///
/// The count is POSITIONAL — it compares each input against the cell it
/// produced. A difference of totals would be wrong: an explicitly-null value in
/// a scalar-list column produces a valid empty list, so outputs can legitimately
/// outnumber non-null inputs and the subtraction would underflow.
pub(crate) fn build_batch<'v, V: JsonView<'v>>(
    schema: &TableSchema,
    source_to_normalized: &[(String, String)],
    rows: &[DrainRow<V>],
    load_id: &LoadId,
) -> Result<(RecordBatch, u64), ArrowError> {
    let mut misfits: u64 = 0;
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.columns.len());
    for column in &schema.columns {
        let array: ArrayRef = match column.name.as_str() {
            system_columns::LOAD_ID => {
                let mut b = StringBuilder::new();
                for _ in rows {
                    b.append_value(load_id.as_str());
                }
                Arc::new(b.finish())
            }
            system_columns::ID => {
                let mut b = StringBuilder::new();
                let mut hex = [0u8; 64];
                for row in rows {
                    append_hex_id(&mut b, &row.id, &mut hex);
                }
                Arc::new(b.finish())
            }
            system_columns::PARENT_ID => {
                let mut b = StringBuilder::new();
                let mut hex = [0u8; 64];
                for row in rows {
                    match &row.parent_id {
                        Some(id) => append_hex_id(&mut b, id, &mut hex),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            system_columns::ROOT_ID => {
                let mut b = StringBuilder::new();
                let mut hex = [0u8; 64];
                for row in rows {
                    match &row.root_id {
                        Some(id) => append_hex_id(&mut b, id, &mut hex),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            system_columns::POS => {
                let mut b = Int64Builder::new();
                for row in rows {
                    match row.pos {
                        Some(pos) => b.append_value(pos as i64),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            _ => {
                let source_key = source_to_normalized
                    .iter()
                    .find(|(_, normalized)| normalized == &column.name)
                    .map(|(source, _)| source.as_str())
                    .unwrap_or(column.name.as_str());
                let values: Vec<Option<V>> =
                    rows.iter().map(|row| row.top_level(source_key)).collect();
                let array = build_column(&column.column_type, &values);
                let nulls = array.nulls();
                misfits += values
                    .iter()
                    .enumerate()
                    .filter(|(i, v)| {
                        v.as_ref().is_some_and(|v| !v.is_null())
                            && nulls.is_some_and(|n| n.is_null(*i))
                    })
                    .count() as u64;
                array
            }
        };
        arrays.push(array);
    }
    let batch = RecordBatch::try_new(Arc::new(arrow_schema(schema)), arrays)?;
    Ok((batch, misfits))
}

/// Append one lineage id to a string column as lowercase hex, reusing `hex` as scratch
/// so the encoding allocates nothing per row.
fn append_hex_id(b: &mut StringBuilder, id: &RowId, hex: &mut [u8; 64]) {
    id.write_hex(hex);
    b.append_value(std::str::from_utf8(hex).expect("hex is ASCII"));
}

fn build_column<'v, V: JsonView<'v>>(ty: &ColumnType, values: &[Option<V>]) -> ArrayRef {
    match ty {
        ColumnType::Scalar { scalar } => build_scalar(*scalar, values),
        ColumnType::Struct { fields } => {
            let validity: Vec<bool> = values
                .iter()
                .map(|v| v.is_some_and(|v| v.is_object()))
                .collect();
            let child_arrays: Vec<ArrayRef> = fields
                .iter()
                .map(|field| {
                    let projected: Vec<Option<V>> = values
                        .iter()
                        .map(|v| match v {
                            Some(v) if v.is_object() => v.obj_get(&field.name),
                            _ => None,
                        })
                        .collect();
                    build_column(&field.column_type, &projected)
                })
                .collect();
            Arc::new(StructArray::new(
                arrow_fields(fields),
                child_arrays,
                Some(NullBuffer::from(validity)),
            ))
        }
        ColumnType::ScalarList { item } => {
            let mut offsets: Vec<i32> = Vec::with_capacity(values.len() + 1);
            offsets.push(0);
            let mut flat: Vec<Option<V>> = Vec::new();
            let mut validity: Vec<bool> = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    Some(v) if v.is_array() => {
                        flat.extend(v.arr_items().map(Some));
                        validity.push(true);
                    }
                    _ => validity.push(value.is_some()),
                }
                offsets.push(flat.len() as i32);
            }
            let item_array = build_scalar(*item, &flat);
            Arc::new(ListArray::new(
                Arc::new(Field::new("item", arrow_scalar_type(*item), true)),
                OffsetBuffer::new(offsets.into()),
                item_array,
                Some(NullBuffer::from(validity)),
            ))
        }
    }
}

/// The observed [`ValueKind`] of an optional view cell — `None` for an absent cell.
fn view_kind<'v, V: JsonView<'v>>(v: &Option<V>) -> Option<ValueKind<'v>> {
    v.map(JsonView::kind)
}

/// Build one scalar column: dispatch to the per-logical-type builder. Each arm
/// is a small named function so the value semantics of a single type read (and
/// diff) in isolation; the dispatch is the whole story of "which type → which
/// builder".
fn build_scalar<'v, V: JsonView<'v>>(ty: LogicalType, values: &[Option<V>]) -> ArrayRef {
    match ty {
        LogicalType::Bool => scalar_bool(values),
        LogicalType::Int64 => scalar_int64(values),
        LogicalType::Float64 => scalar_float64(values),
        LogicalType::Utf8 | LogicalType::Uuid => scalar_utf8(values),
        LogicalType::Json => scalar_json(values),
        LogicalType::TimestampTz => scalar_timestamp_tz(values),
        LogicalType::TimestampNaive => scalar_timestamp_naive(values),
        LogicalType::Date => scalar_date(values),
        LogicalType::Time => scalar_time(values),
        LogicalType::Decimal { precision, scale } => scalar_decimal(values, precision, scale),
        LogicalType::Binary => scalar_binary(values),
    }
}

fn scalar_bool<'v, V: JsonView<'v>>(values: &[Option<V>]) -> ArrayRef {
    let mut b = BooleanBuilder::new();
    for v in values {
        b.append_option(match view_kind(v) {
            Some(ValueKind::Bool(x)) => Some(x),
            _ => None,
        });
    }
    Arc::new(b.finish())
}

fn scalar_int64<'v, V: JsonView<'v>>(values: &[Option<V>]) -> ArrayRef {
    let mut b = Int64Builder::new();
    for v in values {
        b.append_option(match view_kind(v) {
            Some(ValueKind::Int(i)) => Some(i),
            _ => None,
        });
    }
    Arc::new(b.finish())
}

fn scalar_float64<'v, V: JsonView<'v>>(values: &[Option<V>]) -> ArrayRef {
    let mut b = Float64Builder::new();
    for v in values {
        // Mirrors `Value::as_f64`: any JSON number converts with `as` casts.
        // No `ValueKind::UInt` arm: a u64 observation resolves the column to text, so
        // a UInt can never reach a Float64 column. Converting one here would be
        // the inexact narrowing the Float64 escalation exists to refuse.
        b.append_option(match view_kind(v) {
            Some(ValueKind::Float(f)) => Some(f),
            Some(ValueKind::Int(i)) => Some(i as f64),
            _ => None,
        });
    }
    Arc::new(b.finish())
}

fn scalar_utf8<'v, V: JsonView<'v>>(values: &[Option<V>]) -> ArrayRef {
    // Same semantics as `render_scalar`, minus the per-cell String
    // clone for the dominant case (values that already ARE strings).
    let mut b = StringBuilder::new();
    let mut scratch = String::new();
    for v in values {
        match view_kind(v) {
            Some(ValueKind::Str(s)) => b.append_value(s),
            Some(ValueKind::Bool(x)) => b.append_value(if x { "true" } else { "false" }),
            Some(ValueKind::Int(i)) => {
                scratch.clear();
                std::fmt::Write::write_fmt(&mut scratch, format_args!("{i}"))
                    .expect("write to String");
                b.append_value(&scratch);
            }
            Some(ValueKind::UInt(u)) => {
                scratch.clear();
                std::fmt::Write::write_fmt(&mut scratch, format_args!("{u}"))
                    .expect("write to String");
                b.append_value(&scratch);
            }
            Some(ValueKind::Float(f)) => {
                scratch.clear();
                std::fmt::Write::write_fmt(&mut scratch, format_args!("{f}"))
                    .expect("write to String");
                b.append_value(&scratch);
            }
            Some(ValueKind::Null | ValueKind::Object | ValueKind::Array) | None => b.append_null(),
        }
    }
    Arc::new(b.finish())
}

fn scalar_json<'v, V: JsonView<'v>>(values: &[Option<V>]) -> ArrayRef {
    let mut b = StringBuilder::new();
    for v in values {
        match v {
            Some(value) if !value.is_null() => {
                let mut out = Vec::new();
                write_compact_json(*value, &mut out);
                b.append_value(std::str::from_utf8(&out).expect("serialized JSON is UTF-8"));
            }
            _ => b.append_null(),
        }
    }
    Arc::new(b.finish())
}

fn scalar_timestamp_tz<'v, V: JsonView<'v>>(values: &[Option<V>]) -> ArrayRef {
    let mut b = TimestampMicrosecondBuilder::new();
    for v in values {
        let micros = match view_kind(v) {
            Some(ValueKind::Str(s)) => parse_timestamp_tz(s).map(|dt| dt.timestamp_micros()),
            _ => None,
        };
        b.append_option(micros);
    }
    Arc::new(b.finish().with_timezone("+00:00"))
}

fn scalar_timestamp_naive<'v, V: JsonView<'v>>(values: &[Option<V>]) -> ArrayRef {
    let mut b = TimestampMicrosecondBuilder::new();
    for v in values {
        let micros = match view_kind(v) {
            Some(ValueKind::Str(s)) => {
                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
                    .ok()
                    .map(|dt| dt.and_utc().timestamp_micros())
            }
            _ => None,
        };
        b.append_option(micros);
    }
    Arc::new(b.finish())
}

fn scalar_date<'v, V: JsonView<'v>>(values: &[Option<V>]) -> ArrayRef {
    let mut b = Date32Builder::new();
    for v in values {
        let days =
            match view_kind(v) {
                Some(ValueKind::Str(s)) => chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .ok()
                    .map(|d| {
                        d.signed_duration_since(
                            chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch"),
                        )
                        .num_days() as i32
                    }),
                _ => None,
            };
        b.append_option(days);
    }
    Arc::new(b.finish())
}

fn scalar_time<'v, V: JsonView<'v>>(values: &[Option<V>]) -> ArrayRef {
    let mut b = Time64MicrosecondBuilder::new();
    for v in values {
        let micros = match view_kind(v) {
            Some(ValueKind::Str(s)) => chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
                .ok()
                .map(|t| {
                    i64::from(chrono::Timelike::num_seconds_from_midnight(&t)) * 1_000_000
                        + i64::from(chrono::Timelike::nanosecond(&t) / 1_000)
                }),
            _ => None,
        };
        b.append_option(micros);
    }
    Arc::new(b.finish())
}

fn scalar_decimal<'v, V: JsonView<'v>>(values: &[Option<V>], precision: u8, scale: u8) -> ArrayRef {
    let mut b = Decimal128Builder::new();
    for v in values {
        b.append_option(v.and_then(|v| parse_decimal(v, precision, scale)));
    }
    Arc::new(
        b.finish()
            .with_precision_and_scale(precision, scale as i8)
            .expect("valid decimal precision/scale by lattice construction"),
    )
}

fn scalar_binary<'v, V: JsonView<'v>>(values: &[Option<V>]) -> ArrayRef {
    // Not producible from JSON inference; hinted Binary without an Arrow-native
    // source yields nulls (never a silent lossy decode).
    let mut b = BinaryBuilder::new();
    for _ in values {
        b.append_null();
    }
    Arc::new(b.finish())
}

/// Compact JSON serialization in NATIVE entry order — byte-identical to
/// `serde_json::to_string(&Value)` (preserve_order semantics, serde escaping,
/// itoa/ryu numbers). Used for the `Json` column type's verbatim subtrees.
///
/// DELIBERATELY NOT unified with [`super::canon::canonical_json_bytes`]: the two
/// share every rule (escaping, number rendering, array recursion) EXCEPT key
/// order — this one preserves native insertion order for the stored value, while
/// `canonical_json_bytes` sorts object keys because `_rdlt_id` hashes must be
/// order-independent. Merging them behind a `sort_keys` flag would put persisted
/// identity bytes one boolean away from a silent change; they stay separate on
/// purpose. Any edit to the shared rules must be mirrored in both.
fn write_compact_json<'v, V: JsonView<'v>>(value: V, out: &mut Vec<u8>) {
    match value.kind() {
        ValueKind::Object => {
            out.push(b'{');
            for (i, (key, item)) in value.obj_entries().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                serde_json::to_writer(&mut *out, key).expect("string serialization");
                out.push(b':');
                write_compact_json(item, out);
            }
            out.push(b'}');
        }
        ValueKind::Array => {
            out.push(b'[');
            for (i, item) in value.arr_items().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_compact_json(item, out);
            }
            out.push(b']');
        }
        ValueKind::Null => out.extend_from_slice(b"null"),
        ValueKind::Bool(b) => out.extend_from_slice(if b { b"true" } else { b"false" }),
        ValueKind::Str(s) => serde_json::to_writer(&mut *out, s).expect("string serialization"),
        ValueKind::Int(i) => serde_json::to_writer(&mut *out, &serde_json::Number::from(i))
            .expect("number serialization"),
        ValueKind::UInt(u) => serde_json::to_writer(&mut *out, &serde_json::Number::from(u))
            .expect("number serialization"),
        ValueKind::Float(f) => serde_json::to_writer(
            &mut *out,
            &serde_json::Number::from_f64(f).expect("finite by JSON grammar"),
        )
        .expect("number serialization"),
    }
}

/// Exact decimal parsing from integers or decimal strings; `None` (→ null) for
/// anything that does not fit the declared type — floats are refused by design
/// (no Float64 → Decimal edge), a fraction longer than `scale` would truncate,
/// and a magnitude needing `precision` digits or more cannot be stored at that
/// precision. The last of these is checked HERE because the array builder does
/// not: arrow validates the precision/scale PAIR, never a value against it.
fn parse_decimal<'v, V: JsonView<'v>>(value: V, precision: u8, scale: u8) -> Option<i128> {
    let owned;
    let text = match value.kind() {
        ValueKind::Int(i) => {
            let scaled = (i as i128).checked_mul(10i128.checked_pow(scale as u32)?)?;
            return fits_precision(scaled, precision);
        }
        // u64 beyond i64 and floats: refused (matches Number::as_i64 semantics).
        ValueKind::UInt(_) | ValueKind::Float(_) => return None,
        ValueKind::Str(s) => {
            owned = s;
            owned.trim()
        }
        _ => return None,
    };
    let (sign, digits) = match text.strip_prefix('-') {
        Some(rest) => (-1i128, rest),
        None => (1i128, text),
    };
    let (int_part, frac_part) = match digits.split_once('.') {
        Some((i, f)) => (i, f),
        None => (digits, ""),
    };
    if frac_part.len() > scale as usize {
        return None; // would truncate — inexact
    }
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
        || int_part.is_empty() && frac_part.is_empty()
    {
        return None;
    }
    let mut result: i128 = if int_part.is_empty() {
        0
    } else {
        int_part.parse().ok()?
    };
    result = result.checked_mul(10i128.checked_pow(scale as u32)?)?;
    if !frac_part.is_empty() {
        let frac: i128 = frac_part.parse().ok()?;
        result = result.checked_add(
            frac.checked_mul(10i128.checked_pow((scale as usize - frac_part.len()) as u32)?)?,
        )?;
    }
    fits_precision(sign * result, precision)
}

/// A decimal of `precision` digits holds magnitudes strictly below `10^precision`.
/// Anything at or beyond that cannot be stored at the declared precision, so it
/// is refused by the same rule that refuses an over-long fraction.
fn fits_precision(scaled: i128, precision: u8) -> Option<i128> {
    let limit = 10i128.checked_pow(precision as u32)?;
    (scaled.unsigned_abs() < limit.unsigned_abs()).then_some(scaled)
}
// T131 — the FIRST test module in crates/rdlt-engine/src/shred/build.rs.
// Appended at the end of the file.

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use serde_json::json;

    fn dec(value: serde_json::Value, precision: u8, scale: u8) -> Option<i128> {
        parse_decimal(&value, precision, scale)
    }

    /// The temporal builders each parse a string and then do ARITHMETIC on the
    /// parsed parts, and the arithmetic is what was unpinned: every operator in
    /// `scalar_time`'s micros computation could be swapped without a test
    /// noticing. Exact values, chosen so each operator change lands somewhere
    /// different.
    #[test]
    fn time_of_day_converts_to_exact_microseconds() {
        use arrow::array::Time64MicrosecondArray;
        // 01:02:03 is 3723 seconds; .456789 is 456_789 microseconds.
        let a = scalar_time(&[Some(&json!("01:02:03.456789")), Some(&json!("not a time"))]);
        let t = a
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .expect("time64 array");
        assert_eq!(
            t.value(0),
            3_723_456_789,
            "seconds * 1_000_000 + nanos / 1_000"
        );
        assert!(t.is_null(1), "an unparseable time is NULL, never a guess");

        // Midnight and the last representable microsecond of the day: the
        // boundaries the destination encoders refuse outside of.
        let edges = scalar_time(&[Some(&json!("00:00:00")), Some(&json!("23:59:59.999999"))]);
        let t = edges
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .expect("time64 array");
        assert_eq!(t.value(0), 0);
        assert_eq!(t.value(1), 86_399_999_999);
    }

    /// Deleting the string arm in any temporal builder makes EVERY value NULL —
    /// the column still builds, the run still succeeds, and the data is simply
    /// gone. Nulls are the shape a silent data-loss defect takes here, so each
    /// builder needs a non-null assertion.
    #[test]
    fn temporal_builders_parse_their_string_forms() {
        use arrow::array::{Date32Array, TimestampMicrosecondArray};

        let ts = scalar_timestamp_naive(&[
            Some(&json!("2026-07-27T01:02:03.5")),
            Some(&json!("nonsense")),
        ]);
        let ts = ts
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp array");
        assert!(!ts.is_null(0), "a valid naive timestamp must parse");
        assert_eq!(
            ts.value(0),
            1_785_114_123_500_000,
            "micros since the epoch, UTC"
        );
        assert!(ts.is_null(1));

        let d = scalar_date(&[Some(&json!("1970-01-02")), Some(&json!("nonsense"))]);
        let d = d
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("date array");
        assert_eq!(d.value(0), 1, "days since the epoch");
        assert!(d.is_null(1));
    }

    /// A JSON `null` in a Json column must land as SQL NULL, not as the
    /// four-character string "null". The guard that decides this is the only
    /// thing standing between the two, and they are indistinguishable once
    /// written — a reader cannot tell an absent value from a stored "null".
    #[test]
    fn json_null_is_sql_null_and_entries_stay_separated() {
        use arrow::array::StringArray;
        let a = scalar_json(&[
            Some(&json!(null)),
            Some(&json!({"a": 1, "b": 2})),
            Some(&json!([1, 2])),
        ]);
        let s = a
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 array");
        assert!(
            s.is_null(0),
            "JSON null is SQL NULL, not the string \"null\""
        );
        // Multi-entry containers need their separators: without them the output
        // is not JSON at all, and nothing downstream would parse it back.
        assert_eq!(s.value(1), r#"{"a":1,"b":2}"#);
        assert_eq!(s.value(2), "[1,2]");
    }

    /// The accepted grammar is: optional leading `-`, then ASCII digits with at
    /// most one `.`, surrounded by ignorable whitespace. Everything else is
    /// refused rather than coerced — a decimal column exists precisely because
    /// the caller wants exact values, so a guess is worse than a refusal.
    #[test]
    fn decimal_grammar_accepts_only_exact_fixed_point_text() {
        // Whole and fractional forms, scaled to the declared scale.
        assert_eq!(dec(json!("5"), 10, 2), Some(500));
        assert_eq!(dec(json!("5.00"), 10, 2), Some(500));
        assert_eq!(dec(json!("5.1"), 10, 2), Some(510), "short fraction pads");
        assert_eq!(dec(json!("-5.10"), 10, 2), Some(-510));
        assert_eq!(dec(json!("  5.00  "), 10, 2), Some(500), "whitespace trims");

        // A bare leading or trailing point is still unambiguous.
        assert_eq!(dec(json!(".5"), 10, 2), Some(50), "\".5\" is 0.50");
        assert_eq!(dec(json!("5."), 10, 2), Some(500), "\"5.\" is 5.00");

        // Negative zero collapses to zero, not to a distinct value.
        assert_eq!(dec(json!("-0.00"), 10, 2), Some(0));

        // Refused: the exponent form, an explicit plus, and any non-digit.
        assert_eq!(dec(json!("1e5"), 10, 2), None, "no exponent form");
        assert_eq!(dec(json!("+5"), 10, 2), None, "no explicit plus sign");
        assert_eq!(dec(json!("5,00"), 10, 2), None, "no locale separators");
        assert_eq!(dec(json!("0x10"), 10, 2), None);
        assert_eq!(
            dec(json!("5 00"), 10, 2),
            None,
            "inner whitespace is not a digit"
        );

        // Refused: nothing to parse.
        assert_eq!(dec(json!(""), 10, 2), None);
        assert_eq!(dec(json!("."), 10, 2), None, "a lone point has no digits");
        assert_eq!(dec(json!("-"), 10, 2), None);
        assert_eq!(dec(json!("--5"), 10, 2), None, "one sign only");
    }

    /// A fraction longer than the declared scale would have to be truncated, and
    /// truncating a decimal silently changes the value. Refused, and therefore
    /// counted as a misfit rather than stored wrong.
    #[test]
    fn a_fraction_longer_than_the_scale_is_refused_not_rounded() {
        assert_eq!(dec(json!("1.23"), 10, 2), Some(123));
        assert_eq!(dec(json!("1.234"), 10, 2), None, "would truncate to 1.23");
        assert_eq!(dec(json!("1.200"), 10, 2), None, "even trailing zeros");
        assert_eq!(dec(json!("1.2"), 10, 0), None, "scale 0 admits no fraction");
        assert_eq!(dec(json!("7"), 10, 0), Some(7));
    }

    /// Arrow validates the precision/scale PAIR but never a value against it, so
    /// this bound is enforced here or nowhere. A decimal of `precision` digits
    /// holds magnitudes strictly below `10^precision`.
    #[test]
    fn a_magnitude_at_or_beyond_the_precision_is_refused() {
        assert_eq!(
            dec(json!("99.99"), 4, 2),
            Some(9999),
            "the largest that fits"
        );
        assert_eq!(dec(json!("100.00"), 4, 2), None, "10^4 needs a 5th digit");
        assert_eq!(dec(json!("-100.00"), 4, 2), None, "sign does not buy room");
        assert_eq!(dec(json!("-99.99"), 4, 2), Some(-9999));
        // Integers take the same bound through the same helper.
        assert_eq!(dec(json!(99), 4, 2), Some(9900));
        assert_eq!(dec(json!(100), 4, 2), None);
    }

    /// JSON numbers: integers scale exactly; floats and out-of-i64 unsigneds are
    /// refused by design, because a decimal built from a binary float would
    /// record a value the source never had.
    #[test]
    fn numeric_kinds_that_cannot_be_exact_are_refused() {
        assert_eq!(dec(json!(5), 10, 2), Some(500));
        assert_eq!(dec(json!(-5), 10, 2), Some(-500));
        assert_eq!(dec(json!(0), 10, 2), Some(0));
        assert_eq!(dec(json!(5.1), 10, 2), None, "no Float64 → Decimal edge");
        assert_eq!(
            dec(json!(u64::MAX), 38, 0),
            None,
            "beyond i64: refused, matching Number::as_i64 semantics"
        );
        assert_eq!(dec(json!(true), 10, 2), None, "not a number or a string");
        assert_eq!(dec(json!(null), 10, 2), None);
        assert_eq!(dec(json!({"a": 1}), 10, 2), None);
        assert_eq!(dec(json!([1]), 10, 2), None);
    }

    /// The i128 arithmetic is all checked, so an unrepresentable scale or a
    /// magnitude that overflows on the way to being scaled refuses rather than
    /// wrapping into a plausible wrong number.
    #[test]
    fn overflowing_scale_and_magnitude_refuse_rather_than_wrap() {
        // 10^39 does not fit in i128, so no value can be scaled by it.
        assert_eq!(dec(json!("1"), 38, 39), None, "10^39 overflows i128");
        assert_eq!(dec(json!(1), 38, 39), None);
        // An integer that cannot be scaled into i128 at this scale.
        assert_eq!(dec(json!(i64::MAX), 38, 38), None);
        // …but the same integer at a workable scale and precision is exact.
        assert_eq!(
            dec(json!(i64::MAX), 38, 0),
            Some(i64::MAX as i128),
            "unscaled, i64::MAX is well inside 38 digits"
        );
    }

    #[test]
    fn fits_precision_is_a_strict_bound() {
        assert_eq!(fits_precision(999, 3), Some(999));
        assert_eq!(fits_precision(1000, 3), None, "10^3 needs 4 digits");
        assert_eq!(fits_precision(-999, 3), Some(-999));
        assert_eq!(fits_precision(-1000, 3), None, "the bound is on magnitude");
        assert_eq!(fits_precision(0, 1), Some(0));
        assert_eq!(fits_precision(i128::MIN, 38), None, "no wrap on abs()");
    }
}
