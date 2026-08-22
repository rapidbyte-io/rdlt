//! Arrow columnar building: resolved rows → `RecordBatch` per the resolved
//! schema. Values land directly in typed arrays; the `Json` column type stores the
//! verbatim serialized subtree (never dropped, never exploded).
//!
//! Generic over [`JsonView`]: the arena and the `&serde_json::Value` test
//! view build through the SAME code — identical arrays, bit for bit.
//!
//! One concept despite its length: the closed type lattice needs one builder
//! arm per logical type, and splitting arms across files would put the lattice
//! in two places with nothing to keep them agreeing.

use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder,
        Int64Builder, ListArray, StringBuilder, StructArray, Time64MicrosecondBuilder,
        TimestampMicrosecondBuilder,
    },
    buffer::{NullBuffer, OffsetBuffer},
    datatypes::Field,
    error::ArrowError,
    record_batch::RecordBatch,
};
use rdlt_core::id::LoadId;
use rdlt_core::schema::{self, ColumnType, TableSchema};
use rdlt_core::types::LogicalType;

use super::canonical::parse_timestamp_tz;
use super::infer::int64_fits_in_f64;
use super::resolve::Row;
use super::types::{arrow_fields, arrow_scalar_type, arrow_schema};
use super::view::{JsonView, ValueKind};
use crate::identity::RowId;

/// Build one table's batch. `normalized_to_source` maps normalized column
/// names back to source keys (the schema speaks normalized; the rows speak
/// source) — the table buffer's map, so each column's lookup is O(1) rather
/// than a linear scan of the pairing list.
///
/// Returns the batch and the number of MISFITS: cells where a present, non-null
/// input produced a NULL output because the value could not be represented under
/// the column's type — at every nesting depth (a struct field, a list element),
/// since a nested builder nulls a misfit exactly like a top-level one. Counting
/// them is what keeps the crate's "counted, never silent" rule true for declared
/// (pinned) columns, whose values are never observed and so can never reach the
/// policy layer.
///
/// The count is POSITIONAL — it compares each input against the cell it
/// produced. A difference of totals would be wrong: an explicitly-null value in
/// a scalar-list column produces a valid empty list, so outputs can legitimately
/// outnumber non-null inputs and the subtraction would underflow.
pub(crate) fn build_batch<'v, V: JsonView<'v>>(
    schema: &TableSchema,
    normalized_to_source: &std::collections::HashMap<String, String>,
    rows: &[Row<V>],
    load_id: &LoadId,
) -> Result<(RecordBatch, u64), ArrowError> {
    let mut misfits: u64 = 0;
    let data_columns = schema
        .columns
        .iter()
        .filter(|column| !schema::system::is_system(column.name.as_str()))
        .count();
    let indexes: Vec<Option<Vec<(&'v str, V)>>> = rows
        .iter()
        .map(|row| row_index(row, data_columns))
        .collect();
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.columns.len());
    for column in &schema.columns {
        let array: ArrayRef = match column.name.as_str() {
            schema::system::LOAD_ID => {
                let mut b = StringBuilder::new();
                for _ in rows {
                    b.append_value(load_id.as_str());
                }
                Arc::new(b.finish())
            }
            schema::system::ID => {
                let mut b = StringBuilder::new();
                let mut hex = [0u8; 64];
                for row in rows {
                    append_hex_id(&mut b, &row.id, &mut hex);
                }
                Arc::new(b.finish())
            }
            schema::system::PARENT_ID => {
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
            schema::system::ROOT_ID => {
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
            schema::system::POS => {
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
                let source_key = normalized_to_source
                    .get(column.name.as_str())
                    .map(String::as_str)
                    .unwrap_or(column.name.as_str());
                let values: Vec<Option<V>> = rows
                    .iter()
                    .zip(&indexes)
                    .map(|(row, index)| match index {
                        Some(index) => {
                            if row.nulled.contains(source_key) {
                                None
                            } else {
                                index
                                    .binary_search_by(|(key, _)| (*key).cmp(source_key))
                                    .ok()
                                    .map(|at| index[at].1)
                            }
                        }
                        None => row.top_level(source_key),
                    })
                    .collect();
                build_column(&column.column_type, &values, &mut misfits)
            }
        };
        arrays.push(array);
    }
    let batch = RecordBatch::try_new(Arc::new(arrow_schema(schema)), arrays)?;
    Ok((batch, misfits))
}

/// The object width from which a row is indexed before extraction, and
/// the table width it takes to make that worthwhile.
///
/// Extraction asks every row for every column, and an object's own
/// lookup answers by scanning its entries when it is narrower than the
/// arena's persisted-index width — priced per OBJECT, which is right
/// for a row read a few times and wrong for one read once per column
/// of a wide table: a row of a hundred keys against four thousand
/// columns pays a hundred compares four thousand times over, and the
/// cell budget, which bounds columns × rows, never sees the multiplier.
/// Sorting a row's keys once costs its width, after which each column
/// is a binary search; below this width the scan is the cheaper of the
/// two and its product with the cell budget stays proportionate.
const INDEX_ROW_FROM: usize = 8;

/// A row's entries sorted by key — its keys are unique by the view
/// contract — when the row and the table are both wide enough for the
/// sort to pay (see [`INDEX_ROW_FROM`]); `None` when the plain lookup
/// is the cheaper answer.
fn row_index<'v, V: JsonView<'v>>(row: &Row<V>, data_columns: usize) -> Option<Vec<(&'v str, V)>> {
    entry_index(row.value.obj_entries(), data_columns)
}

/// [`row_index`]'s rule over any object's entries, for any width of
/// consumer — a row against the table, a nested object against its
/// struct type.
fn entry_index<'v, V: JsonView<'v>>(
    entries: impl Iterator<Item = (&'v str, V)>,
    consumer_width: usize,
) -> Option<Vec<(&'v str, V)>> {
    if consumer_width <= INDEX_ROW_FROM {
        return None;
    }
    // The width is known before anything is collected (the views'
    // entry iterators are exact), so a narrow object costs no
    // allocation here — the common row on the hot path.
    if entries.size_hint().0 < INDEX_ROW_FROM {
        return None;
    }
    let mut entries: Vec<(&'v str, V)> = entries.collect();
    if entries.len() < INDEX_ROW_FROM {
        return None;
    }
    entries.sort_unstable_by_key(|(key, _)| *key);
    Some(entries)
}

/// Append one lineage id to a string column as lowercase hex, reusing `hex` as scratch
/// so the encoding allocates nothing per row.
fn append_hex_id(b: &mut StringBuilder, id: &RowId, hex: &mut [u8; 64]) {
    id.write_hex(hex);
    b.append_value(std::str::from_utf8(hex).expect("hex is ASCII"));
}

/// Build one column of any type; `misfits` accumulates the positional
/// misfit count of this column and every nested one under it.
fn build_column<'v, V: JsonView<'v>>(
    ty: &ColumnType,
    values: &[Option<V>],
    misfits: &mut u64,
) -> ArrayRef {
    let array = match ty {
        ColumnType::Scalar { scalar } => build_scalar(*scalar, values),
        ColumnType::Struct { fields } => {
            let validity: Vec<bool> = values
                .iter()
                .map(|v| v.is_some_and(|v| v.is_object()))
                .collect();
            // The top-level rule one level down: a nested object narrow
            // enough to answer lookups by scanning is asked once per
            // FIELD of the struct type, and a struct is one column to
            // the cell budget — so the object is indexed once when it
            // and the type are both wide enough for that to pay.
            let indexes: Vec<Option<Vec<(&'v str, V)>>> = values
                .iter()
                .map(|v| match v {
                    Some(v) if v.is_object() => entry_index(v.obj_entries(), fields.len()),
                    _ => None,
                })
                .collect();
            let child_arrays: Vec<ArrayRef> = fields
                .iter()
                .map(|field| {
                    let projected: Vec<Option<V>> = values
                        .iter()
                        .zip(&indexes)
                        .map(|(v, index)| match (v, index) {
                            (Some(_), Some(index)) => index
                                .binary_search_by(|(key, _)| (*key).cmp(field.name.as_str()))
                                .ok()
                                .map(|at| index[at].1),
                            (Some(v), None) if v.is_object() => v.obj_get(&field.name),
                            _ => None,
                        })
                        .collect();
                    build_column(&field.column_type, &projected, misfits)
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
            *misfits += count_misfits(&flat, item_array.as_ref());
            Arc::new(ListArray::new(
                Arc::new(Field::new("item", arrow_scalar_type(*item), true)),
                OffsetBuffer::new(offsets.into()),
                item_array,
                Some(NullBuffer::from(validity)),
            ))
        }
    };
    *misfits += count_misfits(values, array.as_ref());
    array
}

/// Positional misfits of one built array: cells where a present, non-null
/// input became a NULL output.
fn count_misfits<'v, V: JsonView<'v>>(
    values: &[Option<V>],
    array: &dyn arrow::array::Array,
) -> u64 {
    let Some(nulls) = array.nulls() else {
        return 0;
    };
    values
        .iter()
        .enumerate()
        .filter(|(i, v)| v.as_ref().is_some_and(|v| !v.is_null()) && nulls.is_null(*i))
        .count() as u64
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
        // Mirrors `Value::as_f64` where lossless, and refuses where not:
        // an Int beyond ±2^53 has no exact f64, and the column's own
        // contract is losslessness at runtime, never assumed — the
        // inference path escalates the same value to Utf8; a HINT-pinned
        // column never observes, so the check lives at the build arm,
        // rendering the value a counted misfit instead of a silent
        // 1-ulp alteration. No `ValueKind::UInt` arm: a u64 observation
        // resolves the column to text, so a UInt can never reach a
        // Float64 column.
        b.append_option(match view_kind(v) {
            Some(ValueKind::Float(f)) => Some(f),
            Some(ValueKind::Int(i)) if int64_fits_in_f64(i) => Some(i as f64),
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

/// True when a temporal literal's fractional-second digits fit the
/// engine's microsecond canonical unit: chrono's `%.f` accepts arbitrary
/// precision and the builders convert through nanoseconds, so 7-9 fraction
/// digits would silently truncate — the same inexactness class
/// `parse_decimal` refuses as a counted misfit, and temporal parsing counts
/// it the same way.
fn fraction_within_micros(literal: &str) -> bool {
    let Some(dot) = literal.find(['.', ',']) else {
        return true;
    };
    let digits = literal[dot + 1..]
        .bytes()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    digits <= 6
}

fn scalar_timestamp_tz<'v, V: JsonView<'v>>(values: &[Option<V>]) -> ArrayRef {
    let mut b = TimestampMicrosecondBuilder::new();
    for v in values {
        let micros = match view_kind(v) {
            Some(ValueKind::Str(s)) if fraction_within_micros(s) => {
                parse_timestamp_tz(s).map(|dt| dt.timestamp_micros())
            }
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
            Some(ValueKind::Str(s)) if fraction_within_micros(s) => {
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
            Some(ValueKind::Str(s)) if fraction_within_micros(s) => {
                chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
                    .ok()
                    .map(|t| {
                        i64::from(chrono::Timelike::num_seconds_from_midnight(&t)) * 1_000_000
                            + i64::from(chrono::Timelike::nanosecond(&t) / 1_000)
                    })
            }
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
/// DELIBERATELY NOT unified with [`super::canonical::canonical_json_bytes`]: the two
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

    /// A temporal literal carrying sub-microsecond fraction digits is a
    /// COUNTED MISFIT, never a silently truncated value —
    /// the same discipline `parse_decimal` applies to over-scale
    /// fractions. Six digits (microseconds exactly) parse.
    #[test]
    fn temporal_builders_count_sub_microsecond_fractions_as_misfits() {
        use arrow::array::Time64MicrosecondArray;

        let array = scalar_time(&[
            Some(&json!("01:02:03.456789")),  // six digits: exact µs
            Some(&json!("01:02:03.4567891")), // seven: sub-µs, misfit
            Some(&json!("01:02:03")),         // no fraction at all
        ]);
        let t = array
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .expect("time array");
        assert_eq!(t.value(0), 3_723_456_789, "01:02:03.456789 is exact µs");
        assert!(
            t.is_null(1),
            "a 7-digit fraction is a misfit, not a truncation"
        );
        assert_eq!(t.value(2), 3_723_000_000, "no fraction parses");
    }

    /// A HINT-pinned Float64 column builds a beyond-±2^53 integer
    /// as a COUNTED MISFIT (null output, non-null input — the same
    /// discipline a wrong-typed value gets), never a silent 1-ulp
    /// rounding. The inference path escalates the same value to Utf8;
    /// the pinned column cannot observe, so the check lives here.
    #[test]
    fn float64_builder_refuses_inexact_integers_as_misfits() {
        use arrow::array::Float64Array;

        let array = scalar_float64(&[
            Some(&json!(9007199254740993i64)), // 2^53 + 1: no exact f64
            Some(&json!(9007199254740992i64)), // 2^53: the last exact one
            Some(&json!(1.5)),
            Some(&json!("text")), // an ordinary misfit for contrast
        ]);
        let f = array
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float64 array");
        assert!(
            f.is_null(0),
            "2^53+1 is a misfit, not a silent rounding to 2^53"
        );
        assert_eq!(f.value(1), 9007199254740992.0, "2^53 itself is exact");
        assert_eq!(f.value(2), 1.5);
        assert!(f.is_null(3), "a string remains an ordinary misfit");
    }
}

#[cfg(test)]
mod extraction_cost_tests {
    use std::collections::BTreeSet;

    use rdlt_core::id::{LoadId, TableName};
    use rdlt_core::schema::{Column, Provenance};

    use super::*;
    use crate::identity::RowId;

    /// Narrow rows against a wide table are extracted in time
    /// proportionate to the cells, not to cells × row width. The
    /// arena's own lookup meter is the witness: rows just under its
    /// persisted-index width would answer every column with a full
    /// scan, and the build asks once per column per row — a product the
    /// cell budget cannot see. Indexed at build, each row costs its
    /// width once and a logarithm per column after.
    #[test]
    fn wide_tables_do_not_rescan_narrow_rows_per_column() {
        const WIDTH: usize = 100;
        const COLUMNS: usize = 2_000;
        const ROWS: usize = 50;
        let mut arena = crate::shred::arena::Arena::default();
        let mut text = Vec::new();
        for _ in 0..ROWS {
            let object: Vec<String> = (0..WIDTH).map(|k| format!("\"k{k}\":{k}")).collect();
            text.extend_from_slice(format!("{{{}}}\n", object.join(",")).as_bytes());
        }
        let ids = arena
            .parse_rows(
                &text,
                rdlt_connector::channel::MAX_RECORD_BATCH_ROWS,
                rdlt_connector::channel::MAX_JSON_VALUES_PER_PUSH,
            )
            .expect("the fixture parses");
        let rows: Vec<Row<_>> = ids
            .into_iter()
            .map(|id| Row {
                value: arena.node(id),
                id: RowId::from_bytes([0u8; 32]),
                parent_id: None,
                root_id: None,
                pos: None,
                nulled: BTreeSet::new(),
            })
            .collect();
        let schema = TableSchema {
            table: TableName::new("wide"),
            parent: None,
            columns: (0..COLUMNS)
                .map(|c| Column {
                    name: format!("k{c}"),
                    column_type: ColumnType::scalar(LogicalType::Int64),
                    nullable: true,
                    provenance: Provenance::Inferred,
                })
                .collect(),
        };
        let before = arena.obj_probes();
        let (batch, misfits) = build_batch(
            &schema,
            &std::collections::HashMap::new(),
            &rows,
            &LoadId::new("l"),
        )
        .expect("builds");
        let probes = arena.obj_probes() - before;
        assert_eq!(
            (batch.num_rows(), batch.num_columns(), misfits),
            (ROWS, COLUMNS, 0)
        );
        // The present columns carry their values: the index answers what
        // the scan did.
        let first = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("int64");
        assert_eq!(first.value(0), 0);
        assert_eq!(
            probes,
            0,
            "no column lookup reached the arena's scan: {probes} probes for {} lookups",
            ROWS * COLUMNS
        );
    }

    /// The same bound one level down: narrow nested objects against a
    /// wide struct type are projected without a scan per field.
    #[test]
    fn wide_struct_types_do_not_rescan_narrow_nested_objects_per_field() {
        const WIDTH: usize = 100;
        const FIELDS: usize = 2_000;
        const ROWS: usize = 50;
        let mut arena = crate::shred::arena::Arena::default();
        let mut text = Vec::new();
        for _ in 0..ROWS {
            let object: Vec<String> = (0..WIDTH).map(|k| format!("\"f{k}\":{k}")).collect();
            text.extend_from_slice(format!("{{\"s\":{{{}}}}}\n", object.join(",")).as_bytes());
        }
        let ids = arena
            .parse_rows(
                &text,
                rdlt_connector::channel::MAX_RECORD_BATCH_ROWS,
                rdlt_connector::channel::MAX_JSON_VALUES_PER_PUSH,
            )
            .expect("the fixture parses");
        let rows: Vec<Row<_>> = ids
            .into_iter()
            .map(|id| Row {
                value: arena.node(id),
                id: RowId::from_bytes([0u8; 32]),
                parent_id: None,
                root_id: None,
                pos: None,
                nulled: BTreeSet::new(),
            })
            .collect();
        let schema = TableSchema {
            table: TableName::new("nested"),
            parent: None,
            columns: vec![Column {
                name: "s".into(),
                column_type: ColumnType::Struct {
                    fields: (0..FIELDS)
                        .map(|f| Column {
                            name: format!("f{f}"),
                            column_type: ColumnType::scalar(LogicalType::Int64),
                            nullable: true,
                            provenance: Provenance::Inferred,
                        })
                        .collect(),
                },
                nullable: true,
                provenance: Provenance::Inferred,
            }],
        };
        let before = arena.obj_probes();
        let (batch, misfits) = build_batch(
            &schema,
            &std::collections::HashMap::new(),
            &rows,
            &LoadId::new("l"),
        )
        .expect("builds");
        let probes = arena.obj_probes() - before;
        assert_eq!((batch.num_rows(), misfits), (ROWS, 0));
        let nested = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .expect("struct");
        let first = nested
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("int64");
        assert_eq!(first.value(0), 0, "the index answers what the scan did");
        // The one top-level lookup per row (`s`) is all the arena sees;
        // the 2,000 nested fields per row never reach its scan.
        assert!(
            probes <= (ROWS * WIDTH) as u64,
            "nested lookups stay off the scan: {probes} probes for {} lookups",
            ROWS * FIELDS
        );
    }
}
