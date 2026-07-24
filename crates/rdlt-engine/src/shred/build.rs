//! Arrow columnar building: drained rows → `RecordBatch` per the resolved
//! schema. Values land directly in typed arrays; the `Json` column type stores the
//! verbatim serialized subtree (never dropped, never exploded).
//!
//! Generic over [`JsonView`]: the tree and streaming paths
//! build through the SAME code — identical arrays, bit for bit.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder,
    Int64Builder, ListArray, StringBuilder, StructArray, Time64MicrosecondBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use rdlt_core::schema::system_columns;
use rdlt_core::{ColumnDef, ColumnType, LoadId, LogicalType, RowId, TableSchema};

use super::DrainRow;
use super::canon::parse_timestamp_tz;
use super::view::{JsonView, Kind};

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
pub(crate) fn build_batch<'v, V: JsonView<'v>>(
    schema: &TableSchema,
    source_to_normalized: &[(String, String)],
    rows: &[DrainRow<V>],
    load_id: &LoadId,
) -> Result<RecordBatch, ArrowError> {
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
                    rows.iter().map(|row| row.get_top(source_key)).collect();
                build_column(&column.column_type, &values)
            }
        };
        arrays.push(array);
    }
    RecordBatch::try_new(Arc::new(arrow_schema(schema)), arrays)
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

/// The observed [`Kind`] of an optional view cell — `None` for an absent cell.
fn view_kind<'v, V: JsonView<'v>>(v: &Option<V>) -> Option<Kind<'v>> {
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
            Some(Kind::Bool(x)) => Some(x),
            _ => None,
        });
    }
    Arc::new(b.finish())
}

fn scalar_int64<'v, V: JsonView<'v>>(values: &[Option<V>]) -> ArrayRef {
    let mut b = Int64Builder::new();
    for v in values {
        b.append_option(match view_kind(v) {
            Some(Kind::Int(i)) => Some(i),
            _ => None,
        });
    }
    Arc::new(b.finish())
}

fn scalar_float64<'v, V: JsonView<'v>>(values: &[Option<V>]) -> ArrayRef {
    let mut b = Float64Builder::new();
    for v in values {
        // Mirrors `Value::as_f64`: any JSON number converts with `as` casts.
        b.append_option(match view_kind(v) {
            Some(Kind::Float(f)) => Some(f),
            Some(Kind::Int(i)) => Some(i as f64),
            Some(Kind::UInt(u)) => Some(u as f64),
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
            Some(Kind::Str(s)) => b.append_value(s),
            Some(Kind::Bool(x)) => b.append_value(if x { "true" } else { "false" }),
            Some(Kind::Int(i)) => {
                scratch.clear();
                std::fmt::Write::write_fmt(&mut scratch, format_args!("{i}"))
                    .expect("write to String");
                b.append_value(&scratch);
            }
            Some(Kind::UInt(u)) => {
                scratch.clear();
                std::fmt::Write::write_fmt(&mut scratch, format_args!("{u}"))
                    .expect("write to String");
                b.append_value(&scratch);
            }
            Some(Kind::Float(f)) => {
                scratch.clear();
                std::fmt::Write::write_fmt(&mut scratch, format_args!("{f}"))
                    .expect("write to String");
                b.append_value(&scratch);
            }
            Some(Kind::Null | Kind::Object | Kind::Array) | None => b.append_null(),
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
            Some(Kind::Str(s)) => parse_timestamp_tz(s).map(|dt| dt.timestamp_micros()),
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
            Some(Kind::Str(s)) => chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
                .ok()
                .map(|dt| dt.and_utc().timestamp_micros()),
            _ => None,
        };
        b.append_option(micros);
    }
    Arc::new(b.finish())
}

fn scalar_date<'v, V: JsonView<'v>>(values: &[Option<V>]) -> ArrayRef {
    let mut b = Date32Builder::new();
    for v in values {
        let days = match view_kind(v) {
            Some(Kind::Str(s)) => chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
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
            Some(Kind::Str(s)) => chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
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
        b.append_option(v.and_then(|v| parse_decimal(v, scale)));
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
        Kind::Object => {
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
        Kind::Array => {
            out.push(b'[');
            for (i, item) in value.arr_items().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_compact_json(item, out);
            }
            out.push(b']');
        }
        Kind::Null => out.extend_from_slice(b"null"),
        Kind::Bool(b) => out.extend_from_slice(if b { b"true" } else { b"false" }),
        Kind::Str(s) => serde_json::to_writer(&mut *out, s).expect("string serialization"),
        Kind::Int(i) => serde_json::to_writer(&mut *out, &serde_json::Number::from(i))
            .expect("number serialization"),
        Kind::UInt(u) => serde_json::to_writer(&mut *out, &serde_json::Number::from(u))
            .expect("number serialization"),
        Kind::Float(f) => serde_json::to_writer(
            &mut *out,
            &serde_json::Number::from_f64(f).expect("finite by JSON grammar"),
        )
        .expect("number serialization"),
    }
}

/// Exact decimal parsing from integers or decimal strings; `None` (→ null) for
/// anything inexact — floats are refused by design (no Float64 → Decimal edge).
fn parse_decimal<'v, V: JsonView<'v>>(value: V, scale: u8) -> Option<i128> {
    let owned;
    let text = match value.kind() {
        Kind::Int(i) => {
            return (i as i128).checked_mul(10i128.checked_pow(scale as u32)?);
        }
        // u64 beyond i64 and floats: refused (matches Number::as_i64 semantics).
        Kind::UInt(_) | Kind::Float(_) => return None,
        Kind::Str(s) => {
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
    Some(sign * result)
}
