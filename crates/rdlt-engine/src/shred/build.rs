//! Arrow columnar building: buffered JSON rows → `RecordBatch` per the resolved
//! schema. Values land directly in typed arrays; the `Json` column type stores the
//! verbatim serialized subtree (never dropped, never exploded).

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
use rdlt_core::{ColumnDef, ColumnType, LoadId, LogicalType, TableSchema};
use serde_json::Value;

use super::canon::{parse_timestamp_tz, render_scalar};
use super::nest::BufferedRow;

/// Arrow physical type for a logical type (design doc §5.1).
pub(crate) fn arrow_scalar_type(ty: LogicalType) -> DataType {
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
        .map(|c| Field::new(&c.name, arrow_column_type(&c.ty), c.nullable))
        .collect()
}

pub(crate) fn arrow_schema(schema: &TableSchema) -> Schema {
    Schema::new(arrow_fields(&schema.columns))
}

/// Build one table's batch. `name_map` maps source keys to normalized column names
/// (the schema speaks normalized; the buffered rows speak source).
pub(crate) fn build_batch(
    schema: &TableSchema,
    name_map: &[(String, String)],
    rows: &[BufferedRow],
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
                for row in rows {
                    b.append_value(row.id.to_hex());
                }
                Arc::new(b.finish())
            }
            system_columns::PARENT_ID => {
                let mut b = StringBuilder::new();
                for row in rows {
                    match &row.parent_id {
                        Some(id) => b.append_value(id.to_hex()),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            system_columns::ROOT_ID => {
                let mut b = StringBuilder::new();
                for row in rows {
                    match &row.root_id {
                        Some(id) => b.append_value(id.to_hex()),
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
                let source_key = name_map
                    .iter()
                    .find(|(_, normalized)| normalized == &column.name)
                    .map(|(source, _)| source.as_str())
                    .unwrap_or(column.name.as_str());
                let values: Vec<Option<&Value>> =
                    rows.iter().map(|row| row.value.get(source_key)).collect();
                build_column(&column.ty, &values)
            }
        };
        arrays.push(array);
    }
    RecordBatch::try_new(Arc::new(arrow_schema(schema)), arrays)
}

fn build_column(ty: &ColumnType, values: &[Option<&Value>]) -> ArrayRef {
    match ty {
        ColumnType::Scalar { scalar } => build_scalar(*scalar, values),
        ColumnType::Struct { fields } => {
            let validity: Vec<bool> = values
                .iter()
                .map(|v| matches!(v, Some(Value::Object(_))))
                .collect();
            let child_arrays: Vec<ArrayRef> = fields
                .iter()
                .map(|field| {
                    let projected: Vec<Option<&Value>> = values
                        .iter()
                        .map(|v| match v {
                            Some(Value::Object(map)) => map.get(&field.name),
                            _ => None,
                        })
                        .collect();
                    build_column(&field.ty, &projected)
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
            let mut flat: Vec<Option<&Value>> = Vec::new();
            let mut validity: Vec<bool> = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    Some(Value::Array(items)) => {
                        flat.extend(items.iter().map(Some));
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

fn build_scalar(ty: LogicalType, values: &[Option<&Value>]) -> ArrayRef {
    match ty {
        LogicalType::Bool => {
            let mut b = BooleanBuilder::new();
            for v in values {
                b.append_option(v.and_then(|v| v.as_bool()));
            }
            Arc::new(b.finish())
        }
        LogicalType::Int64 => {
            let mut b = Int64Builder::new();
            for v in values {
                b.append_option(v.and_then(|v| v.as_i64()));
            }
            Arc::new(b.finish())
        }
        LogicalType::Float64 => {
            let mut b = Float64Builder::new();
            for v in values {
                b.append_option(v.and_then(|v| v.as_f64()));
            }
            Arc::new(b.finish())
        }
        LogicalType::Utf8 | LogicalType::Uuid => {
            let mut b = StringBuilder::new();
            for v in values {
                match v.and_then(render_scalar) {
                    Some(text) => b.append_value(text),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        LogicalType::Json => {
            let mut b = StringBuilder::new();
            for v in values {
                match v {
                    Some(value) if !value.is_null() => {
                        b.append_value(serde_json::to_string(value).expect("Value serialization"))
                    }
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        LogicalType::TimestampTz => {
            let mut b = TimestampMicrosecondBuilder::new();
            for v in values {
                let micros = v
                    .and_then(|v| v.as_str())
                    .and_then(parse_timestamp_tz)
                    .and_then(|dt| dt.timestamp_micros().into());
                b.append_option(micros);
            }
            Arc::new(b.finish().with_timezone("+00:00"))
        }
        LogicalType::TimestampNaive => {
            let mut b = TimestampMicrosecondBuilder::new();
            for v in values {
                let micros = v.and_then(|v| v.as_str()).and_then(|s| {
                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
                        .ok()
                        .map(|dt| dt.and_utc().timestamp_micros())
                });
                b.append_option(micros);
            }
            Arc::new(b.finish())
        }
        LogicalType::Date => {
            let mut b = Date32Builder::new();
            for v in values {
                let days = v.and_then(|v| v.as_str()).and_then(|s| {
                    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                        .ok()
                        .map(|d| {
                            d.signed_duration_since(
                                chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch"),
                            )
                            .num_days() as i32
                        })
                });
                b.append_option(days);
            }
            Arc::new(b.finish())
        }
        LogicalType::Time => {
            let mut b = Time64MicrosecondBuilder::new();
            for v in values {
                let micros = v.and_then(|v| v.as_str()).and_then(|s| {
                    chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
                        .ok()
                        .map(|t| {
                            i64::from(chrono::Timelike::num_seconds_from_midnight(&t)) * 1_000_000
                                + i64::from(chrono::Timelike::nanosecond(&t) / 1_000)
                        })
                });
                b.append_option(micros);
            }
            Arc::new(b.finish())
        }
        LogicalType::Decimal { precision, scale } => {
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
        LogicalType::Binary => {
            // Not producible from JSON inference; hinted Binary without an Arrow-native
            // source yields nulls (never a silent lossy decode).
            let mut b = BinaryBuilder::new();
            for _ in values {
                b.append_null();
            }
            Arc::new(b.finish())
        }
    }
}

/// Exact decimal parsing from integers or decimal strings; `None` (→ null) for
/// anything inexact — floats are refused by design (no Float64 → Decimal edge).
fn parse_decimal(value: &Value, scale: u8) -> Option<i128> {
    let text = match value {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                return (i as i128).checked_mul(10i128.checked_pow(scale as u32)?);
            }
            return None; // floats: refused
        }
        Value::String(s) => s.trim(),
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
