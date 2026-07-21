//! Binary-COPY wire encoding: arrow column → Postgres wire type + cell values.
//! (Feature 008 T001: relocated verbatim; the native NUMERIC/JSONB/UUID
//! encoders land in T002.)

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use rdlt_connector::DestError;
use tokio_postgres::types::{ToSql, Type};

use super::fatal;

/// Postgres wire type for binary COPY, per arrow column type.
pub(super) fn copy_type(dt: &DataType) -> Result<Type, DestError> {
    Ok(match dt {
        DataType::Boolean => Type::BOOL,
        DataType::Int64 => Type::INT8,
        DataType::Float64 => Type::FLOAT8,
        DataType::Utf8 => Type::TEXT,
        DataType::Binary => Type::BYTEA,
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => Type::TIMESTAMPTZ,
        DataType::Timestamp(TimeUnit::Microsecond, None) => Type::TIMESTAMP,
        DataType::Date32 => Type::DATE,
        other => return Err(fatal(format!("unsupported arrow type for COPY: {other}"))),
    })
}

/// One cell as an owned ToSql value for binary COPY.
pub(super) fn cell_value(
    dt: &DataType,
    array: &dyn Array,
    row: usize,
) -> Result<Box<dyn ToSql + Sync + Send>, DestError> {
    macro_rules! cast {
        ($ty:ty) => {
            array
                .as_any()
                .downcast_ref::<$ty>()
                .ok_or_else(|| fatal("array type mismatch"))?
        };
    }
    if array.is_null(row) {
        // Binary COPY checks the ToSql type against the column's wire type — a NULL
        // must still be typed correctly.
        return Ok(match dt {
            DataType::Boolean => Box::new(Option::<bool>::None),
            DataType::Int64 => Box::new(Option::<i64>::None),
            DataType::Float64 => Box::new(Option::<f64>::None),
            DataType::Utf8 => Box::new(Option::<String>::None),
            DataType::Binary => Box::new(Option::<Vec<u8>>::None),
            DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
                Box::new(Option::<chrono::DateTime<chrono::Utc>>::None)
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                Box::new(Option::<chrono::NaiveDateTime>::None)
            }
            DataType::Date32 => Box::new(Option::<chrono::NaiveDate>::None),
            other => return Err(fatal(format!("unsupported arrow type for COPY: {other}"))),
        });
    }
    Ok(match dt {
        DataType::Boolean => Box::new(cast!(BooleanArray).value(row)),
        DataType::Int64 => Box::new(cast!(Int64Array).value(row)),
        DataType::Float64 => Box::new(cast!(Float64Array).value(row)),
        DataType::Utf8 => Box::new(cast!(StringArray).value(row).to_owned()),
        DataType::Binary => Box::new(cast!(BinaryArray).value(row).to_owned()),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            let micros = cast!(TimestampMicrosecondArray).value(row);
            Box::new(
                chrono::DateTime::<chrono::Utc>::from_timestamp_micros(micros)
                    .ok_or_else(|| fatal("timestamp out of range"))?,
            )
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let micros = cast!(TimestampMicrosecondArray).value(row);
            Box::new(
                chrono::DateTime::from_timestamp_micros(micros)
                    .ok_or_else(|| fatal("timestamp out of range"))?
                    .naive_utc(),
            )
        }
        DataType::Date32 => {
            let days = cast!(Date32Array).value(row);
            Box::new(
                chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163)
                    .ok_or_else(|| fatal("date out of range"))?,
            )
        }
        other => return Err(fatal(format!("unsupported arrow type for COPY: {other}"))),
    })
}
