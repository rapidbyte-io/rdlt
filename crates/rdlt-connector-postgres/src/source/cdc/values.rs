//! Text-form tuple values → Arrow: pgoutput
//! proto_version 1 delivers every column in postgres's TEXT output form;
//! this module parses those forms into the SAME Arrow shapes the binary
//! COPY decoder produces (one conversion vocabulary, one target schema per
//! table). Errors are typed and name the column; nothing silently nulls.

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder, Int64Builder,
    StringBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};

use crate::source::copy_decode::FieldPlan;
use crate::source::types::Decode;

/// One cell of one change row: SQL NULL or the text form. (Unchanged-TOAST
/// markers are resolved BEFORE rows reach this module — substitution or a
/// typed error.)
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Cell {
    Null,
    Text(String),
}

/// Build one Arrow batch for `plans` + the trailing deletion-flag column.
/// `rows` are change rows (each exactly `plans.len()` cells); `deleted`
/// marks delete rows (flag TRUE; the flag is NULL otherwise). Every field
/// is nullable: delete rows carry NULL in non-key columns by design.
pub(crate) fn rows_to_batch(
    plans: &[FieldPlan],
    flag_column: &str,
    rows: &[Vec<Cell>],
    deleted: &[bool],
) -> Result<RecordBatch, String> {
    debug_assert_eq!(rows.len(), deleted.len());
    let mut fields: Vec<Field> = Vec::with_capacity(plans.len() + 1);
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(plans.len() + 1);
    for (idx, plan) in plans.iter().enumerate() {
        let column = rows.iter().map(|row| &row[idx]);
        let array = build_column(plan, column.clone(), rows.len())
            .map_err(|detail| format!("column `{}`: {detail}", plan.name))?;
        fields.push(Field::new(&plan.name, array.data_type().clone(), true));
        arrays.push(array);
    }
    let mut flag = BooleanBuilder::with_capacity(rows.len());
    for &is_delete in deleted {
        if is_delete {
            flag.append_value(true);
        } else {
            flag.append_null();
        }
    }
    fields.push(Field::new(flag_column, DataType::Boolean, true));
    arrays.push(std::sync::Arc::new(flag.finish()));
    RecordBatch::try_new(std::sync::Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| format!("assembling change batch: {e}"))
}

fn build_column<'a>(
    plan: &FieldPlan,
    cells: impl Iterator<Item = &'a Cell>,
    rows: usize,
) -> Result<ArrayRef, String> {
    use std::sync::Arc;
    let text = |cell: &'a Cell| match cell {
        Cell::Null => None,
        Cell::Text(t) => Some(t.as_str()),
    };
    macro_rules! build {
        ($builder:expr, $parse:expr) => {{
            let mut b = $builder;
            for cell in cells {
                match text(cell) {
                    None => b.append_null(),
                    Some(t) => b.append_value($parse(t)?),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }};
    }
    Ok(match plan.decode {
        Decode::Bool => build!(BooleanBuilder::with_capacity(rows), |t| match t {
            "t" => Ok(true),
            "f" => Ok(false),
            other => Err(format!("`{other}` is not a bool")),
        }),
        Decode::Int2 | Decode::Int4 | Decode::Int8 => {
            build!(Int64Builder::with_capacity(rows), |t: &str| t
                .parse::<i64>()
                .map_err(|e| format!("`{t}` is not an integer: {e}")))
        }
        Decode::Float4 | Decode::Float8 => {
            build!(Float64Builder::with_capacity(rows), parse_float)
        }
        Decode::Decimal { precision, scale } => {
            let mut b = Decimal128Builder::with_capacity(rows)
                .with_precision_and_scale(precision, scale as i8)
                .map_err(|e| e.to_string())?;
            for cell in cells {
                match text(cell) {
                    None => b.append_null(),
                    Some(t) => b.append_value(parse_decimal_scaled(t, scale)?),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }
        // Text-family targets: jsonb and uuid ALREADY arrive in their
        // canonical text forms from the server.
        Decode::Utf8 | Decode::JsonbText | Decode::UuidText => {
            let mut b = StringBuilder::new();
            for cell in cells {
                match text(cell) {
                    None => b.append_null(),
                    Some(t) => b.append_value(t),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }
        Decode::Bytea => {
            let mut b = BinaryBuilder::new();
            for cell in cells {
                match text(cell) {
                    None => b.append_null(),
                    Some(t) => b.append_value(parse_bytea_hex(t)?),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }
        Decode::Timestamp { tz } => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(rows);
            for cell in cells {
                match text(cell) {
                    None => b.append_null(),
                    Some(t) => b.append_value(parse_timestamp_us(t, tz)?),
                }
            }
            let array = b.finish();
            let array = if tz {
                array.with_timezone("UTC")
            } else {
                array
            };
            debug_assert!(matches!(
                plan.arrow,
                DataType::Timestamp(TimeUnit::Microsecond, _)
            ));
            Arc::new(array) as ArrayRef
        }
        Decode::Date => build!(Date32Builder::with_capacity(rows), parse_date_days),
        Decode::Time => build!(Time64MicrosecondBuilder::with_capacity(rows), parse_time_us),
    })
}

fn parse_float(t: &str) -> Result<f64, String> {
    // Rust accepts "NaN"/"Infinity"/"-Infinity" case-insensitively — the
    // exact postgres text spellings.
    t.parse::<f64>()
        .map_err(|e| format!("`{t}` is not a float: {e}"))
}

/// numeric text → i128 at the declared scale (exact; excess fractional
/// digits are a typed error, and `NaN` has no arrow representation).
fn parse_decimal_scaled(t: &str, scale: u8) -> Result<i128, String> {
    let (sign, body) = match t.strip_prefix('-') {
        Some(rest) => (-1i128, rest),
        None => (1, t),
    };
    let (int_part, frac_part) = body.split_once('.').unwrap_or((body, ""));
    if int_part.is_empty() && frac_part.is_empty()
        || !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return Err(format!("`{t}` is not a plain numeric literal"));
    }
    if frac_part.len() > scale as usize {
        return Err(format!(
            "`{t}` carries more fractional digits than numeric scale {scale}"
        ));
    }
    let mut digits = String::with_capacity(int_part.len() + scale as usize);
    digits.push_str(int_part);
    digits.push_str(frac_part);
    for _ in frac_part.len()..scale as usize {
        digits.push('0');
    }
    digits
        .parse::<i128>()
        .map(|v| sign * v)
        .map_err(|e| format!("`{t}` overflows the numeric wire range: {e}"))
}

fn parse_bytea_hex(t: &str) -> Result<Vec<u8>, String> {
    let hex = t
        .strip_prefix("\\x")
        .ok_or_else(|| format!("bytea text `{t}` lacks the \\x prefix"))?;
    if hex.len() % 2 != 0 {
        return Err("bytea hex has odd length".into());
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&hex[i..i + 2], 16)
                .map_err(|_| format!("bytea hex contains a non-hex pair at offset {i}"))
        })
        .collect()
}

/// Postgres timestamp text (`2026-07-21 10:11:12.123456` with an optional
/// `+HH[:MM]` offset when `tz`) → µs since the Unix epoch. `±infinity`
/// saturates, matching the binary COPY decoder.
fn parse_timestamp_us(t: &str, tz: bool) -> Result<i64, String> {
    match t {
        "infinity" => return Ok(i64::MAX),
        "-infinity" => return Ok(i64::MIN),
        _ => {}
    }
    if tz {
        // `%#z` accepts +00, +05:30, and +0530 spellings.
        chrono::DateTime::parse_from_str(t, "%Y-%m-%d %H:%M:%S%.f%#z")
            .map(|dt| dt.timestamp_micros())
            .map_err(|e| format!("`{t}` is not a timestamptz: {e}"))
    } else {
        chrono::NaiveDateTime::parse_from_str(t, "%Y-%m-%d %H:%M:%S%.f")
            .map(|dt| dt.and_utc().timestamp_micros())
            .map_err(|e| format!("`{t}` is not a timestamp: {e}"))
    }
}

fn parse_date_days(t: &str) -> Result<i32, String> {
    match t {
        "infinity" => return Ok(i32::MAX),
        "-infinity" => return Ok(i32::MIN),
        _ => {}
    }
    let date = chrono::NaiveDate::parse_from_str(t, "%Y-%m-%d")
        .map_err(|e| format!("`{t}` is not a date: {e}"))?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    i32::try_from((date - epoch).num_days()).map_err(|_| format!("date `{t}` out of range"))
}

fn parse_time_us(t: &str) -> Result<i64, String> {
    use chrono::Timelike;
    let time = chrono::NaiveTime::parse_from_str(t, "%H:%M:%S%.f")
        .map_err(|e| format!("`{t}` is not a time: {e}"))?;
    Ok(time.num_seconds_from_midnight() as i64 * 1_000_000 + (time.nanosecond() / 1_000) as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;

    fn plan(name: &str, decode: Decode, arrow: DataType) -> FieldPlan {
        FieldPlan {
            name: name.into(),
            decode,
            arrow,
            not_null: false,
        }
    }

    #[test]
    fn text_forms_parse_into_the_copy_decoder_shapes() {
        let plans = vec![
            plan("i", Decode::Int8, DataType::Int64),
            plan("f", Decode::Float8, DataType::Float64),
            plan(
                "n",
                Decode::Decimal {
                    precision: 10,
                    scale: 2,
                },
                DataType::Decimal128(10, 2),
            ),
            plan("s", Decode::Utf8, DataType::Utf8),
            plan("b", Decode::Bool, DataType::Boolean),
            plan("y", Decode::Bytea, DataType::Binary),
            plan(
                "ts",
                Decode::Timestamp { tz: true },
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
            plan("d", Decode::Date, DataType::Date32),
            plan("t", Decode::Time, DataType::Time64(TimeUnit::Microsecond)),
        ];
        let row = vec![
            Cell::Text("42".into()),
            Cell::Text("-Infinity".into()),
            Cell::Text("-12345.67".into()),
            Cell::Text("héllo".into()),
            Cell::Text("t".into()),
            Cell::Text("\\x0aff".into()),
            Cell::Text("2026-07-21 10:11:12.123456+00".into()),
            Cell::Text("2026-07-21".into()),
            Cell::Text("10:11:12.123456".into()),
        ];
        let nulls: Vec<Cell> = std::iter::repeat_n(Cell::Null, plans.len()).collect();
        let batch =
            rows_to_batch(&plans, "_rdlt_deleted", &[row, nulls], &[false, true]).expect("batch");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), plans.len() + 1);
        let ints = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(ints.value(0), 42);
        assert!(ints.is_null(1));
        let floats = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap();
        assert_eq!(floats.value(0), f64::NEG_INFINITY);
        let nums = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap();
        assert_eq!(nums.value(0), -1_234_567);
        let bins = batch
            .column(5)
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .unwrap();
        assert_eq!(bins.value(0), &[0x0a, 0xff]);
        let flags = batch
            .column(plans.len())
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .unwrap();
        assert!(flags.is_null(0), "insert/update rows carry a NULL flag");
        assert!(flags.value(1), "delete rows carry TRUE");
    }

    #[test]
    fn malformed_text_is_a_typed_error_naming_the_column() {
        let plans = vec![plan("qty", Decode::Int8, DataType::Int64)];
        let err = rows_to_batch(
            &plans,
            "_rdlt_deleted",
            &[vec![Cell::Text("not-a-number".into())]],
            &[false],
        )
        .expect_err("typed");
        assert!(err.contains("`qty`"), "{err}");
        // Excess numeric precision and NaN are refused, not rounded.
        let plans = vec![plan(
            "n",
            Decode::Decimal {
                precision: 10,
                scale: 2,
            },
            DataType::Decimal128(10, 2),
        )];
        for bad in ["1.234", "NaN"] {
            let err = rows_to_batch(
                &plans,
                "_rdlt_deleted",
                &[vec![Cell::Text(bad.into())]],
                &[false],
            )
            .expect_err(bad);
            assert!(err.contains("`n`"), "{err}");
        }
    }

    #[test]
    fn timestamp_offset_spellings_and_infinities() {
        for (text, expect) in [
            ("2026-01-02 03:04:05+00", Some(1_767_323_045_000_000i64)),
            ("2026-01-02 08:34:05+05:30", Some(1_767_323_045_000_000)),
            ("infinity", Some(i64::MAX)),
            ("-infinity", Some(i64::MIN)),
            ("2026-99-02 03:04:05+00", None),
        ] {
            assert_eq!(parse_timestamp_us(text, true).ok(), expect, "{text}");
        }
    }
}
