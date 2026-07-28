//! Turning an Arrow batch into statements Snowflake can run.
//!
//! Values are rendered into the statement text rather than bound, because a
//! multi-row `INSERT … VALUES` is what makes a batch one round trip instead of
//! thousands — and the binding API is positional, so a thousand-row batch
//! would need a thousand placeholders and the same number of binds.
//!
//! That makes correct escaping load-bearing rather than cosmetic: every value
//! goes through [`sql_literal_body`], which is the ONLY place a value becomes
//! text. A value that escaped incorrectly would not merely corrupt a row, it
//! would change the statement.

use arrow_array::{Array, RecordBatch};
use rdlt_connector::DestinationError;
use rdlt_connector::core::TableSchema;

use super::ddl::quote;

/// Rows per statement.
///
/// A placeholder pending measurement (US5 sweeps it against the qual account):
/// it is a real trade — larger statements amortise the round trip, and
/// Snowflake caps statement text, so the knee is a property of the data's
/// width, not a constant to guess at. Recorded here rather than tuned blind.
const ROWS_PER_STATEMENT: usize = 1_000;

/// Escape a string for use inside single quotes.
///
/// The only value-to-text conversion in this crate. Doubling the quote is the
/// escape Snowflake accepts; the backslash also escapes by default, so it is
/// doubled too — leaving it alone lets a trailing backslash swallow the
/// closing quote and turn the rest of the statement into string data.
pub(super) fn sql_literal_body(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "''")
}

/// The `INSERT` statements for one batch, in order.
///
/// By NAME, never positionally: the target's column order is historical while
/// the batch carries this run's order, so a positional insert would silently
/// shift values between columns after any drift.
pub(super) fn insert_statements(
    qualified_target: &str,
    schema: &TableSchema,
    batch: &RecordBatch,
) -> Result<Vec<String>, DestinationError> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }
    let columns: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    let column_list = columns
        .iter()
        .map(|name| quote(name))
        .collect::<Vec<_>>()
        .join(", ");

    // Resolve each schema column to its position in the batch ONCE. A column
    // the batch does not carry is a contract violation, not a NULL: the engine
    // guarantees the batch conforms to the ensured schema.
    let indices: Vec<usize> = columns
        .iter()
        .map(|name| {
            batch.schema().index_of(name).map_err(|_| {
                DestinationError::fatal(format!(
                    "snowflake: batch for `{}` is missing column `{name}`",
                    schema.table
                ))
            })
        })
        .collect::<Result<_, _>>()?;

    let mut out = Vec::new();
    for chunk_start in (0..batch.num_rows()).step_by(ROWS_PER_STATEMENT) {
        let chunk_end = (chunk_start + ROWS_PER_STATEMENT).min(batch.num_rows());
        let mut rows = Vec::with_capacity(chunk_end - chunk_start);
        for row in chunk_start..chunk_end {
            let mut values = Vec::with_capacity(indices.len());
            for &index in &indices {
                values.push(render(batch.column(index).as_ref(), row)?);
            }
            rows.push(format!("({})", values.join(", ")));
        }
        out.push(format!(
            "INSERT INTO {qualified_target} ({column_list}) VALUES {}",
            rows.join(", ")
        ));
    }
    Ok(out)
}

/// Render one cell as a SQL literal.
///
/// Arrow's own display is deliberately NOT used: it formats for humans, and a
/// human-readable rendering of a timestamp or a decimal is not necessarily one
/// the service parses back to the same value.
fn render(array: &dyn Array, row: usize) -> Result<String, DestinationError> {
    use arrow_array::{
        BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array, LargeStringArray,
        StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    };
    use arrow_schema::{DataType, TimeUnit};

    if array.is_null(row) {
        return Ok("NULL".to_owned());
    }
    let downcast = |ok: bool| {
        ok.then_some(())
            .ok_or_else(|| DestinationError::fatal("snowflake: batch column type mismatch"))
    };
    let text = match array.data_type() {
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>();
            downcast(a.is_some())?;
            if a.expect("checked").value(row) {
                "TRUE".to_owned()
            } else {
                "FALSE".to_owned()
            }
        }
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>();
            downcast(a.is_some())?;
            a.expect("checked").value(row).to_string()
        }
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>();
            downcast(a.is_some())?;
            let value = a.expect("checked").value(row);
            // Non-finite floats have no literal here; the engine's own type
            // rules keep them out of a Float64 column, so reaching this is a
            // contract violation worth naming rather than rendering as text
            // the service would reject with a confusing message.
            if !value.is_finite() {
                return Err(DestinationError::fatal(format!(
                    "snowflake: {value} has no SQL literal; a non-finite float cannot be stored"
                )));
            }
            format!("{value:?}")
        }
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>();
            downcast(a.is_some())?;
            quoted(a.expect("checked").value(row))
        }
        DataType::LargeUtf8 => {
            let a = array.as_any().downcast_ref::<LargeStringArray>();
            downcast(a.is_some())?;
            quoted(a.expect("checked").value(row))
        }
        DataType::Date32 => {
            let a = array.as_any().downcast_ref::<Date32Array>();
            downcast(a.is_some())?;
            // Days since the epoch, converted by the service rather than by
            // us formatting a date string it must parse back.
            format!(
                "DATEADD(day, {}, '1970-01-01'::DATE)",
                a.expect("checked").value(row)
            )
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let a = array.as_any().downcast_ref::<Time64MicrosecondArray>();
            downcast(a.is_some())?;
            let micros = a.expect("checked").value(row);
            format!("TIME_FROM_PARTS(0, 0, 0, {})", micros * 1_000)
        }
        DataType::Timestamp(TimeUnit::Microsecond, zone) => {
            let a = array.as_any().downcast_ref::<TimestampMicrosecondArray>();
            downcast(a.is_some())?;
            let micros = a.expect("checked").value(row);
            // Built from the epoch by the service: a rendered timestamp string
            // would depend on the session's format parameters, which a user is
            // free to change underneath us.
            let seconds = micros.div_euclid(1_000_000);
            let nanos = micros.rem_euclid(1_000_000) * 1_000;
            if zone.is_some() {
                format!(
                    "TO_TIMESTAMP_TZ(TO_TIMESTAMP_LTZ({seconds}, 0) + INTERVAL '{nanos} NANOSECONDS')"
                )
            } else {
                format!("TO_TIMESTAMP_NTZ({seconds}, 0) + INTERVAL '{nanos} NANOSECONDS'")
            }
        }
        DataType::Decimal128(_, scale) => {
            let a = array.as_any().downcast_ref::<Decimal128Array>();
            downcast(a.is_some())?;
            // The array's OWN scale, never the schema's: the i128 payload is
            // stored at the array's scale, and reading the other would move
            // the decimal point by whatever the two disagree by.
            decimal_literal(a.expect("checked").value(row), *scale)
        }
        other => {
            return Err(DestinationError::fatal(format!(
                "snowflake: no SQL literal for arrow type {other}"
            )));
        }
    };
    Ok(text)
}

/// A quoted string literal.
fn quoted(value: &str) -> String {
    format!("'{}'", sql_literal_body(value))
}

/// Render a scaled integer as a decimal literal.
///
/// Text rather than arithmetic: dividing would go through a float and lose
/// exactly the precision the decimal type exists to keep.
fn decimal_literal(value: i128, scale: i8) -> String {
    if scale <= 0 {
        // A negative scale multiplies; the engine refuses negative scales
        // upstream, so this only ever runs for scale 0.
        return value.to_string();
    }
    let scale = scale as usize;
    let negative = value < 0;
    let digits = value.unsigned_abs().to_string();
    let padded = if digits.len() <= scale {
        format!("{}{}", "0".repeat(scale - digits.len() + 1), digits)
    } else {
        digits
    };
    let split = padded.len() - scale;
    let sign = if negative { "-" } else { "" };
    format!("{sign}{}.{}", &padded[..split], &padded[split..])
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array, RecordBatch,
        StringArray, TimestampMicrosecondArray,
    };
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use rdlt_connector::core::{ColumnDef, ColumnType, LogicalType, Provenance, TableName};

    use super::*;

    fn schema_of(names: &[&str]) -> TableSchema {
        TableSchema {
            table: TableName::from("events"),
            parent: None,
            columns: names
                .iter()
                .map(|n| ColumnDef {
                    name: (*n).to_owned(),
                    column_type: ColumnType::scalar(LogicalType::Utf8),
                    nullable: true,
                    provenance: Provenance::Inferred,
                })
                .collect(),
        }
    }

    #[test]
    fn quotes_and_backslashes_are_both_escaped() {
        // Not cosmetic: an unescaped quote ends the literal and the rest of
        // the value becomes SQL, and a trailing backslash swallows the closing
        // quote to the same effect.
        assert_eq!(sql_literal_body("o'brien"), "o''brien");
        assert_eq!(sql_literal_body("back\\slash"), "back\\\\slash");
        assert_eq!(sql_literal_body("trailing\\"), "trailing\\\\");
        assert_eq!(
            sql_literal_body("'; DROP TABLE t; --"),
            "''; DROP TABLE t; --"
        );
    }

    #[test]
    fn a_null_renders_as_null_not_as_empty_text() {
        let array = StringArray::from(vec![None::<&str>]);
        assert_eq!(render(&array, 0).expect("render"), "NULL");
    }

    #[test]
    fn decimals_keep_every_digit_they_were_given() {
        // Through text, because dividing would route the value through a
        // float and lose exactly what a decimal exists to keep.
        assert_eq!(decimal_literal(123_456, 2), "1234.56");
        assert_eq!(decimal_literal(-123_456, 2), "-1234.56");
        assert_eq!(decimal_literal(5, 4), "0.0005");
        assert_eq!(decimal_literal(-5, 4), "-0.0005");
        assert_eq!(decimal_literal(0, 2), "0.00");
        assert_eq!(decimal_literal(42, 0), "42");
        // The widest value the type holds must not overflow into a float.
        assert_eq!(
            decimal_literal(i128::MAX, 0),
            i128::MAX.to_string(),
            "no precision is lost at the extreme"
        );
    }

    #[test]
    fn booleans_and_integers_render_as_literals() {
        assert_eq!(render(&BooleanArray::from(vec![true]), 0).unwrap(), "TRUE");
        assert_eq!(
            render(&BooleanArray::from(vec![false]), 0).unwrap(),
            "FALSE"
        );
        assert_eq!(
            render(&Int64Array::from(vec![i64::MIN]), 0).unwrap(),
            i64::MIN.to_string()
        );
    }

    #[test]
    fn a_non_finite_float_is_refused_rather_than_rendered() {
        let err =
            render(&Float64Array::from(vec![f64::NAN]), 0).expect_err("NaN has no SQL literal");
        assert!(format!("{err}").contains("non-finite"), "{err}");
    }

    #[test]
    fn dates_and_timestamps_are_built_by_the_service_not_formatted_by_us() {
        // A rendered date/timestamp string would depend on session format
        // parameters a user can change underneath the pipeline.
        let date = render(&Date32Array::from(vec![19_000]), 0).unwrap();
        assert!(date.contains("DATEADD(day, 19000"), "{date}");
        let naive = render(
            &TimestampMicrosecondArray::from(vec![1_700_000_000_000_000]),
            0,
        )
        .unwrap();
        assert!(naive.contains("TO_TIMESTAMP_NTZ"), "{naive}");
        let zoned = render(
            &TimestampMicrosecondArray::from(vec![1_700_000_000_000_000]).with_timezone("UTC"),
            0,
        )
        .unwrap();
        assert!(zoned.contains("TO_TIMESTAMP_TZ"), "{zoned}");
    }

    #[test]
    fn a_negative_timestamp_floors_rather_than_truncating_toward_zero() {
        // Pre-epoch instants: truncating toward zero would move them a whole
        // second and only for values before 1970, which is the kind of bug
        // that survives a demo.
        let before = render(&TimestampMicrosecondArray::from(vec![-1_500_000]), 0).unwrap();
        assert!(before.contains("-2"), "seconds floor to -2: {before}");
        assert!(before.contains("500000000 NANOSECONDS"), "{before}");
    }

    #[test]
    fn a_batch_becomes_one_statement_per_chunk_inserting_by_name() {
        let arrow = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("note", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            arrow,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
            ],
        )
        .expect("batch");
        let sql = insert_statements(
            "\"DB\".\"S\".\"EVENTS\"",
            &schema_of(&["id", "note"]),
            &batch,
        )
        .expect("statements");
        assert_eq!(sql.len(), 1, "one chunk: {sql:?}");
        assert_eq!(
            sql[0],
            "INSERT INTO \"DB\".\"S\".\"EVENTS\" (\"ID\", \"NOTE\") VALUES (1, 'a'), (2, NULL)"
        );
    }

    #[test]
    fn an_empty_batch_costs_no_statement() {
        let arrow = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::new_empty(arrow);
        assert!(
            insert_statements("\"T\"", &schema_of(&["id"]), &batch)
                .expect("statements")
                .is_empty()
        );
    }

    #[test]
    fn columns_are_resolved_by_name_so_a_reordered_batch_still_lands_correctly() {
        // The target's column order is historical; the batch carries this
        // run's. A positional insert would silently shift values sideways.
        let arrow = Arc::new(ArrowSchema::new(vec![
            Field::new("note", DataType::Utf8, true),
            Field::new("id", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            arrow,
            vec![
                Arc::new(StringArray::from(vec![Some("a")])),
                Arc::new(Int64Array::from(vec![7])),
            ],
        )
        .expect("batch");
        let sql = insert_statements("\"T\"", &schema_of(&["id", "note"]), &batch).expect("sql");
        assert!(sql[0].ends_with("VALUES (7, 'a')"), "{}", sql[0]);
    }

    #[test]
    fn a_batch_missing_an_ensured_column_is_a_named_error() {
        let arrow = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(arrow, vec![Arc::new(Int64Array::from(vec![1]))]).expect("batch");
        let err = insert_statements("\"T\"", &schema_of(&["id", "note"]), &batch)
            .expect_err("the missing column is refused");
        assert!(format!("{err}").contains("note"), "{err}");
    }

    #[test]
    fn a_batch_larger_than_the_chunk_becomes_several_statements() {
        let arrow = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let rows: Vec<i64> = (0..ROWS_PER_STATEMENT as i64 + 1).collect();
        let batch =
            RecordBatch::try_new(arrow, vec![Arc::new(Int64Array::from(rows))]).expect("batch");
        let sql = insert_statements("\"T\"", &schema_of(&["id"]), &batch).expect("sql");
        assert_eq!(sql.len(), 2, "the chunk boundary splits the batch");
    }

    #[test]
    fn decimal_uses_the_arrays_own_scale() {
        // Reading the schema's scale instead would move the decimal point by
        // however much the two disagree.
        let array = Decimal128Array::from(vec![123_456])
            .with_precision_and_scale(10, 3)
            .expect("decimal");
        assert_eq!(render(&array, 0).expect("render"), "123.456");
    }
}
