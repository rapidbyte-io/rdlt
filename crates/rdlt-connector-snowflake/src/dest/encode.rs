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

/// How much rendered VALUES text one statement carries, in bytes.
///
/// Measured against the qual account, 20,000 rows × 12 columns, timing the
/// same rows split into statements different ways:
///
/// ```text
///   100 rows/stmt → 200 statements,  20 KB each,  101 rows/s
///   500 rows/stmt →  40 statements, 101 KB each,  405 rows/s
///  2000 rows/stmt →  10 statements, 405 KB each,  659 rows/s
///  5000 rows/stmt →   4 statements, 1.0 MB each,  612 rows/s
/// 20000 rows/stmt →   1 statement,  4.0 MB,      1064 rows/s
/// ```
///
/// Throughput rises 6.5× from 100 to 2,000 rows per statement and then goes
/// flat and noisy — the 5,000 point measured SLOWER than 2,000. So the knee is
/// around 400 KB, and the gains past it are not worth the risk beside it.
///
/// The budget is in BYTES rather than rows because that is what the trade is
/// actually about. Rows were the obvious unit and the wrong one: this shape
/// reaches 400 KB at 2,000 rows, while a hundred-column table would reach it
/// at a few hundred — and a row-count constant tuned on one shape becomes a
/// multi-megabyte statement on the other. Snowflake documents a 1 MB limit for
/// a single statement (a 4 MB statement was nonetheless accepted through the
/// driver in the sweep above, so the effective cap is higher than the
/// documented one — which is a reason to stay under the documented one, not to
/// rely on the observed one).
const STATEMENT_VALUE_BUDGET: usize = 400 * 1024;

/// Rows per statement, when the budget cannot decide.
///
/// A ceiling, not a target: it bounds the per-statement row count for
/// pathologically narrow rows, where the byte budget alone would put millions
/// of them in one statement and make a single failure expensive to retry.
const MAX_ROWS_PER_STATEMENT: usize = 20_000;

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
    insert_statements_chunked(qualified_target, schema, batch, STATEMENT_VALUE_BUDGET)
}

/// [`insert_statements`] against a caller-chosen byte budget.
///
/// Exists so the budget can be MEASURED without becoming configuration:
/// shipping a knob in order to tune it would leave every user holding a
/// decision that belongs to whoever measured it.
///
/// Rows are rendered once and packed until the budget is reached, so a wide
/// row simply yields fewer rows per statement — no shape needs its own
/// constant. A single row over budget still gets its own statement rather
/// than being refused: splitting it is not possible, and a row too large for
/// the service is the service's error to report, with its own limit named.
pub(super) fn insert_statements_chunked(
    qualified_target: &str,
    schema: &TableSchema,
    batch: &RecordBatch,
    value_budget: usize,
) -> Result<Vec<String>, DestinationError> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }
    let column_list = schema
        .columns
        .iter()
        .map(|c| quote(&c.name))
        .collect::<Vec<_>>()
        .join(", ");

    // Resolved ONCE, not per row.
    let indices = column_indices(schema, batch)?;

    let mut out = Vec::new();
    let mut rows: Vec<String> = Vec::new();
    let mut bytes = 0usize;
    for row in 0..batch.num_rows() {
        let mut values = Vec::with_capacity(indices.len());
        for &index in &indices {
            values.push(render(batch.column(index).as_ref(), row)?);
        }
        let rendered = format!("({})", values.join(", "));

        // Flushed BEFORE adding, so a statement never exceeds the budget by a
        // whole row — and never before it holds anything, so an oversized row
        // still ships rather than producing an empty statement forever.
        let would_be = bytes + rendered.len() + 2;
        if !rows.is_empty() && (would_be > value_budget || rows.len() >= MAX_ROWS_PER_STATEMENT) {
            out.push(format!(
                "INSERT INTO {qualified_target} ({column_list}) VALUES {}",
                rows.join(", ")
            ));
            rows.clear();
            bytes = 0;
        }
        bytes += rendered.len() + 2;
        rows.push(rendered);
    }
    if !rows.is_empty() {
        out.push(format!(
            "INSERT INTO {qualified_target} ({column_list}) VALUES {}",
            rows.join(", ")
        ));
    }
    Ok(out)
}

/// One batch as a parquet part, projected to the ensured schema.
///
/// Projected rather than written as-delivered because the load matches columns
/// BY NAME: a column the target does not have would fail the `COPY` for the
/// whole file, and the batch is free to carry more than the schema ensured.
/// The column NAMES are written as the schema spells them — the load is
/// case-insensitive, so the catalog's upper-case form matches either way.
///
/// Snappy, matching the workspace's parquet default: the part exists for one
/// network hop and one read, so decompression speed is worth more than ratio.
pub(super) fn parquet_part(
    schema: &TableSchema,
    batch: &RecordBatch,
) -> Result<Vec<u8>, DestinationError> {
    let indices = column_indices(schema, batch)?;
    let projected = batch.project(&indices).map_err(|e| {
        DestinationError::fatal(format!(
            "snowflake: projecting batch for `{}`: {e}",
            schema.table
        ))
    })?;
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();
    let mut buf = Vec::new();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(&mut buf, projected.schema(), Some(properties))
            .map_err(DestinationError::fatal)?;
    writer.write(&projected).map_err(DestinationError::fatal)?;
    writer.close().map_err(DestinationError::fatal)?;
    Ok(buf)
}

/// Where each ensured column sits in the batch.
///
/// A column the batch does not carry is a contract violation, not a NULL: the
/// engine guarantees the batch conforms to the ensured schema.
fn column_indices(
    schema: &TableSchema,
    batch: &RecordBatch,
) -> Result<Vec<usize>, DestinationError> {
    schema
        .columns
        .iter()
        .map(|column| {
            batch.schema().index_of(&column.name).map_err(|_| {
                DestinationError::fatal(format!(
                    "snowflake: batch for `{}` is missing column `{}`",
                    schema.table, column.name
                ))
            })
        })
        .collect()
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
    fn a_batch_over_the_budget_becomes_several_statements() {
        let arrow = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let rows: Vec<i64> = (0..5_000).collect();
        let batch =
            RecordBatch::try_new(arrow, vec![Arc::new(Int64Array::from(rows))]).expect("batch");
        // A budget small enough that this narrow batch must split.
        let sql =
            insert_statements_chunked("\"T\"", &schema_of(&["id"]), &batch, 4_096).expect("sql");
        assert!(sql.len() > 1, "the budget must split the batch");
        for statement in &sql {
            assert!(
                statement.len() < 8_192,
                "no statement may run far past the budget: {} bytes",
                statement.len()
            );
        }
    }

    #[test]
    fn a_wide_row_yields_fewer_rows_per_statement_than_a_narrow_one() {
        // The property a row-count constant could not have: the SAME budget
        // packs fewer wide rows than narrow ones, so no shape needs its own
        // tuning. A row-count constant tuned on a narrow table becomes a
        // multi-megabyte statement on a wide one.
        let narrow = {
            let arrow = Arc::new(ArrowSchema::new(vec![Field::new(
                "id",
                DataType::Int64,
                false,
            )]));
            RecordBatch::try_new(
                arrow,
                vec![Arc::new(Int64Array::from((0..2_000).collect::<Vec<i64>>()))],
            )
            .expect("batch")
        };
        let wide = {
            let arrow = Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("note", DataType::Utf8, true),
            ]));
            RecordBatch::try_new(
                arrow,
                vec![
                    Arc::new(Int64Array::from((0..2_000).collect::<Vec<i64>>())),
                    Arc::new(StringArray::from(
                        (0..2_000).map(|_| "x".repeat(200)).collect::<Vec<_>>(),
                    )),
                ],
            )
            .expect("batch")
        };

        let narrow_sql =
            insert_statements_chunked("\"T\"", &schema_of(&["id"]), &narrow, 32_768).expect("sql");
        let wide_sql =
            insert_statements_chunked("\"T\"", &schema_of(&["id", "note"]), &wide, 32_768)
                .expect("sql");
        assert!(
            wide_sql.len() > narrow_sql.len(),
            "the same budget must pack fewer wide rows: {} wide statements vs {} narrow",
            wide_sql.len(),
            narrow_sql.len()
        );
    }

    #[test]
    fn a_single_row_over_budget_still_ships() {
        // Splitting one row is impossible, and refusing it here would turn a
        // service limit into a client-side dead end with no way forward. The
        // service reports its own limit, naming it.
        let arrow = Arc::new(ArrowSchema::new(vec![Field::new(
            "note",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            arrow,
            vec![Arc::new(StringArray::from(vec!["y".repeat(10_000)]))],
        )
        .expect("batch");
        let sql =
            insert_statements_chunked("\"T\"", &schema_of(&["note"]), &batch, 128).expect("sql");
        assert_eq!(sql.len(), 1, "the oversized row is emitted, not dropped");
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
