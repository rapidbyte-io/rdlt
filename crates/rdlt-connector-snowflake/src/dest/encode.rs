//! Turning an Arrow batch into a parquet part, and the one escape this crate
//! renders values through.
//!
//! Row data never becomes statement text: a batch leaves as a parquet file
//! that the service reads back. What remains here is [`sql_literal_body`],
//! used only by the bookkeeping statements — the state document and the
//! receipt — which carry short, known strings rather than user rows.
//!
//! Escaping is still load-bearing rather than cosmetic: it is the ONLY place a
//! value becomes text, and a value that escaped incorrectly would not merely
//! corrupt a row, it would change the statement.

use arrow_array::RecordBatch;
use rdlt_connector::DestinationError;
use rdlt_connector::core::TableSchema;

/// Escape a string for use inside single quotes.
///
/// The only value-to-text conversion in this crate. Doubling the quote is the
/// escape Snowflake accepts; the backslash also escapes by default, so it is
/// doubled too — leaving it alone lets a trailing backslash swallow the
/// closing quote and turn the rest of the statement into string data.
pub(super) fn sql_literal_body(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "''")
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
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

    fn reordered_batch() -> RecordBatch {
        let arrow = Arc::new(ArrowSchema::new(vec![
            Field::new("note", DataType::Utf8, true),
            Field::new("id", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            arrow,
            vec![
                Arc::new(StringArray::from(vec![Some("a")])),
                Arc::new(Int64Array::from(vec![7])),
            ],
        )
        .expect("batch")
    }

    #[test]
    fn quotes_and_backslashes_are_both_escaped() {
        // Not cosmetic: an unescaped quote ends the literal and the rest of
        // the value becomes SQL, and a trailing backslash swallows the closing
        // quote to the same effect. Row data no longer travels this way, but
        // the state document and the receipt still do.
        assert_eq!(sql_literal_body("o'brien"), "o''brien");
        assert_eq!(sql_literal_body("back\\slash"), "back\\\\slash");
        assert_eq!(sql_literal_body("trailing\\"), "trailing\\\\");
        assert_eq!(
            sql_literal_body("'; DROP TABLE t; --"),
            "''; DROP TABLE t; --"
        );
    }

    #[test]
    fn columns_are_resolved_by_name_so_a_reordered_batch_still_lands_correctly() {
        // The target's column order is historical; the batch carries this
        // run's. Order the part by position and values would shift sideways
        // into columns that happen to accept them.
        let part = parquet_part(&schema_of(&["id", "note"]), &reordered_batch()).expect("part");
        assert_eq!(&part[..4], b"PAR1", "a parquet file, magic first");

        // The written order is the SCHEMA's, not the batch's — which is what
        // lets the load statement name columns in the catalog's order.
        let text = String::from_utf8_lossy(&part);
        let (id_at, note_at) = (
            text.find("id").expect("id in the footer"),
            text.find("note").expect("note in the footer"),
        );
        assert!(id_at < note_at, "schema order, not batch order");
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
        let err = parquet_part(&schema_of(&["id", "note"]), &batch)
            .expect_err("the missing column is refused");
        assert!(format!("{err}").contains("note"), "{err}");
    }

    #[test]
    fn a_non_finite_float_travels_rather_than_being_refused() {
        // A CHANGE, pinned so it is not mistaken for an oversight. Rendering
        // values into statement text could not express NaN or an infinity as a
        // numeric literal, so they were refused at the encoder. A parquet file
        // carries them natively and the service's own float type accepts them,
        // so the refusal was an artefact of the transport rather than a rule
        // about the data — and it goes with the transport.
        let arrow = Arc::new(ArrowSchema::new(vec![Field::new(
            "amount",
            DataType::Float64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            arrow,
            vec![Arc::new(Float64Array::from(vec![
                f64::NAN,
                f64::INFINITY,
                f64::NEG_INFINITY,
            ]))],
        )
        .expect("batch");
        let part = parquet_part(&schema_of(&["amount"]), &batch).expect("non-finite floats encode");
        assert_eq!(&part[..4], b"PAR1");
    }

    #[test]
    fn a_part_carries_every_row_it_was_given() {
        let arrow = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            arrow,
            vec![Arc::new(Int64Array::from((0..1_000).collect::<Vec<_>>()))],
        )
        .expect("batch");
        let part = parquet_part(&schema_of(&["id"]), &batch).expect("part");
        assert_eq!(&part[..4], b"PAR1");
        assert_eq!(&part[part.len() - 4..], b"PAR1", "and magic last");
    }
}
