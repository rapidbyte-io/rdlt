//! Batches become parquet parts; short bookkeeping strings become SQL
//! literals. Nothing else in this crate turns a value into text.
//!
//! Row data never rides statement text on this path — it travels as a
//! parquet file the service reads back — so the literal escape survives
//! only for the state document and the receipt, which carry short,
//! known strings. It stays load-bearing all the same: this is the one
//! value-to-text seam, and a bad escape would not corrupt a row, it
//! would rewrite the statement.

use arrow_array::RecordBatch;
use rdlt_connector_sdk::spi::DestinationError;
use rdlt_connector_sdk::spi::core::TableSchema;

/// Escape a string for the inside of single quotes.
///
/// Quotes double (the service's escape), and so do backslashes — the
/// service treats backslash as an escape by default, and an unhandled
/// trailing one would swallow the closing quote and turn the rest of
/// the statement into string data.
pub(super) fn sql_literal_body(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "''")
}

/// One batch as a parquet part, projected to the ensured schema.
///
/// Projected, not written as-delivered: the load statement projects
/// columns by NAME out of the file, a batch may legitimately carry more
/// than the schema ensured, and an extra column would fail the COPY for
/// the whole file. Written in the SCHEMA's column order and case — the
/// COPY's projection names exactly these.
///
/// Snappy, the workspace's parquet default: a part lives for one
/// network hop and one read, where decompression speed beats ratio.
pub(super) fn parquet_part(
    schema: &TableSchema,
    batch: &RecordBatch,
) -> Result<Vec<u8>, DestinationError> {
    let indices = schema
        .columns
        .iter()
        .map(|column| {
            // A missing column is a CONTRACT violation, not a NULL: the
            // engine guarantees batches conform to the ensured schema.
            batch.schema().index_of(&column.name).map_err(|_| {
                DestinationError::fatal(format!(
                    "snowflake: batch for `{}` is missing column `{}`",
                    schema.table, column.name
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use rdlt_connector_sdk::spi::core::{
        ColumnDef, ColumnType, LogicalType, Provenance, TableName,
    };

    use super::*;

    fn utf8_schema(names: &[&str]) -> TableSchema {
        TableSchema {
            table: TableName::from("events"),
            parent: None,
            columns: names
                .iter()
                .map(|name| ColumnDef {
                    name: (*name).to_owned(),
                    column_type: ColumnType::scalar(LogicalType::Utf8),
                    nullable: true,
                    provenance: Provenance::Inferred,
                })
                .collect(),
        }
    }

    /// Both escapes, including the injection shape and the trailing
    /// backslash that would otherwise eat the closing quote.
    #[test]
    fn quotes_and_backslashes_are_both_escaped() {
        assert_eq!(sql_literal_body("o'brien"), "o''brien");
        assert_eq!(sql_literal_body("back\\slash"), "back\\\\slash");
        assert_eq!(sql_literal_body("trailing\\"), "trailing\\\\");
        assert_eq!(
            sql_literal_body("'; DROP TABLE t; --"),
            "''; DROP TABLE t; --"
        );
    }

    /// Name-resolved projection: a batch in a different column order
    /// still lands correctly, written in the SCHEMA's order.
    #[test]
    fn a_reordered_batch_is_projected_into_schema_order() {
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
        let part = parquet_part(&utf8_schema(&["id", "note"]), &batch).expect("part");
        assert_eq!(&part[..4], b"PAR1", "parquet magic first");
        let footer = String::from_utf8_lossy(&part);
        let (id_at, note_at) = (
            footer.find("id").expect("id present"),
            footer.find("note").expect("note present"),
        );
        assert!(id_at < note_at, "schema order, not batch order");
    }

    /// A batch missing an ensured column refuses, naming the column.
    #[test]
    fn a_batch_missing_an_ensured_column_is_a_named_error() {
        let arrow = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(arrow, vec![Arc::new(Int64Array::from(vec![1]))]).expect("batch");
        let err =
            parquet_part(&utf8_schema(&["id", "note"]), &batch).expect_err("missing is refused");
        assert!(format!("{err}").contains("note"), "{err}");
    }

    /// Non-finite floats travel — a 023-pinned CHANGE from the
    /// statement-text era, whose refusal was an artefact of the old
    /// transport, not a rule about data.
    #[test]
    fn non_finite_floats_encode_rather_than_refuse() {
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
        let part = parquet_part(&utf8_schema(&["amount"]), &batch).expect("encodes");
        assert_eq!(&part[..4], b"PAR1");
    }

    /// A part is a complete parquet file, magic at both ends.
    #[test]
    fn a_part_is_a_complete_parquet_file() {
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
        let part = parquet_part(&utf8_schema(&["id"]), &batch).expect("part");
        assert_eq!(&part[..4], b"PAR1");
        assert_eq!(&part[part.len() - 4..], b"PAR1");
    }
}
