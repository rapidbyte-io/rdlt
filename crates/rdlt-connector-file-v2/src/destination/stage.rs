//! Part staging: batch → partition groups → encoded bytes.
//!
//! Three pure seams the session composes: the ONE translation from the
//! SPI's `ParquetOptions` to the parquet crate's `WriterProperties`
//! (library types stop here), the partition split, and the per-format
//! encoders. Nothing here touches storage.

use arrow::array::UInt32Array;
use arrow::compute::take_record_batch;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use parquet::arrow::ArrowWriter;
use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;
use rdlt_connector_sdk::spi::core::TableName;
use rdlt_connector_sdk::spi::{DestinationError, ParquetCompression, ParquetOptions, RecordBatch};

use super::DestFormat;
use crate::destination::layout::{NULL_PARTITION, path_safe};

/// Build the `WriterProperties` a session writes every part with.
///
/// This runs once, at session open — an option the library cannot
/// honour is refused BEFORE any data moves, naming what was wrong.
pub(crate) fn writer_props(options: &ParquetOptions) -> Result<WriterProperties, DestinationError> {
    let mut props = WriterProperties::builder()
        .set_compression(compression_of(options)?)
        .set_dictionary_enabled(options.dictionary_enabled)
        .set_dictionary_page_size_limit(options.dictionary_page_size_limit);
    if let Some(bytes) = options.data_page_size_limit {
        props = props.set_data_page_size_limit(bytes);
    }
    match options.max_row_group_rows {
        // An unset option must NOT be forwarded: this setter reads
        // `None` as "no bound at all", and forwarding it would quietly
        // trade the library's own row-group ceiling for unbounded
        // groups. (`Some(0)` would panic inside the setter — config
        // validation refused that value long before this point.)
        None => {}
        bound @ Some(_) => props = props.set_max_row_group_row_count(bound),
    }
    Ok(props.build())
}

/// Resolve the configured codec, applying the level on codecs that
/// carry one.
///
/// A level on a levelless codec was already refused at validation, so
/// the missing-value branch inside `unsigned_level` guards against a
/// future bug, not against any reachable user input.
fn compression_of(options: &ParquetOptions) -> Result<Compression, DestinationError> {
    let level = options.compression_level;
    Ok(match options.compression {
        ParquetCompression::Uncompressed => Compression::UNCOMPRESSED,
        ParquetCompression::Snappy => Compression::SNAPPY,
        ParquetCompression::Lz4Raw => Compression::LZ4_RAW,
        ParquetCompression::Gzip => match level {
            None => Compression::GZIP(GzipLevel::default()),
            Some(_) => Compression::GZIP(
                GzipLevel::try_new(unsigned_level(level, "gzip")?)
                    .map_err(|e| out_of_range(level, "gzip", e))?,
            ),
        },
        ParquetCompression::Brotli => match level {
            None => Compression::BROTLI(BrotliLevel::default()),
            Some(_) => Compression::BROTLI(
                BrotliLevel::try_new(unsigned_level(level, "brotli")?)
                    .map_err(|e| out_of_range(level, "brotli", e))?,
            ),
        },
        ParquetCompression::Zstd => match level {
            None => Compression::ZSTD(ZstdLevel::default()),
            // Zstd is the signed one — the format defines negative
            // "fast" levels — so the value goes to the library as-is
            // and its own bound does any refusing.
            Some(value) => Compression::ZSTD(
                ZstdLevel::try_new(value).map_err(|e| out_of_range(level, "zstd", e))?,
            ),
        },
        // The SPI's codec vocabulary is non_exhaustive. A variant this
        // build has never heard of gets refused BY NAME — falling back
        // to uncompressed silently is exactly the wrong default.
        other => {
            return Err(DestinationError::fatal(format!(
                "compression `{}` is not supported by the file destination",
                other.as_str()
            )));
        }
    })
}

/// Gzip and brotli take unsigned levels only: a negative value is a
/// user mistake to refuse outright, never something to clamp.
fn unsigned_level(level: Option<i32>, codec: &str) -> Result<u32, DestinationError> {
    let Some(value) = level else {
        return Err(DestinationError::fatal(format!(
            "internal: {codec} level requested without a value"
        )));
    };
    u32::try_from(value).map_err(|_| {
        DestinationError::fatal(format!(
            "`compression_level` {value} is negative — {codec} levels start at 0"
        ))
    })
}

/// A level the library's own bounds rejected, wrapped with the option
/// name so the user sees what to change.
fn out_of_range(
    level: Option<i32>,
    codec: &str,
    e: parquet::errors::ParquetError,
) -> DestinationError {
    DestinationError::fatal(format!(
        "`compression_level` {} is not valid for {codec}: {e}",
        level.unwrap_or_default()
    ))
}

/// Split one batch by the partition column, or hand it back whole.
///
/// The slug in each group is the PATH-SAFE rendered value (stored
/// sanitized; NULL → `__null__`) — one `ArrayFormatter` for the whole
/// column, hoisted out of the row loop. Group order is deterministic
/// (BTreeMap) so staged names cannot depend on hash order.
pub(crate) fn split_partitions(
    table: &TableName,
    batch: &RecordBatch,
    partition_by: Option<&str>,
) -> Result<Vec<(Option<String>, RecordBatch)>, DestinationError> {
    let Some(column) = partition_by else {
        return Ok(vec![(None, batch.clone())]);
    };
    let schema = batch.schema();
    let Some((index, _)) = schema.column_with_name(column) else {
        let columns: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        return Err(DestinationError::fatal(format!(
            "partition_by column `{column}` does not exist in stream `{table}` \
             (columns: {columns:?})"
        )));
    };
    let array = batch.column(index);
    let options = FormatOptions::default().with_display_error(true);
    let formatter = ArrayFormatter::try_new(array.as_ref(), &options)
        .map_err(|e| DestinationError::fatal(format!("partition column `{column}`: {e}")))?;
    let mut groups: std::collections::BTreeMap<String, Vec<u32>> = Default::default();
    for row in 0..batch.num_rows() {
        let slug = if array.is_null(row) {
            NULL_PARTITION.to_string()
        } else {
            let rendered = formatter.value(row).try_to_string().map_err(|e| {
                DestinationError::fatal(format!("partition value at row {row}: {e}"))
            })?;
            path_safe(&rendered)
        };
        groups.entry(slug).or_default().push(
            u32::try_from(row).map_err(|e| {
                DestinationError::fatal(format!("partition value at row {row}: {e}"))
            })?,
        );
    }
    groups
        .into_iter()
        .map(|(slug, rows)| {
            let taken = take_record_batch(batch, &UInt32Array::from(rows))
                .map_err(|e| DestinationError::fatal(format!("partition split: {e}")))?;
            Ok((Some(slug), taken))
        })
        .collect()
}

/// Encode one partition group as one part's bytes.
pub(crate) fn encode_part(
    format: DestFormat,
    batch: &RecordBatch,
    props: &WriterProperties,
) -> Result<Vec<u8>, DestinationError> {
    match format {
        DestFormat::Parquet => {
            let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), Some(props.clone()))
                .map_err(|e| DestinationError::fatal(format!("parquet encode: {e}")))?;
            writer
                .write(batch)
                .map_err(|e| DestinationError::fatal(format!("parquet encode: {e}")))?;
            writer
                .into_inner()
                .map_err(|e| DestinationError::fatal(format!("parquet encode: {e}")))
        }
        DestFormat::Jsonl => {
            let mut writer = arrow::json::LineDelimitedWriter::new(Vec::new());
            writer
                .write(batch)
                .map_err(|e| DestinationError::fatal(format!("jsonl encode: {e}")))?;
            writer
                .finish()
                .map_err(|e| DestinationError::fatal(format!("jsonl encode: {e}")))?;
            Ok(writer.into_inner())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn batch(values: &[Option<&str>]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, true),
        ]));
        let ids: Vec<i64> = (0..values.len() as i64).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(values.to_vec())),
            ],
        )
        .expect("batch builds")
    }

    /// The shipped defaults COMPRESS. The parquet library's own
    /// default is uncompressed output, and an earlier era of this
    /// connector shipped it — files roughly three times the size a
    /// competing tool wrote from the same rows. The pin keeps that
    /// from coming back.
    #[test]
    fn the_default_props_compress_with_snappy() {
        let props = writer_props(&ParquetOptions::default()).expect("defaults valid");
        assert_eq!(props.compression(&"any".into()), Compression::SNAPPY);
        assert!(props.dictionary_enabled(&"any".into()));
        let library = WriterProperties::builder().build();
        assert!(props.dictionary_page_size_limit() < library.dictionary_page_size_limit());
    }

    /// `None` must NOT reach the row-group setter: the setter reads it
    /// as unlimited, not as the default.
    #[test]
    fn an_unset_row_group_count_keeps_the_library_bound() {
        let props = writer_props(&ParquetOptions::default()).expect("valid");
        let library = WriterProperties::builder()
            .build()
            .max_row_group_row_count();
        assert!(library.is_some(), "`None` here would be a real change");
        assert_eq!(props.max_row_group_row_count(), library);
        let bounded = writer_props(&ParquetOptions {
            max_row_group_rows: Some(5_000),
            ..Default::default()
        })
        .expect("valid");
        assert_eq!(bounded.max_row_group_row_count(), Some(5_000));
    }

    /// Every codec maps; levelled codecs take their level; a negative
    /// level for the unsigned pair is refused with the frozen spelling.
    #[test]
    fn codecs_map_and_negative_levels_are_refused() {
        for (codec, want) in [
            (ParquetCompression::Uncompressed, Compression::UNCOMPRESSED),
            (ParquetCompression::Snappy, Compression::SNAPPY),
            (ParquetCompression::Lz4Raw, Compression::LZ4_RAW),
        ] {
            let props = writer_props(&ParquetOptions {
                compression: codec,
                ..Default::default()
            })
            .expect("valid");
            assert_eq!(props.compression(&"any".into()), want);
        }
        let zstd = writer_props(&ParquetOptions {
            compression: ParquetCompression::Zstd,
            compression_level: Some(3),
            ..Default::default()
        })
        .expect("a levelled codec takes its level");
        assert!(matches!(
            zstd.compression(&"any".into()),
            Compression::ZSTD(_)
        ));
        // Zstd's level is typed i32 (the format defines negative fast
        // levels) but the library bounds it 1..=22 — a negative level
        // is refused THROUGH the library's message, not ours.
        let err = writer_props(&ParquetOptions {
            compression: ParquetCompression::Zstd,
            compression_level: Some(-3),
            ..Default::default()
        })
        .expect_err("refused by the library bound");
        assert!(format!("{err}").contains("is not valid for zstd"), "{err}");
        for codec in [ParquetCompression::Gzip, ParquetCompression::Brotli] {
            let err = writer_props(&ParquetOptions {
                compression: codec,
                compression_level: Some(-1),
                ..Default::default()
            })
            .expect_err("refused");
            let text = format!("{err}");
            assert!(
                text.contains("is negative")
                    && text.contains(&format!("{} levels start at 0", codec.as_str())),
                "{text}"
            );
        }
    }

    /// No partitioning = one whole group; partitioning groups rows by
    /// the sanitized value, NULL under `__null__`, deterministic order.
    #[test]
    fn the_split_groups_by_sanitized_value_with_null_partition() {
        let table = TableName::new("events");
        let b = batch(&[Some("eu"), None, Some("us/east"), Some("eu")]);
        let whole = split_partitions(&table, &b, None).expect("whole");
        assert_eq!(whole.len(), 1);
        assert!(whole[0].0.is_none());
        assert_eq!(whole[0].1.num_rows(), 4);

        let split = split_partitions(&table, &b, Some("region")).expect("split");
        let names: Vec<&str> = split.iter().map(|(s, _)| s.as_deref().unwrap()).collect();
        assert_eq!(names, vec!["__null__", "eu", "us_east"]);
        let rows: Vec<usize> = split.iter().map(|(_, b)| b.num_rows()).collect();
        assert_eq!(rows, vec![1, 2, 1]);
    }

    /// A missing partition column is refused with the frozen spelling,
    /// naming the columns that DO exist.
    #[test]
    fn a_missing_partition_column_is_refused_by_name() {
        let table = TableName::new("events");
        let err =
            split_partitions(&table, &batch(&[Some("eu")]), Some("zone")).expect_err("refused");
        assert_eq!(
            format!("{err}"),
            "fatal destination error: partition_by column `zone` does not exist in stream \
             `events` (columns: [\"id\", \"region\"])"
        );
    }

    /// Both encoders round-trip: parquet bytes carry a readable footer
    /// with the row count, jsonl bytes are one JSON object per line.
    #[test]
    fn both_encoders_produce_readable_parts() {
        let b = batch(&[Some("eu"), Some("us")]);
        let props = writer_props(&ParquetOptions::default()).expect("valid");
        let parquet_bytes = encode_part(DestFormat::Parquet, &b, &props).expect("encodes");
        let reader =
            parquet::file::reader::SerializedFileReader::new(bytes::Bytes::from(parquet_bytes))
                .expect("readable");
        use parquet::file::reader::FileReader;
        assert_eq!(reader.metadata().file_metadata().num_rows(), 2);

        let jsonl_bytes = encode_part(DestFormat::Jsonl, &b, &props).expect("encodes");
        let text = String::from_utf8(jsonl_bytes).expect("utf8");
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        for line in lines {
            serde_json::from_str::<serde_json::Value>(line).expect("each line is JSON");
        }
    }
}
