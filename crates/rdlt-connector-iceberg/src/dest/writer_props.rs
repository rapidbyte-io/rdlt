//! The ONE place this crate turns [`ParquetOptions`] into the parquet
//! library's `WriterProperties` (Principle III).
//!
//! Deliberately a sibling of the file destination's translator rather than a
//! shared helper. Sharing would mean one of these crates depending on the
//! other, or a third crate existing to hold twenty lines — and the two are
//! free to diverge, because the constraints differ: Iceberg data files are
//! always parquet, so there is no sibling `format` field to contradict, and
//! the catalog owns file naming and rolling.

use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;
use rdlt_connector::{ParquetCompression, ParquetOptions};

/// Translate, or explain what could not be honoured. The caller maps the
/// message to a typed error at the SPI boundary.
pub(crate) fn writer_properties(options: &ParquetOptions) -> Result<WriterProperties, String> {
    let mut builder = WriterProperties::builder()
        .set_compression(compression(options)?)
        .set_dictionary_enabled(options.dictionary_enabled)
        .set_dictionary_page_size_limit(options.dictionary_page_size_limit);

    if let Some(limit) = options.data_page_size_limit {
        builder = builder.set_data_page_size_limit(limit);
    }
    // Only when set: the setter reads `None` as UNLIMITED rather than "use the
    // default", and panics on `Some(0)` — which config validation refuses
    // before we get here.
    if options.max_row_group_rows.is_some() {
        builder = builder.set_max_row_group_row_count(options.max_row_group_rows);
    }
    Ok(builder.build())
}

fn compression(options: &ParquetOptions) -> Result<Compression, String> {
    let level = options.compression_level;
    let unsigned = |what: &str| -> Result<u32, String> {
        let Some(level) = level else {
            return Err(format!("internal: {what} level requested without a value"));
        };
        u32::try_from(level).map_err(|_| {
            format!("`compression_level` {level} is negative — {what} levels start at 0")
        })
    };
    let bad = |what: &str, e: parquet::errors::ParquetError| {
        format!(
            "`compression_level` {} is not valid for {what}: {e}",
            level.unwrap_or_default()
        )
    };

    Ok(match (options.compression, level) {
        (ParquetCompression::Uncompressed, _) => Compression::UNCOMPRESSED,
        (ParquetCompression::Snappy, _) => Compression::SNAPPY,
        (ParquetCompression::Lz4Raw, _) => Compression::LZ4_RAW,
        (ParquetCompression::Gzip, None) => Compression::GZIP(GzipLevel::default()),
        (ParquetCompression::Gzip, Some(_)) => {
            Compression::GZIP(GzipLevel::try_new(unsigned("gzip")?).map_err(|e| bad("gzip", e))?)
        }
        (ParquetCompression::Brotli, None) => Compression::BROTLI(BrotliLevel::default()),
        (ParquetCompression::Brotli, Some(_)) => Compression::BROTLI(
            BrotliLevel::try_new(unsigned("brotli")?).map_err(|e| bad("brotli", e))?,
        ),
        (ParquetCompression::Zstd, None) => Compression::ZSTD(ZstdLevel::default()),
        (ParquetCompression::Zstd, Some(level)) => {
            Compression::ZSTD(ZstdLevel::try_new(level).map_err(|e| bad("zstd", e))?)
        }
        (other, _) => {
            return Err(format!(
                "compression `{}` is not supported by the iceberg destination",
                other.as_str()
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Iceberg data files compress by default too. Before this they used the
    /// library default, which is uncompressed — and unlike the file
    /// destination, an Iceberg table's files are read by every other engine
    /// pointed at the catalog.
    #[test]
    fn the_default_options_compress() {
        let props = writer_properties(&ParquetOptions::default()).expect("defaults are valid");
        assert_eq!(props.compression(&"any".into()), Compression::SNAPPY);
    }

    #[test]
    fn an_unset_row_group_count_keeps_the_library_default_not_unlimited() {
        let props = writer_properties(&ParquetOptions::default()).expect("valid");
        assert_eq!(
            props.max_row_group_row_count(),
            WriterProperties::builder().build().max_row_group_row_count()
        );
    }

    #[test]
    fn an_out_of_range_level_is_reported_not_panicked() {
        assert!(
            writer_properties(&ParquetOptions {
                compression: ParquetCompression::Gzip,
                compression_level: Some(9_999),
                ..Default::default()
            })
            .is_err()
        );
    }
}
