//! The ONE place this crate turns [`ParquetOptions`] into the parquet
//! library's `WriterProperties` (Principle III: wrap a library at a single
//! boundary).
//!
//! [`ParquetOptions`] is plain data in the SPI and knows nothing about
//! parquet. Everything version-specific — which setter is deprecated, which
//! codecs carry a level and in what integer type, which setter panics on zero
//! — is confined to this file, so a parquet upgrade has exactly one place to
//! break.

use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;
use rdlt_connector::{ParquetCompression, ParquetOptions};

/// Translate, or explain what could not be honoured.
///
/// Returns a plain `String`; the caller maps it to a typed `DestinationError`
/// at the SPI boundary, following the same convention as config validation.
pub(super) fn writer_properties(options: &ParquetOptions) -> Result<WriterProperties, String> {
    let mut builder = WriterProperties::builder()
        .set_compression(compression(options)?)
        .set_dictionary_enabled(options.dictionary_enabled)
        .set_dictionary_page_size_limit(options.dictionary_page_size_limit);

    if let Some(limit) = options.data_page_size_limit {
        builder = builder.set_data_page_size_limit(limit);
    }
    // Called ONLY when set. The setter takes `Option<usize>` where `None`
    // means UNLIMITED, not "use the default" — passing this field straight
    // through would silently replace parquet's 1,048,576-row default with
    // unbounded row groups. It also panics on `Some(0)`, which
    // `ParquetOptions::validate` refuses before we get here.
    if options.max_row_group_rows.is_some() {
        builder = builder.set_max_row_group_row_count(options.max_row_group_rows);
    }
    Ok(builder.build())
}

/// The codec, with its level applied where the codec has one.
///
/// A level for a levelless codec is refused during validation, so reaching
/// here with one would be a bug; it is still handled rather than ignored.
fn compression(options: &ParquetOptions) -> Result<Compression, String> {
    let level = options.compression_level;
    // Gzip and Brotli take an unsigned level, Zstd a signed one (it defines
    // negative "fast" levels). A negative value for the first two is a
    // configuration mistake, not a clamp.
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
        (ParquetCompression::Gzip, Some(_)) => Compression::GZIP(
            GzipLevel::try_new(unsigned("gzip")?).map_err(|e| bad("gzip", e))?,
        ),
        (ParquetCompression::Brotli, None) => Compression::BROTLI(BrotliLevel::default()),
        (ParquetCompression::Brotli, Some(_)) => Compression::BROTLI(
            BrotliLevel::try_new(unsigned("brotli")?).map_err(|e| bad("brotli", e))?,
        ),
        (ParquetCompression::Zstd, None) => Compression::ZSTD(ZstdLevel::default()),
        (ParquetCompression::Zstd, Some(level)) => {
            Compression::ZSTD(ZstdLevel::try_new(level).map_err(|e| bad("zstd", e))?)
        }
        // `ParquetCompression` is `#[non_exhaustive]`: a codec added to the SPI
        // that this crate has not been taught is refused by name rather than
        // silently written uncompressed.
        (other, _) => {
            return Err(format!(
                "compression `{}` is not supported by the file destination",
                other.as_str()
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shipped defaults must produce COMPRESSED output. Before this
    /// existed every parquet writer here used the library default, which is
    /// uncompressed — the whole reason rdlt wrote 210 MB where dlt wrote 74.
    #[test]
    fn the_default_options_compress() {
        let props = writer_properties(&ParquetOptions::default()).expect("defaults are valid");
        assert_eq!(
            props.compression(&"any".into()),
            Compression::SNAPPY,
            "default output must be compressed"
        );
    }

    /// The dictionary limit must actually reach the writer — this is the
    /// setting that decides whether compression pays or costs on
    /// high-cardinality columns.
    #[test]
    fn the_dictionary_limit_reaches_the_writer_and_is_below_the_library_default() {
        let props = writer_properties(&ParquetOptions::default()).expect("valid");
        let ours = props.dictionary_page_size_limit();
        let library_default = WriterProperties::builder().build().dictionary_page_size_limit();
        assert!(
            ours < library_default,
            "ours {ours} must be below the library's {library_default}"
        );
        assert!(props.dictionary_enabled(&"any".into()));
    }

    /// `None` must NOT be passed through: the setter reads it as unlimited.
    #[test]
    fn an_unset_row_group_count_keeps_the_library_default_not_unlimited() {
        let props = writer_properties(&ParquetOptions::default()).expect("valid");
        let library_default = WriterProperties::builder().build().max_row_group_row_count();
        assert_eq!(
            props.max_row_group_row_count(),
            library_default,
            "leaving the setting unset must not mean unlimited"
        );
        assert!(
            library_default.is_some(),
            "the library default is a bound, so `None` here would be a real change"
        );
    }

    #[test]
    fn a_set_row_group_count_reaches_the_writer() {
        let props = writer_properties(&ParquetOptions {
            max_row_group_rows: Some(5_000),
            ..Default::default()
        })
        .expect("valid");
        assert_eq!(props.max_row_group_row_count(), Some(5_000));
    }

    #[test]
    fn every_codec_translates() {
        for (codec, expected) in [
            (ParquetCompression::Uncompressed, Compression::UNCOMPRESSED),
            (ParquetCompression::Snappy, Compression::SNAPPY),
            (ParquetCompression::Lz4Raw, Compression::LZ4_RAW),
        ] {
            let props = writer_properties(&ParquetOptions {
                compression: codec,
                ..Default::default()
            })
            .expect(codec.as_str());
            assert_eq!(props.compression(&"any".into()), expected);
        }
        // Levelled codecs translate with and without an explicit level.
        for codec in [
            ParquetCompression::Gzip,
            ParquetCompression::Zstd,
            ParquetCompression::Brotli,
        ] {
            assert!(
                writer_properties(&ParquetOptions {
                    compression: codec,
                    ..Default::default()
                })
                .is_ok(),
                "{} without a level",
                codec.as_str()
            );
            assert!(
                writer_properties(&ParquetOptions {
                    compression: codec,
                    compression_level: Some(3),
                    ..Default::default()
                })
                .is_ok(),
                "{} with level 3",
                codec.as_str()
            );
        }
    }

    /// An out-of-range level is a typed message, never a panic.
    #[test]
    fn an_out_of_range_level_is_reported_not_panicked() {
        let err = writer_properties(&ParquetOptions {
            compression: ParquetCompression::Gzip,
            compression_level: Some(9_999),
            ..Default::default()
        })
        .expect_err("gzip has no level 9999");
        assert!(err.contains("compression_level"), "{err}");

        let err = writer_properties(&ParquetOptions {
            compression: ParquetCompression::Gzip,
            compression_level: Some(-1),
            ..Default::default()
        })
        .expect_err("gzip levels are unsigned");
        assert!(err.contains("negative"), "{err}");
    }
}
