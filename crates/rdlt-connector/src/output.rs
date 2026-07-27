//! [`ParquetOptions`] — how a destination should write parquet files.
//!
//! One shared type for every connector that writes parquet, following the
//! [`crate::Secret`] precedent: plain data in the SPI, `schemars` behind the
//! crate's `schema` feature, re-exported from each connector's own config
//! path.
//!
//! **The SPI gains no parquet dependency.** These are the user's stated
//! intentions, not a builder: each connector translates them into that
//! library's `WriterProperties` at its own boundary, which is where the
//! library is already a dependency (Principle III — a library type never
//! crosses the public surface).
//!
//! # Why the defaults are what they are
//!
//! Before this existed, every parquet writer in the workspace used the
//! library's defaults, which meant **uncompressed output**. rdlt wrote 210.0 MB
//! where dlt wrote 73.7 MB for the same million rows — so the two were not
//! writing comparable artifacts, and a benchmark comparing them was comparing
//! different work.
//!
//! Compression alone is not the whole answer, and this is the part that is
//! easy to get wrong: parquet dictionary-encodes by default, and a dictionary
//! page grows until it hits `dictionary_page_size_limit`. On a
//! high-cardinality column — an id, a UUID, a free-text field — that means
//! interning close to every distinct value before giving up. Turning on a
//! compressor and leaving the limit at its default therefore makes encoder CPU
//! *rise*: the dictionary work still happens, and now its output is compressed
//! too. Lowering the limit lets such a column abandon dictionary encoding
//! early and fall back to plain encoding, which is what makes compression pay.
//! A low-cardinality column fills a much smaller dictionary and is unaffected.

use serde::{Deserialize, Serialize};

/// Parquet codecs, spelled as users write them in configuration.
///
/// `#[non_exhaustive]` because parquet gains codecs: adding one must not be a
/// breaking change for anyone matching on this.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[non_exhaustive]
pub enum ParquetCompression {
    /// No compression — what every rdlt parquet writer did before this type
    /// existed.
    Uncompressed,
    /// The default: consistently the best speed-for-size trade on this
    /// workload, and what the ecosystem assumes when it reads a parquet file.
    #[default]
    Snappy,
    /// Widely readable, slower than Snappy; accepts a level.
    Gzip,
    /// The frame-less LZ4 variant parquet readers expect. No level.
    Lz4Raw,
    /// Smaller than Snappy at more CPU; accepts a level.
    Zstd,
    /// Smallest of these at the most CPU; accepts a level.
    Brotli,
}

impl ParquetCompression {
    /// Whether this codec accepts a compression level.
    ///
    /// Snappy and LZ4_RAW have no level to set — the format defines a single
    /// mode — so naming one alongside them is a mistake worth refusing rather
    /// than ignoring, since silently dropping it would leave the user
    /// believing they had tuned something.
    pub fn takes_level(self) -> bool {
        matches!(self, Self::Gzip | Self::Zstd | Self::Brotli)
    }

    /// The spelling used in error messages and diagnostics.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Uncompressed => "uncompressed",
            Self::Snappy => "snappy",
            Self::Gzip => "gzip",
            Self::Lz4Raw => "lz4_raw",
            Self::Zstd => "zstd",
            Self::Brotli => "brotli",
        }
    }
}

/// The default dictionary page size limit, in bytes — 16x below parquet's own
/// default of 1 MiB.
///
/// Chosen by sweep, not by taste. Writing 200k rows of an all-distinct string
/// column with snappy, median of 5 (the low-cardinality shape is in the second
/// column, and is the reason this default is safe):
///
/// | limit  | high-card µs | high-card KiB | low-card µs |
/// |--------|-------------:|--------------:|------------:|
/// | 4 KiB  |         4397 |          1770 |        3664 |
/// | 16 KiB |         4280 |          1771 |        3545 |
/// | 64 KiB |         4322 |          1783 |        3571 |
/// | 256 KiB|         4936 |          1839 |        3558 |
/// | 1 MiB  |         7280 |          2079 |        3569 |
///
/// Two things decide the value. High-cardinality encoding is flat from 4 KiB
/// to 64 KiB and degrades sharply above it — 1 MiB costs 68% more CPU and
/// produces a LARGER file, because the column interns nearly every distinct
/// value before giving up. And low-cardinality encoding is flat across the
/// whole range, so a lower cap takes nothing away from the columns dictionary
/// encoding actually helps.
///
/// 64 KiB is the top of the flat region rather than the bottom on purpose:
/// 4 and 16 KiB are no faster, and a smaller cap would abandon dictionary
/// encoding for medium-cardinality columns that 64 KiB still serves.
const DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT: usize = 64 * 1024;

const fn default_compression() -> ParquetCompression {
    ParquetCompression::Snappy
}

const fn default_dictionary_enabled() -> bool {
    true
}

const fn default_dictionary_page_size_limit() -> usize {
    DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT
}

/// How to write parquet. Every field is optional in configuration and falls
/// back to the value below it.
///
/// # The serde-default trap
///
/// Each field spells its own default with `#[serde(default = "…")]`. A bare
/// `#[serde(default)]` would be a silent inversion: it calls
/// `Default::default()` on the FIELD TYPE, so an omitted `dictionary_enabled`
/// would deserialize to `false` and the omitted limits to `0` — the opposite
/// of the intent, and `0` would then trip the row-count assert downstream.
/// The struct's own `Default` impl delegates to the same functions, so the two
/// paths cannot drift.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ParquetOptions {
    /// Compression codec. Defaults to `snappy`; `uncompressed` restores the
    /// pre-configuration behaviour.
    #[serde(default = "default_compression")]
    pub compression: ParquetCompression,

    /// Compression level, for codecs that have one. Rejected for codecs that
    /// do not — see [`ParquetCompression::takes_level`]. `None` leaves the
    /// codec's own default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression_level: Option<i32>,

    /// Whether to dictionary-encode at all. On by default, as in parquet
    /// itself; turning it off suits data known to be high-cardinality
    /// throughout.
    #[serde(default = "default_dictionary_enabled")]
    pub dictionary_enabled: bool,

    /// Bytes a column's dictionary page may reach before that column abandons
    /// dictionary encoding for the rest of the row group.
    #[serde(default = "default_dictionary_page_size_limit")]
    pub dictionary_page_size_limit: usize,

    /// Target size of a data page, in bytes. `None` leaves the library's
    /// default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_page_size_limit: Option<usize>,

    /// Maximum ROWS per row group. `None` leaves the library's default of
    /// 1,048,576.
    ///
    /// Rows, not bytes: parquet 58 deprecated the byte-oriented
    /// `set_max_row_group_size` in favour of `set_max_row_group_row_count`.
    ///
    /// Two things about that setter shape the code that consumes this field.
    /// It takes `Option<usize>` where `None` means UNLIMITED — not "use the
    /// default" — so a translator must skip the call entirely when this is
    /// `None` rather than pass it through, or a row group would grow without
    /// bound. And it panics on `Some(0)`, which is why zero is refused in
    /// [`ParquetOptions::validate`] instead of being allowed to reach it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_row_group_rows: Option<usize>,
}

impl Default for ParquetOptions {
    fn default() -> Self {
        Self {
            compression: default_compression(),
            compression_level: None,
            dictionary_enabled: default_dictionary_enabled(),
            dictionary_page_size_limit: default_dictionary_page_size_limit(),
            data_page_size_limit: None,
            max_row_group_rows: None,
        }
    }
}

impl ParquetOptions {
    /// Reject settings that cannot be honoured, naming the offending one.
    ///
    /// Only the rules decidable from these fields alone live here. Whether a
    /// `parquet` block belongs on this destination at all depends on the
    /// sibling `format` field, so that rule lives on the destination's own
    /// config where `format` is in scope.
    pub fn validate(&self) -> Result<(), String> {
        if self.compression_level.is_some() && !self.compression.takes_level() {
            return Err(format!(
                "`compression_level` is set but `{}` has no compression level — \
                 remove the level, or choose a codec that takes one (gzip, zstd, brotli)",
                self.compression.as_str()
            ));
        }
        if self.max_row_group_rows == Some(0) {
            return Err(
                "`max_row_group_rows` is 0 — a row group must hold at least one row; \
                 remove the setting to use the default, or give a positive count"
                    .into(),
            );
        }
        if self.dictionary_enabled && self.dictionary_page_size_limit == 0 {
            return Err(
                "`dictionary_page_size_limit` is 0 while dictionary encoding is enabled — \
                 a dictionary page cannot be zero bytes; raise the limit, or set \
                 `dictionary_enabled: false` to disable dictionary encoding outright"
                    .into(),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The trap this type exists to avoid: an omitted field must take the
    /// DOCUMENTED default, not the field type's `Default`. With a bare
    /// `#[serde(default)]` these would come back `false` and `0`.
    #[test]
    fn omitted_fields_take_the_documented_defaults_not_the_types() {
        let parsed: ParquetOptions = serde_json::from_str("{}").expect("empty block is valid");
        assert_eq!(parsed, ParquetOptions::default());
        assert_eq!(parsed.compression, ParquetCompression::Snappy);
        assert!(parsed.dictionary_enabled, "must not deserialize to false");
        assert_eq!(
            parsed.dictionary_page_size_limit, DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT,
            "must not deserialize to 0"
        );
        // …and the default is that LITERAL size. Comparing the parsed value to
        // the constant is tautological — it holds whatever the constant becomes
        // — so the chosen number is asserted directly. 64 KiB is a measured
        // decision (the top of the flat region; 4 and 16 KiB are no faster, and
        // a smaller cap abandons dictionary encoding for medium-cardinality
        // columns), which is exactly the kind of choice that should not drift
        // silently.
        assert_eq!(
            DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT, 65_536,
            "64 KiB, chosen by measurement — change it deliberately"
        );
    }

    /// Partial blocks keep the defaults for what they leave out.
    #[test]
    fn a_partial_block_keeps_the_other_defaults() {
        let parsed: ParquetOptions =
            serde_json::from_str(r#"{"compression": "zstd"}"#).expect("valid");
        assert_eq!(parsed.compression, ParquetCompression::Zstd);
        assert!(parsed.dictionary_enabled);
        assert_eq!(
            parsed.dictionary_page_size_limit,
            DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT
        );
    }

    #[test]
    fn unknown_settings_are_refused_rather_than_ignored() {
        let err = serde_json::from_str::<ParquetOptions>(r#"{"compresion": "zstd"}"#)
            .expect_err("a typo must not be silently dropped");
        assert!(err.to_string().contains("compresion"), "{err}");
    }

    #[test]
    fn a_level_on_a_levelless_codec_is_refused_naming_it() {
        let options = ParquetOptions {
            compression: ParquetCompression::Snappy,
            compression_level: Some(3),
            ..Default::default()
        };
        let err = options.validate().expect_err("snappy has no level");
        assert!(
            err.contains("snappy") && err.contains("compression_level"),
            "{err}"
        );
        // …and the same level is fine on a codec that has one.
        assert!(
            ParquetOptions {
                compression: ParquetCompression::Zstd,
                compression_level: Some(3),
                ..Default::default()
            }
            .validate()
            .is_ok()
        );
    }

    /// Zero is refused HERE because the parquet setter asserts on it — a
    /// panic in a library is not an acceptable way to report a config mistake.
    #[test]
    fn zero_row_group_rows_is_refused_before_it_can_panic() {
        let err = ParquetOptions {
            max_row_group_rows: Some(0),
            ..Default::default()
        }
        .validate()
        .expect_err("zero rows per row group is impossible");
        assert!(err.contains("max_row_group_rows"), "{err}");
        assert!(
            ParquetOptions {
                max_row_group_rows: Some(1),
                ..Default::default()
            }
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn levelled_codecs_are_exactly_the_ones_that_take_a_level() {
        for codec in [
            ParquetCompression::Gzip,
            ParquetCompression::Zstd,
            ParquetCompression::Brotli,
        ] {
            assert!(codec.takes_level(), "{}", codec.as_str());
        }
        for codec in [
            ParquetCompression::Uncompressed,
            ParquetCompression::Snappy,
            ParquetCompression::Lz4Raw,
        ] {
            assert!(!codec.takes_level(), "{}", codec.as_str());
        }
    }

    /// Config documents keep their spelling in and out.
    #[test]
    fn codec_names_round_trip_as_written() {
        for (text, codec) in [
            ("uncompressed", ParquetCompression::Uncompressed),
            ("snappy", ParquetCompression::Snappy),
            ("gzip", ParquetCompression::Gzip),
            ("lz4_raw", ParquetCompression::Lz4Raw),
            ("zstd", ParquetCompression::Zstd),
            ("brotli", ParquetCompression::Brotli),
        ] {
            let parsed: ParquetCompression =
                serde_json::from_str(&format!("\"{text}\"")).expect(text);
            assert_eq!(parsed, codec);
            assert_eq!(
                serde_json::to_string(&codec).expect(text),
                format!("\"{text}\"")
            );
            assert_eq!(codec.as_str(), text);
        }
    }
}
