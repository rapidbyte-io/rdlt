//! How a destination writes parquet — stated intentions, not a builder.
//!
//! One shared vocabulary for every connector that writes parquet, on the
//! [`crate::Secret`] pattern: plain data in the SPI, `schemars` behind the
//! `schema` feature, re-exported from each connector's own config path.
//! **The SPI gains no parquet dependency** — each connector translates
//! these intentions into its parquet library's `WriterProperties` at its
//! own boundary, where that library is already a dependency.
//!
//! # Why these defaults
//!
//! Left to the library's defaults, every writer in the workspace produced
//! UNCOMPRESSED output (210.0 MB where a comparable tool wrote 73.7 MB
//! for the same million rows) — so the defaults here exist to make the
//! artifact comparable at all. And compression alone is a trap: parquet
//! dictionary-encodes by default, and a high-cardinality column (ids,
//! UUIDs, free text) interns nearly every distinct value before its
//! dictionary page hits the limit — turning a compressor on while leaving
//! the 1 MiB library limit made encoder CPU RISE. A lower page limit lets
//! such a column abandon dictionary encoding early and fall back to plain
//! encoding, which is what makes compression pay; low-cardinality columns
//! fill small dictionaries and never notice.

use serde::{Deserialize, Serialize};

/// Parquet codecs, spelled as configuration writes them.
///
/// `#[non_exhaustive]`: parquet grows codecs, and adding one must not
/// break anyone matching on this.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[non_exhaustive]
pub enum ParquetCompression {
    /// No compression.
    Uncompressed,
    /// The default: consistently the best speed-for-size trade on this
    /// workload, and what the ecosystem assumes a parquet file carries.
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
    /// Whether this codec has a level to set.
    ///
    /// Snappy and LZ4_RAW define a single mode — naming a level beside
    /// them is a mistake worth refusing, because silently dropping it
    /// would leave the user believing they tuned something.
    pub fn takes_level(self) -> bool {
        matches!(self, Self::Gzip | Self::Zstd | Self::Brotli)
    }

    /// The configuration spelling, for error messages and diagnostics.
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

/// The default dictionary page limit: 64 KiB, 16× below parquet's own
/// 1 MiB default.
///
/// Chosen by a recorded sweep (200k rows, all-distinct string column,
/// snappy, median of 5), not by taste: high-cardinality encoding is flat
/// from 4 KiB to 64 KiB and degrades sharply above — at 1 MiB it costs
/// 68% more CPU and produces a LARGER file — while low-cardinality
/// encoding is flat across the whole range, so a lower cap takes nothing
/// from the columns dictionaries actually help. 64 KiB is the TOP of the
/// flat region on purpose: 4 and 16 KiB are no faster, and a smaller cap
/// would abandon dictionary encoding for medium-cardinality columns that
/// 64 KiB still serves.
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

/// How to write parquet. Every field is optional in configuration and
/// falls back to its documented default.
///
/// # The serde-default trap
///
/// Every field names its own default function. A bare `#[serde(default)]`
/// would silently invert the intent: it calls `Default::default()` on the
/// FIELD TYPE, so an omitted `dictionary_enabled` would come back `false`
/// and omitted limits `0`. The struct's own `Default` impl delegates to
/// the same functions so the two paths cannot drift.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ParquetOptions {
    /// Compression codec; defaults to `snappy`.
    #[serde(default = "default_compression")]
    pub compression: ParquetCompression,

    /// Compression level, for codecs that have one — refused for codecs
    /// that do not (see [`ParquetCompression::takes_level`]). `None`
    /// leaves the codec's own default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression_level: Option<i32>,

    /// Whether to dictionary-encode at all. On by default, as in parquet
    /// itself; off suits data known to be high-cardinality throughout.
    #[serde(default = "default_dictionary_enabled")]
    pub dictionary_enabled: bool,

    /// Bytes a column's dictionary page may reach before that column
    /// abandons dictionary encoding for the rest of the row group.
    #[serde(default = "default_dictionary_page_size_limit")]
    pub dictionary_page_size_limit: usize,

    /// Target data-page size in bytes. `None` leaves the library default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_page_size_limit: Option<usize>,

    /// Maximum ROWS per row group; `None` leaves the library's default
    /// (1,048,576).
    ///
    /// Rows, not bytes — parquet 58 deprecated the byte-oriented setter.
    /// Two facts about the library's row-count setter shape the code that
    /// consumes this field: its `None` means UNLIMITED (not "default"),
    /// so a translator must skip the call entirely when this is `None`;
    /// and it panics on `Some(0)`, which is why zero is refused in
    /// [`ParquetOptions::validate`] before it can reach the panic.
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

/// A parquet setting that cannot be honoured, named by what failed.
///
/// `#[non_exhaustive]`: validation can learn new refusals without a
/// breaking change.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum OptionsError {
    /// A `compression_level` beside a codec that defines a single mode.
    #[error(
        "`compression_level` is set but `{codec}` has no compression level — \
         remove the level, or choose a codec that takes one (gzip, zstd, brotli)"
    )]
    LevelOnLevellessCodec {
        /// The configuration spelling of the levelless codec.
        codec: &'static str,
    },
    /// `max_row_group_rows: 0`, which the parquet setter would panic on.
    #[error(
        "`max_row_group_rows` is 0 — a row group must hold at least one row; \
         remove the setting to use the default, or give a positive count"
    )]
    ZeroRowGroupRows,
    /// A zero-byte dictionary page while dictionary encoding is enabled.
    #[error(
        "`dictionary_page_size_limit` is 0 while dictionary encoding is enabled — \
         a dictionary page cannot be zero bytes; raise the limit, or set \
         `dictionary_enabled: false` to disable dictionary encoding outright"
    )]
    ZeroDictionaryPageLimit,
}

impl ParquetOptions {
    /// Refuse settings that cannot be honoured, naming the offender.
    ///
    /// Only rules decidable from these fields alone live here; whether a
    /// `parquet` block belongs on a destination at all depends on that
    /// destination's sibling `format` field, so that rule lives where
    /// `format` is in scope.
    pub fn validate(&self) -> Result<(), OptionsError> {
        if self.compression_level.is_some() && !self.compression.takes_level() {
            return Err(OptionsError::LevelOnLevellessCodec {
                codec: self.compression.as_str(),
            });
        }
        if self.max_row_group_rows == Some(0) {
            return Err(OptionsError::ZeroRowGroupRows);
        }
        if self.dictionary_enabled && self.dictionary_page_size_limit == 0 {
            return Err(OptionsError::ZeroDictionaryPageLimit);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The trap this type exists to dodge: omitted fields take the
    /// DOCUMENTED defaults, not the field types' `Default`s (which would
    /// be `false` and `0`).
    #[test]
    fn an_empty_block_takes_the_documented_defaults() {
        let parsed: ParquetOptions = serde_json::from_str("{}").expect("empty block valid");
        assert_eq!(parsed, ParquetOptions::default());
        assert_eq!(parsed.compression, ParquetCompression::Snappy);
        assert!(parsed.dictionary_enabled, "must not default to false");
        assert_eq!(
            parsed.dictionary_page_size_limit, DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT,
            "must not default to 0"
        );
        // The constant itself is a measured decision; drifting it should
        // be a deliberate act, not a refactor side effect.
        assert_eq!(DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT, 65_536);
    }

    #[test]
    fn a_partial_block_keeps_the_defaults_it_leaves_out() {
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
    fn unknown_settings_are_refused_naming_the_typo() {
        let error = serde_json::from_str::<ParquetOptions>(r#"{"compresion": "zstd"}"#)
            .expect_err("typos must not be dropped");
        assert!(error.to_string().contains("compresion"), "{error}");
    }

    #[test]
    fn a_level_on_a_levelless_codec_is_refused_naming_the_codec() {
        let refused = ParquetOptions {
            compression: ParquetCompression::Snappy,
            compression_level: Some(3),
            ..Default::default()
        }
        .validate()
        .expect_err("snappy has no level")
        .to_string();
        assert!(
            refused.contains("snappy") && refused.contains("compression_level"),
            "{refused}"
        );
        assert!(
            ParquetOptions {
                compression: ParquetCompression::Zstd,
                compression_level: Some(3),
                ..Default::default()
            }
            .validate()
            .is_ok(),
            "the same level is fine on a levelled codec"
        );
    }

    /// Zero is refused HERE because the parquet setter panics on it — a
    /// library panic is no way to report a configuration mistake.
    #[test]
    fn zero_row_group_rows_is_refused_before_the_library_can_panic() {
        let refused = ParquetOptions {
            max_row_group_rows: Some(0),
            ..Default::default()
        }
        .validate()
        .expect_err("zero rows per group is impossible")
        .to_string();
        assert!(refused.contains("max_row_group_rows"), "{refused}");
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
    fn zero_dictionary_limit_is_refused_only_while_encoding_is_enabled() {
        let refused = ParquetOptions {
            dictionary_page_size_limit: 0,
            ..Default::default()
        }
        .validate()
        .expect_err("an enabled zero-byte dictionary is impossible")
        .to_string();
        assert!(refused.contains("dictionary_page_size_limit"), "{refused}");
        assert!(
            ParquetOptions {
                dictionary_enabled: false,
                dictionary_page_size_limit: 0,
                ..Default::default()
            }
            .validate()
            .is_ok(),
            "with encoding off the limit is inert"
        );
    }

    #[test]
    fn levelled_codecs_are_exactly_the_three_that_take_a_level() {
        for levelled in [
            ParquetCompression::Gzip,
            ParquetCompression::Zstd,
            ParquetCompression::Brotli,
        ] {
            assert!(levelled.takes_level(), "{}", levelled.as_str());
        }
        for levelless in [
            ParquetCompression::Uncompressed,
            ParquetCompression::Snappy,
            ParquetCompression::Lz4Raw,
        ] {
            assert!(!levelless.takes_level(), "{}", levelless.as_str());
        }
    }

    /// Config documents keep their spelling in and out.
    #[test]
    fn codec_spellings_round_trip_as_written() {
        for (spelling, codec) in [
            ("uncompressed", ParquetCompression::Uncompressed),
            ("snappy", ParquetCompression::Snappy),
            ("gzip", ParquetCompression::Gzip),
            ("lz4_raw", ParquetCompression::Lz4Raw),
            ("zstd", ParquetCompression::Zstd),
            ("brotli", ParquetCompression::Brotli),
        ] {
            let parsed: ParquetCompression =
                serde_json::from_str(&format!("\"{spelling}\"")).expect(spelling);
            assert_eq!(parsed, codec);
            assert_eq!(
                serde_json::to_string(&codec).expect(spelling),
                format!("\"{spelling}\"")
            );
            assert_eq!(codec.as_str(), spelling);
        }
    }
}
