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

/// When an output part is closed and the next one started.
///
/// SHARED across every destination that materialises files — the
/// file family, iceberg and snowflake — so the vocabulary and the
/// rule are written once. A per-connector spelling would mean three
/// defaults and three rolling bugs.
///
/// # What `target_bytes` measures
///
/// The ENCODED size of the part being written: what lands on disk or
/// in the object store, after compression. This is deliberately NOT
/// the engine's `batch_policy.every_bytes`, which measures the Arrow
/// IN-MEMORY footprint — snappy parquet can be 5-10x smaller than
/// its Arrow form, and the ratio moves with the data, so in-memory
/// size is not a usable proxy for file size.
///
/// # It is a floor, not a target
///
/// A batch is never split, so a part is closed AFTER crossing the
/// threshold. A 128 MiB target with large batches yields 128-140 MiB
/// files.
///
/// # A part never spans a commit
///
/// The publish protocols need whole files in a commit, so a commit
/// closes the open part. The commit cadence is therefore an UPPER
/// BOUND on part size: parts cannot grow past what one commit unit
/// contains.
///
/// # What each adopting destination does with these
///
/// `target_bytes` and `roll_after_seconds` are behavioural promises
/// and every adopting destination honours both. `max_open_bytes` is a
/// resource bound, and a destination that streams its part out rather
/// than accumulating it in memory MEETS the bound trivially — Iceberg
/// hands rows to the library's own rolling file writer, so nothing
/// accumulates for the ceiling to cap. It is satisfied there, not
/// ignored.
///
/// A destination that cannot honour a field must REFUSE it at its
/// config gate. Accepting a setting and not applying it is the defect
/// class this whole feature exists to end.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct PartOptions {
    /// Close the part once its ENCODED size reaches this many bytes.
    ///
    /// Defaults to 128 MiB — the size query engines and object stores
    /// are happiest with. Without it a destination writes one part
    /// per batch handed to it, which on a paging source means many
    /// small files: the data-lake anti-pattern.
    ///
    /// `None` disables size-based rolling entirely.
    #[serde(default = "default_target_bytes")]
    pub target_bytes: Option<u64>,

    /// Close the part after this many seconds, whichever comes first.
    ///
    /// Defaults to OFF, and it is an approximation worth
    /// understanding: it is evaluated when a write arrives, not on a
    /// background timer, so a stream that goes quiet holds its part
    /// open until the next write or the commit. It bounds staleness
    /// on a BUSY stream; it cannot bound it on an idle one.
    #[serde(default = "default_roll_after_seconds")]
    pub roll_after_seconds: Option<u32>,
    /// The SAFETY VALVE, not a tuning knob: the most memory all open
    /// parts may hold between them before the largest is closed early.
    ///
    /// An open part lives in RAM until it closes, and a partitioned
    /// destination holds ONE PER PARTITION — so without a ceiling the
    /// footprint is `partitions × target_bytes`, which a 128 MiB
    /// target turns into gigabytes at a hundred partitions. MEASURED:
    /// 1.5M rows across 97 partitions in one commit peaked at 538 MB
    /// RSS with every part still open.
    #[serde(default = "default_max_open_bytes")]
    pub max_open_bytes: Option<u64>,
}

/// 128 MiB. See [`PartOptions::target_bytes`].
fn default_target_bytes() -> Option<u64> {
    Some(128 * 1024 * 1024)
}

/// Off. See [`PartOptions::roll_after_seconds`].
fn default_roll_after_seconds() -> Option<u32> {
    None
}

/// 512 MiB — four default-sized parts. See
/// [`PartOptions::max_open_bytes`].
fn default_max_open_bytes() -> Option<u64> {
    Some(512 * 1024 * 1024)
}

impl Default for PartOptions {
    fn default() -> Self {
        Self {
            target_bytes: default_target_bytes(),
            roll_after_seconds: default_roll_after_seconds(),
            max_open_bytes: default_max_open_bytes(),
        }
    }
}

/// Why a `parts` block was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum PartsError {
    /// `target_bytes: 0` — every batch would close its own part.
    #[error(
        "`target_bytes` is 0 — a part must hold something; remove the setting to use \
         the 128 MiB default, or give a positive size"
    )]
    ZeroTargetBytes,
    /// `roll_after_seconds: 0` — the same, in the time dimension.
    #[error(
        "`roll_after_seconds` is 0 — a part would close on every write; remove the \
         setting, or give a positive number of seconds"
    )]
    ZeroRollSeconds,
    /// `max_open_bytes: 0` — no part could hold anything.
    #[error(
        "`max_open_bytes` is 0 — open parts must be allowed some memory; remove the \
         setting to use the 512 MiB default, or give a positive size"
    )]
    ZeroMaxOpenBytes,
    /// A memory ceiling below the size parts are asked to reach: the
    /// target could never be met, and would be missed SILENTLY.
    #[error(
        "`max_open_bytes` ({budget}) is below `target_bytes` ({target}) — no part could \
         ever reach its target before the memory ceiling closed it; raise the ceiling, \
         or lower the target"
    )]
    BudgetBelowTarget {
        /// The configured memory ceiling.
        budget: u64,
        /// The configured part target it cannot accommodate.
        target: u64,
    },
}

impl PartOptions {
    /// Refuse settings that cannot be honoured, naming the offender.
    ///
    /// Zero is the whole rulebook. `should_roll` already refuses to
    /// close an empty part, so a zero threshold would not spin — it
    /// would silently degrade to one part per batch, which is what
    /// `parts` exists to stop. Say so instead.
    pub fn validate(&self) -> Result<(), PartsError> {
        if self.target_bytes == Some(0) {
            return Err(PartsError::ZeroTargetBytes);
        }
        if self.roll_after_seconds == Some(0) {
            return Err(PartsError::ZeroRollSeconds);
        }
        match (self.max_open_bytes, self.target_bytes) {
            (Some(0), _) => return Err(PartsError::ZeroMaxOpenBytes),
            (Some(budget), Some(target)) if budget < target => {
                return Err(PartsError::BudgetBelowTarget { budget, target });
            }
            _ => {}
        }
        Ok(())
    }

    /// Never roll — no size target, no time bound, and NO memory
    /// ceiling either, because a ceiling splits parts exactly like a
    /// target does and "unbounded" must not quietly mean "512 MiB".
    ///
    /// NOT "one part per write" — a part still closes at every commit,
    /// because no part may span one. This is therefore ONE part per
    /// table, partition and commit, however much lands in it — and the
    /// caller accepts that an accumulating destination holds that much
    /// in MEMORY. The default exists precisely so nobody gets these
    /// semantics without asking.
    #[must_use]
    pub fn unbounded() -> Self {
        Self {
            target_bytes: None,
            roll_after_seconds: None,
            max_open_bytes: None,
        }
    }

    /// Close a part after EVERY write — the behaviour that existed
    /// before parts had a size.
    ///
    /// Spelled as a one-byte target rather than its own variant: a
    /// part that has taken a batch is always at least one byte, so the
    /// threshold trips on every write and on no empty one.
    #[must_use]
    pub fn per_write() -> Self {
        Self {
            target_bytes: Some(1),
            roll_after_seconds: None,
            max_open_bytes: default_max_open_bytes(),
        }
    }

    /// The size threshold as a destination that does its OWN rolling
    /// wants it: a plain byte count, with `None` spelled as "never".
    ///
    /// Iceberg hands this to the library's rolling file writer, which
    /// takes a `usize` and has no way to say "no limit" — so `None`
    /// becomes the largest value it can hold, which no file reaches.
    #[must_use]
    pub fn target_file_size(&self) -> usize {
        self.target_bytes
            .and_then(|bytes| usize::try_from(bytes).ok())
            .unwrap_or(usize::MAX)
    }

    /// Has an open part been open long enough to close on time alone?
    ///
    /// Split out of [`PartOptions::should_roll`] for destinations that
    /// delegate SIZE to something else — the size half would be
    /// answered twice, and the two answers would disagree.
    #[must_use]
    pub fn rolls_on_time(&self, open_for_secs: u64) -> bool {
        self.roll_after_seconds
            .is_some_and(|secs| open_for_secs >= u64::from(secs))
    }

    /// Are the open parts together holding too much?
    ///
    /// Answered across ALL open parts rather than per part, because
    /// the thing being protected is the process, and one destination
    /// can hold a part open per table and partition at once.
    #[must_use]
    pub fn over_budget(&self, total_open_bytes: u64) -> bool {
        self.max_open_bytes
            .is_some_and(|budget| total_open_bytes > budget.max(1))
    }

    /// Should the open part be closed now?
    ///
    /// `true` when ANY threshold is reached — a disjunction, matching
    /// `CommitPolicy` and `BatchPolicy`.
    #[must_use]
    pub fn should_roll(&self, encoded_bytes: u64, open_for_secs: u64) -> bool {
        // `max(1)` so a zero threshold cannot close an EMPTY part and
        // spin: a part must hold something to be worth closing.
        self.target_bytes
            .is_some_and(|target| encoded_bytes >= target.max(1))
            || (encoded_bytes > 0
                && self
                    .roll_after_seconds
                    .is_some_and(|secs| open_for_secs >= u64::from(secs)))
    }
}

#[cfg(test)]
mod part_options_tests {
    use super::PartOptions;

    /// The default is 128 MiB and no time bound.
    #[test]
    fn the_default_targets_128_mib() {
        let options = PartOptions::default();
        assert_eq!(options.target_bytes, Some(128 * 1024 * 1024));
        assert_eq!(options.roll_after_seconds, None);
        assert!(!options.should_roll(1, 0));
        assert!(options.should_roll(128 * 1024 * 1024, 0));
    }

    /// Whichever threshold is reached first closes the part.
    #[test]
    fn any_threshold_alone_rolls() {
        let options = PartOptions {
            target_bytes: Some(1_000),
            roll_after_seconds: Some(60),
            ..PartOptions::default()
        };
        assert!(options.should_roll(1_000, 0), "size alone");
        assert!(options.should_roll(1, 60), "time alone");
        assert!(!options.should_roll(999, 59));
    }

    /// An EMPTY part is never rolled by time — closing nothing would
    /// mint empty files forever on an idle stream.
    #[test]
    fn time_never_rolls_an_empty_part() {
        let options = PartOptions {
            target_bytes: None,
            roll_after_seconds: Some(1),
            ..PartOptions::default()
        };
        assert!(!options.should_roll(0, 86_400));
        assert!(options.should_roll(1, 1));
    }

    /// Zero says "one part per batch" without saying so — refused.
    #[test]
    fn a_zero_threshold_is_refused_by_name() {
        let zero_bytes = PartOptions {
            target_bytes: Some(0),
            ..PartOptions::default()
        };
        assert_eq!(
            zero_bytes.validate(),
            Err(super::PartsError::ZeroTargetBytes)
        );
        let zero_secs = PartOptions {
            roll_after_seconds: Some(0),
            ..PartOptions::default()
        };
        assert_eq!(
            zero_secs.validate(),
            Err(super::PartsError::ZeroRollSeconds)
        );
        assert_eq!(PartOptions::default().validate(), Ok(()));
        assert_eq!(PartOptions::unbounded().validate(), Ok(()));
    }

    #[test]
    fn unbounded_never_rolls() {
        let options = PartOptions::unbounded();
        assert!(!options.should_roll(u64::MAX, u64::MAX));
        // Including the memory ceiling: review round 3 caught the
        // constructor keeping the 512 MiB default, which split parts
        // on the accumulating destinations while the doc promised one
        // part per commit "however much lands in it".
        assert!(!options.over_budget(u64::MAX));
    }

    /// `per_write` rolls on any content and on no empty part.
    /// The ceiling is answered across ALL open parts, and a ceiling
    /// below the target is refused rather than silently missing it.
    #[test]
    fn the_memory_ceiling_is_a_total_and_must_admit_the_target() {
        let options = PartOptions::default();
        assert!(!options.over_budget(512 * 1024 * 1024));
        assert!(options.over_budget(512 * 1024 * 1024 + 1));

        let contradiction = PartOptions {
            target_bytes: Some(1_024),
            max_open_bytes: Some(512),
            ..PartOptions::default()
        };
        assert_eq!(
            contradiction.validate(),
            Err(super::PartsError::BudgetBelowTarget {
                budget: 512,
                target: 1_024,
            })
        );

        // No ceiling means no budget question to answer.
        let uncapped = PartOptions {
            max_open_bytes: None,
            ..PartOptions::default()
        };
        assert!(!uncapped.over_budget(u64::MAX));
        assert_eq!(uncapped.validate(), Ok(()));
    }

    #[test]
    fn per_write_rolls_on_every_written_batch() {
        let options = PartOptions::per_write();
        assert!(options.should_roll(1, 0));
        assert!(!options.should_roll(0, 0));
        assert_eq!(options.validate(), Ok(()));
    }

    /// The serde-default trap this module already documents: an
    /// omitted field must take the DOCUMENTED default, not the
    /// field type's.
    #[test]
    fn omitted_fields_take_the_documented_defaults() {
        let options: PartOptions = serde_json::from_str("{}").expect("empty document");
        assert_eq!(options, PartOptions::default());
        let sized: PartOptions =
            serde_json::from_str(r#"{"target_bytes": 1024}"#).expect("partial document");
        assert_eq!(sized.target_bytes, Some(1024));
        assert_eq!(sized.roll_after_seconds, None);
    }
}
