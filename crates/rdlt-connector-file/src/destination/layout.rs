//! The layout vocabulary: every name this destination writes, the
//! path-safety rule, and the persisted commit log — all FROZEN (the
//! staged and final names must reproduce identically under WAL
//! replay, and the receipts are the exactly-once evidence).

use rdlt_connector_sdk::spi::DestinationError;

/// The persisted layout/commit-log version this build writes.
pub(super) const LAYOUT_FORMAT_VERSION: u32 = 1;

/// The hidden staging prefix.
pub(crate) const STAGING_DIR: &str = ".rdlt-staging";

/// The per-pipeline scope: a 12-hex identity hash, never the raw name.
pub(super) fn scope_of(pipeline: &str) -> String {
    rdlt_connector_sdk::spi::core::naming::ident_hash(pipeline, 12)
}

/// The state document's file name for one scope.
pub(super) fn state_file(scope: &str) -> String {
    format!("_rdlt_state.{scope}.json")
}

/// The commit log's file name for one scope.
pub(super) fn commits_file(scope: &str) -> String {
    format!("_rdlt_commits.{scope}.json")
}

/// The publish manifest's file name for one scope.
pub(super) fn manifest_file(scope: &str) -> String {
    format!("_rdlt_manifest.{scope}.json")
}

/// A staged part's tail under the staging prefix.
pub(super) fn staging_tail(scope: &str, load: &str, name: &str) -> String {
    format!("{STAGING_DIR}/{scope}/{load}/{name}")
}

/// A staged part's NAME under the load's staging directory: the table
/// as its OWN path segment, then `{slug}-{index}.{ext}` — deterministic
/// GIVEN the same part boundaries, so a crash-replay that splits its
/// rows the same way stages the same names it staged before. Since 034
/// the boundaries themselves need not reproduce (`roll_after_seconds`
/// is wall clock), so name determinism alone no longer carries
/// convergence — publish's [`Manifest`] sweep removes whatever a
/// differently-split predecessor left behind.
///
/// 030 review amendment to the gen-1 flat shape
/// `{load}-{table}-{slug}-{index}`: both table and slug may contain
/// `-`, so the flat join was AMBIGUOUS — (`events`, `us-east`) and
/// (`events-us`, `east`) collided to one staging file, the second
/// overwriting the first's bytes (the 029 colliding-path corruption
/// class, in the staging namespace). A path segment cannot contain
/// `/`, so the segment form is injective; the `{slug}-{index}` half is
/// unambiguous because the index is purely numeric and last. Staging
/// is TRANSIENT (reclaimed wholesale at open, never re-interpreted
/// across versions), so this is not a persisted-format change.
pub(super) fn staged_name(
    table: &str,
    partition: Option<&str>,
    index: usize,
    extension: &str,
) -> String {
    let slug = partition.unwrap_or("all");
    format!("{table}/{slug}-{index}.{extension}")
}

/// A published part's tail under the destination root. The index
/// counts per TABLE+PARTITION, so cross-table arrival order can never
/// change a final name.
pub(crate) fn final_tail(
    table: &str,
    partition: Option<&str>,
    load: &str,
    seq: u64,
    index: usize,
    extension: &str,
) -> String {
    match partition {
        Some(partition) => format!("{table}/{partition}/part-{load}-{seq}-{index}.{extension}"),
        None => format!("{table}/part-{load}-{seq}-{index}.{extension}"),
    }
}

/// Make a partition VALUE path-safe: ascii-alphanumeric plus `-_.`
/// survive, everything else becomes `_`; an empty result is
/// `__empty__`. NULL is rendered `__null__` by the splitter before it
/// gets here. Partition directories are BARE values, not Hive
/// `col=value`.
///
/// `.` and `..` are the two names path resolution INTERPRETS instead
/// of storing: a partition value of `..` used to walk the part OUT of
/// its table directory entirely — published where no ownership rule
/// could count or reclaim it (030 review). They map to sentinel
/// directories in the `__empty__` family instead.
pub(super) fn path_safe(value: &str) -> String {
    match value {
        "." => return "__dot__".to_owned(),
        ".." => return "__dotdot__".to_owned(),
        _ => {}
    }
    let safe: String = value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if safe.is_empty() {
        "__empty__".to_owned()
    } else {
        safe
    }
}

/// The NULL partition's directory.
pub(super) const NULL_PARTITION: &str = "__null__";

/// The persisted receipt log: the D3 planted-commit-log weld proof.
/// Receipts are retained for the LIFE of the destination — the SPI
/// commit contract is unconditional, and trimming would re-truncate
/// Replace targets on a redelivered trimmed load.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub(super) struct CommitLog {
    /// Absent in pre-versioning logs — v0, accepted.
    #[serde(default)]
    pub format_version: u32,
    #[serde(default)]
    pub receipts: Vec<(String, u64)>,
}

/// The publish MANIFEST: the final names ONE commit intends to
/// publish, written durably before any of them is, so a retry of a
/// crashed attempt knows exactly which files its predecessor may have
/// left and removes the ones it no longer writes itself.
///
/// PIPELINE-SCOPED like the state doc and the commit log — which is
/// the safety property: final file names carry no pipeline marker and
/// load ids are only unique WITHIN a pipeline, so any sweep that
/// worked by parsing directory listings could, on a load-id collision
/// between two pipelines sharing one output table, delete the other
/// pipeline's rows. The manifest never names a file this pipeline did
/// not itself intend, so the sweep cannot either.
///
/// Only the LAST commit's manifest is kept: a commit is only left
/// behind once its receipt landed (WAL replay re-drives a pending
/// commit before anything newer), and a receipted commit's manifest
/// has nothing left to sweep.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub(super) struct Manifest {
    /// Absent in none — the manifest is new at layout v1.
    #[serde(default)]
    pub format_version: u32,
    #[serde(default)]
    pub load_id: String,
    #[serde(default)]
    pub commit_seq: u64,
    /// Final tails (relative to the output root) this commit publishes.
    #[serde(default)]
    pub names: Vec<String>,
}

impl Manifest {
    /// Decode verbatim bytes; absent means "no prior attempt".
    pub(super) fn decode(
        bytes: Option<&[u8]>,
        file: &str,
    ) -> Result<Option<Self>, DestinationError> {
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let manifest: Self = serde_json::from_slice(bytes)
            .map_err(|e| DestinationError::fatal(format!("unreadable manifest `{file}`: {e}")))?;
        if manifest.format_version > LAYOUT_FORMAT_VERSION {
            return Err(DestinationError::fatal(format!(
                "manifest `{file}` format v{} is newer than this build supports                  (v{LAYOUT_FORMAT_VERSION}); upgrade rdlt instead of resetting",
                manifest.format_version
            )));
        }
        Ok(Some(manifest))
    }
}

impl CommitLog {
    /// Decode verbatim bytes; absent means empty.
    pub(super) fn decode(bytes: Option<&[u8]>, file: &str) -> Result<Self, DestinationError> {
        let Some(bytes) = bytes else {
            return Ok(Self::default());
        };
        let log: Self = serde_json::from_slice(bytes)
            .map_err(|e| DestinationError::fatal(format!("unreadable commit log `{file}`: {e}")))?;
        log.check_readable(file)?;
        Ok(log)
    }

    /// A STRICTLY newer version refuses as an upgrade prompt, never a
    /// reset — resetting forgets receipts and republishes.
    pub(super) fn check_readable(&self, file: &str) -> Result<(), DestinationError> {
        if self.format_version > LAYOUT_FORMAT_VERSION {
            return Err(DestinationError::fatal(format!(
                "commit log `{file}` format v{} is newer than this build supports \
                 (v{LAYOUT_FORMAT_VERSION}); upgrade rdlt instead of resetting",
                self.format_version
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The frozen name shapes, literally.
    #[test]
    fn every_name_shape_is_the_frozen_one() {
        assert_eq!(state_file("abc123def456"), "_rdlt_state.abc123def456.json");
        assert_eq!(commits_file("abc"), "_rdlt_commits.abc.json");
        assert_eq!(
            staging_tail("abc", "load-1", "t/all-0.parquet"),
            ".rdlt-staging/abc/load-1/t/all-0.parquet"
        );
        assert_eq!(staged_name("t", None, 0, "parquet"), "t/all-0.parquet");
        assert_eq!(staged_name("t", Some("us"), 2, "jsonl"), "t/us-2.jsonl");
        // The 030 injectivity amendment: dashed table and slug pairs
        // that collided under the gen-1 flat join stay distinct.
        assert_ne!(
            staged_name("events", Some("us-east"), 0, "parquet"),
            staged_name("events-us", Some("east"), 0, "parquet"),
        );
        assert_eq!(
            final_tail("t", None, "l", 3, 0, "parquet"),
            "t/part-l-3-0.parquet"
        );
        assert_eq!(
            final_tail("t", Some("us"), "l", 3, 1, "jsonl"),
            "t/us/part-l-3-1.jsonl"
        );
        assert_eq!(scope_of("p").len(), 12, "12-hex scope");
    }

    /// The path-safety rule: survivors, replacement, and the empty
    /// sentinel.
    #[test]
    fn partition_values_become_path_safe() {
        assert_eq!(path_safe("us-east.1_x"), "us-east.1_x");
        assert_eq!(path_safe("a b/c"), "a_b_c");
        assert_eq!(path_safe("日本"), "__", "non-ascii replaced per char");
        assert_eq!(path_safe(""), "__empty__");
        // Path resolution must never INTERPRET a partition directory:
        // `..` used to walk the part out of its table dir (030 review).
        assert_eq!(path_safe("."), "__dot__");
        assert_eq!(path_safe(".."), "__dotdot__");
        assert_eq!(path_safe("..."), "...", "only the two special names map");
    }

    /// The commit log: v0 accepted, the current version round-trips,
    /// and a strictly newer version refuses with the upgrade prompt.
    #[test]
    fn the_commit_log_versioning_is_upgrade_not_reset() {
        let v0 = CommitLog::decode(Some(br#"{"receipts": [["load-x", 1]]}"#), "f").expect("v0");
        assert_eq!(v0.format_version, 0);
        assert_eq!(v0.receipts, vec![("load-x".to_owned(), 1)]);

        let current = CommitLog {
            format_version: LAYOUT_FORMAT_VERSION,
            receipts: vec![("l".into(), 2)],
        };
        let bytes = serde_json::to_vec(&current).expect("encodes");
        assert_eq!(
            CommitLog::decode(Some(&bytes), "f").expect("round-trips"),
            current
        );

        let future = br#"{"format_version": 2, "receipts": []}"#;
        let err = CommitLog::decode(Some(future), "log.json").expect_err("refuses");
        assert!(
            format!("{err}").contains(
                "commit log `log.json` format v2 is newer than this build supports (v1); \
                 upgrade rdlt instead of resetting"
            ),
            "{err}"
        );

        let err = CommitLog::decode(Some(b"not json"), "log.json").expect_err("unreadable");
        assert!(
            format!("{err}").contains("unreadable commit log `log.json`"),
            "{err}"
        );
    }
}
