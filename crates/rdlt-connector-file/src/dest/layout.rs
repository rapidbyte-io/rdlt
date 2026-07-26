//! Persisted-format identity for the file destination: the file-name vocabulary
//! (state/commit-log/staged/final part names, the pipeline scope key, path-safe
//! partition values) and the commit-log document. These spellings are the WR1
//! frozen contract — a product-wide rename would be a one-line decision here,
//! never a config option. This family deliberately does NOT share the SQL naming
//! vocabulary; it owns its own file-name spellings.

use rdlt_connector::DestinationError;
use rdlt_connector::core::{LoadId, PipelineId, TableName, naming::ident_hash};
use serde::{Deserialize, Serialize};

use crate::location::STAGING_DIR;

pub(crate) const LAYOUT_FORMAT_VERSION: u32 = 1;

const STATE_FILE_PREFIX: &str = "_rdlt_state";
const COMMITS_FILE_PREFIX: &str = "_rdlt_commits";

/// Short stable scope key for one pipeline's files inside a shared output dir.
pub(crate) fn pipeline_scope(pipeline: &PipelineId) -> String {
    ident_hash(pipeline.as_str(), 12)
}

pub(crate) fn state_file(scope: &str) -> String {
    format!("{STATE_FILE_PREFIX}.{scope}.json")
}

pub(crate) fn commits_file(scope: &str) -> String {
    format!("{COMMITS_FILE_PREFIX}.{scope}.json")
}

/// The staged (pre-publish) tail for one part: scoped by pipeline and load so
/// sibling pipelines and dead sessions cannot clobber each other.
pub(crate) fn staging_tail(scope: &str, load: &LoadId, name: &str) -> String {
    format!("{STAGING_DIR}/{scope}/{load}/{name}")
}

/// The staged file NAME (deterministic per table+partition+per-part index, so
/// crash-recovery replay reproduces it identically). The final name is assigned
/// at commit (it needs the commit_seq).
pub(crate) fn staged_part_name(
    load: &LoadId,
    table: &TableName,
    partition: Option<&str>,
    index: u64,
    extension: &str,
) -> String {
    let slug = partition.unwrap_or("all");
    format!("{load}-{table}-{slug}-{index}.{extension}")
}

/// Final data-file tail path for a part.
pub(crate) fn final_tail(
    table: &TableName,
    partition: Option<&str>,
    load: &LoadId,
    seq: u64,
    index: u64,
    extension: &str,
) -> String {
    match partition {
        Some(value) => format!("{table}/{value}/part-{load}-{seq}-{index}.{extension}"),
        None => format!("{table}/part-{load}-{seq}-{index}.{extension}"),
    }
}

/// Render a partition value path-safe (never a separator or hidden name).
pub(crate) fn path_safe(value: &str) -> String {
    let cleaned: String = value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if cleaned.is_empty() {
        "__empty__".to_owned()
    } else {
        cleaned
    }
}

/// The receipt log: `(load_id, commit_seq)` pairs proving what has committed.
/// The DURABLE idempotency guard — replay dedup and the Replace once-per-load
/// truncation guard both read it.
///
/// **Receipts are retained for the life of the destination — deliberately.**
/// The log grows by one small tuple per commit and is rewritten whole on each
/// one, which is a real cost on a long-lived output.
///
/// It is paid because the SPI's commit contract is UNCONDITIONAL: re-committing
/// the same `(load_id, commit_seq)` returns the prior receipt without
/// re-publishing, with no clause about how recently that load ran. Trimming even
/// a bounded tail makes the guarantee conditional on recency, and a redelivery
/// of a trimmed load then re-truncates its Replace targets — destroying data a
/// later load published — and re-publishes under Append. Redelivery of an older
/// load is reachable through WAL replay from a restored workdir, through two
/// engines sharing an output prefix, and through any embedder driving
/// `LoadSession` directly, which is what the SPI is for.
///
/// Bounding this safely needs a persisted watermark and a TYPED refusal for a
/// commit that falls below it — a design, not a trim.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct CommitLog {
    #[serde(default)]
    pub(crate) format_version: u32,
    #[serde(default)]
    pub(crate) receipts: Vec<(String, u64)>,
}

impl CommitLog {
    /// A commit log written by a NEWER layout than this build understands is a
    /// typed failure, never a silent reset (a reset would re-truncate and
    /// re-deliver, duplicating under Append) — the same future-version rule the
    /// engine's WAL manifest and state doc enforce. Version 0 (absent field, a
    /// pre-versioning log) is accepted.
    pub(crate) fn check_readable(&self, file: &str) -> Result<(), DestinationError> {
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
    // Mutation-report closure: the future-version guard is `>`, strictly —
    // the current version and a pre-versioning (absent = 0) log both read.
    use super::*;

    #[test]
    fn future_commit_log_version_is_a_typed_error_current_is_fine() {
        let mut log = CommitLog::default();
        assert!(log.check_readable("_rdlt_commits.abc.json").is_ok());
        log.format_version = LAYOUT_FORMAT_VERSION;
        assert!(log.check_readable("_rdlt_commits.abc.json").is_ok());
        log.format_version = LAYOUT_FORMAT_VERSION + 1;
        let err = log
            .check_readable("_rdlt_commits.abc.json")
            .expect_err("future version must be rejected");
        assert!(err.to_string().contains("_rdlt_commits.abc.json"));
    }
}
