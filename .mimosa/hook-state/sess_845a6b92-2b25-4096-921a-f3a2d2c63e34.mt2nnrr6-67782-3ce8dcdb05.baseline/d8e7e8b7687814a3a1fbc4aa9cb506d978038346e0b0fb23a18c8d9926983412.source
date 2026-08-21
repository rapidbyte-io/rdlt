//! [`JsonlDirProbe`] — the reference destination's visibility contract
//! as a [`TableProbe`]: published parts are flat
//! `<table>-<load_id>-<part>.jsonl` files in the ONE output directory,
//! and ONLY published data counts — underscore-prefixed names (the
//! `_reference_*` bookkeeping documents and `_staged-*` write
//! temporaries) are what a reader never sees, so they never match a
//! table's part prefix and stay out of the count.

use std::path::PathBuf;

use async_trait::async_trait;
use rdlt_connector::core::id::TableName;
use rdlt_testkit::conformance::destination::{ProbeError, TableProbe};

/// Counts reader-visible rows for `table` in a reference-destination
/// output directory rooted at `root`: parsed jsonl rows across every
/// published `<table>-…jsonl` part. A missing directory is zero rows —
/// the destination creates it at the first connect, so absence means
/// nothing was ever published.
pub(crate) struct JsonlDirProbe {
    pub(crate) root: PathBuf,
}

#[async_trait]
impl TableProbe for JsonlDirProbe {
    async fn count(&self, table: &TableName) -> Result<u64, ProbeError> {
        let Ok(entries) = std::fs::read_dir(&self.root) else {
            return Ok(0);
        };
        let prefix = format!("{table}-");
        let mut rows = 0u64;
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with(&prefix) && name.ends_with(".jsonl") {
                rows += parsed_rows(&entry.path());
            }
        }
        Ok(rows)
    }
}

/// Rows in one jsonl file: lines that parse as JSON documents — the
/// probe counts what a jsonl READER would yield, not raw line count.
fn parsed_rows(path: &std::path::Path) -> u64 {
    let Ok(text) = std::fs::read_to_string(path) else {
        return 0;
    };
    text.lines()
        .filter(|line| serde_json::from_str::<serde_json::Value>(line).is_ok())
        .count() as u64
}
