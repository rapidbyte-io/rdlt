//! [`JsonlDirProbe`] — the file destination's visibility contract as a
//! [`TableProbe`]: the table is a path segment under the output root,
//! and ONLY published data counts (the 030 rule) — dot-prefixed entries
//! (the `.rdlt-staging` tree) and `_rdlt_*` bookkeeping files are what
//! a reader never sees, so they are excluded from the count.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use rdlt_connector::core::TableName;
use rdlt_testkit::conformance::destination::TableProbe;

/// Counts reader-visible rows in a jsonl file destination rooted at
/// `root`: parsed jsonl rows in `<root>/<table>/**`, staging and
/// bookkeeping excluded.
pub(crate) struct JsonlDirProbe {
    pub(crate) root: PathBuf,
}

#[async_trait]
impl TableProbe for JsonlDirProbe {
    async fn count(&self, table: &TableName) -> u64 {
        visible_rows(&self.root.join(table.as_str()))
    }
}

/// Recursive count over one directory. A missing directory is zero rows
/// — the table simply has no published data yet.
fn visible_rows(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut rows = 0;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        // The invisibility rule: dot-prefixed names are staging
        // (`.rdlt-staging`), `_rdlt_*` names are bookkeeping (state,
        // commits, manifest, lease) — neither is published data.
        if name.starts_with('.') || name.starts_with("_rdlt_") {
            continue;
        }
        let path = entry.path();
        if path.is_dir() {
            rows += visible_rows(&path);
        } else if name.ends_with(".jsonl") {
            rows += parsed_rows(&path);
        }
    }
    rows
}

/// Rows in one jsonl file: lines that parse as JSON documents — the
/// probe counts what a jsonl READER would yield, not raw line count.
fn parsed_rows(path: &Path) -> u64 {
    let Ok(text) = std::fs::read_to_string(path) else {
        return 0;
    };
    text.lines()
        .filter(|line| serde_json::from_str::<serde_json::Value>(line).is_ok())
        .count() as u64
}
