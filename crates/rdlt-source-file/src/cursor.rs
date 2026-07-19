//! Per-file progress cursor (data-model §1): `path → {done, size}`.
//!
//! Complete ⇔ `done == size`. Resume rules: grown file → read the tail from `done`;
//! shrunk file (or `done > current size`) → fatal, naming the file — never read from
//! a stale offset (spec FR-003).

use std::collections::BTreeMap;

use rdlt_connector::{Cursor, SourceError};
use serde::{Deserialize, Serialize};

pub const CURSOR_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileProgress {
    /// Bytes consumed (jsonl) / row groups consumed (parquet).
    pub done: u64,
    /// Total size (bytes / row groups) when last read.
    pub size: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FileCursor {
    #[serde(default = "default_version")]
    pub format_version: u32,
    pub files: BTreeMap<String, FileProgress>,
}

fn default_version() -> u32 {
    CURSOR_FORMAT_VERSION
}

/// What to do with one matched file this run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileTask {
    pub path: String,
    /// Where to start (bytes / row groups); equals prior `done` (0 for new files).
    pub start: u64,
}

impl FileCursor {
    pub fn decode(cursor: Option<&Cursor>) -> Result<Self, SourceError> {
        match cursor {
            None => Ok(Self {
                format_version: CURSOR_FORMAT_VERSION,
                files: BTreeMap::new(),
            }),
            Some(cursor) => serde_json::from_value(cursor.as_value().clone())
                .map_err(|e| SourceError::fatal(format!("unreadable file cursor: {e}"))),
        }
    }

    pub fn encode(&self) -> Cursor {
        Cursor::new(serde_json::to_value(self).expect("cursor serialization"))
    }

    /// Plan this run against the matched files (already sorted) and their current
    /// sizes. Skips complete-and-unchanged files; fails on shrunk/rewritten ones.
    pub fn plan(&self, matched: &[(String, u64)]) -> Result<Vec<FileTask>, SourceError> {
        let mut tasks = Vec::new();
        for (path, current_size) in matched {
            match self.files.get(path) {
                None => tasks.push(FileTask {
                    path: path.clone(),
                    start: 0,
                }),
                Some(progress) => {
                    if *current_size < progress.size || progress.done > *current_size {
                        return Err(SourceError::fatal(format!(
                            "file `{path}` shrank or was rewritten (recorded {} of {} \
                             bytes, now {current_size}); refusing to read from a stale \
                             offset — clear it from the pipeline state or restore the file",
                            progress.done, progress.size
                        )));
                    }
                    if progress.done < *current_size {
                        tasks.push(FileTask {
                            path: path.clone(),
                            start: progress.done,
                        });
                    }
                    // done == current_size: complete and unchanged → skip entirely.
                }
            }
        }
        Ok(tasks)
    }

    pub fn record(&mut self, path: &str, done: u64, size: u64) {
        self.files
            .insert(path.to_owned(), FileProgress { done, size });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_skips_complete_reads_tails_and_rejects_shrunk() {
        let mut cursor = FileCursor::default();
        cursor.record("a", 10, 10); // complete
        cursor.record("b", 10, 10); // will have grown
        cursor.record("c", 10, 10); // will have shrunk

        let plan = cursor
            .plan(&[("a".into(), 10), ("b".into(), 15)])
            .expect("plan");
        assert_eq!(
            plan,
            vec![FileTask {
                path: "b".into(),
                start: 10
            }]
        );

        let err = cursor.plan(&[("c".into(), 5)]).expect_err("shrunk");
        assert!(err.to_string().contains('c'));
    }

    #[test]
    fn round_trips_through_cursor() {
        let mut cursor = FileCursor::default();
        cursor.record("x", 3, 9);
        let decoded = FileCursor::decode(Some(&cursor.encode())).expect("decode");
        assert_eq!(decoded, cursor);
    }
}
