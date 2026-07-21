//! Per-file progress cursor (data-model §1): `path → {done, size, eol, mtime}`.
//!
//! Complete ⇔ `done == size`. Resume rules: grown file → read the tail from `done`
//! (only if the consumed range ended at a record boundary); shrunk file, `done >
//! current size`, or a same-size file whose mtime moved → fatal, naming the file —
//! never read from a stale offset (spec FR-003).

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
    /// Whether the consumed range ended at a record boundary (a newline for jsonl;
    /// always true for parquet's row-group unit). A range that swallowed an
    /// unterminated final line must not be resumed past if the file later grows —
    /// `done` would point mid-record.
    #[serde(default = "default_true")]
    pub eol: bool,
    /// File mtime (ms since epoch) observed when this progress was recorded. A
    /// same-size file whose mtime moved was rewritten in place — size alone cannot
    /// see that, so this is the loud-failure tripwire for it.
    #[serde(default)]
    pub mtime_ms: Option<u64>,
}

fn default_true() -> bool {
    true
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

/// One matched file as observed on disk this run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMeta {
    pub path: String,
    /// Bytes (jsonl) / row groups (parquet).
    pub size: u64,
    pub mtime_ms: Option<u64>,
}

/// What to do with one matched file this run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileTask {
    pub path: String,
    /// Where to start (bytes / row groups); equals prior `done` (0 for new files).
    pub start: u64,
    /// The mtime observed at planning time, recorded with this file's progress.
    pub mtime_ms: Option<u64>,
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

    /// Plan this run against the matched files (already sorted). Skips
    /// complete-and-unchanged files; fails on shrunk/rewritten ones.
    pub fn plan(&self, matched: &[FileMeta]) -> Result<Vec<FileTask>, SourceError> {
        let mut tasks = Vec::new();
        for meta in matched {
            match self.files.get(&meta.path) {
                None => tasks.push(FileTask {
                    path: meta.path.clone(),
                    start: 0,
                    mtime_ms: meta.mtime_ms,
                }),
                Some(progress) => {
                    if meta.size < progress.size || progress.done > meta.size {
                        return Err(SourceError::fatal(format!(
                            "file `{}` shrank or was rewritten (recorded {} of {} \
                             bytes, now {}); refusing to read from a stale \
                             offset — clear it from the pipeline state or restore the file",
                            meta.path, progress.done, progress.size, meta.size
                        )));
                    }
                    if meta.size == progress.size
                        && let (Some(then), Some(now)) = (progress.mtime_ms, meta.mtime_ms)
                        && then != now
                    {
                        return Err(SourceError::fatal(format!(
                            "file `{}` was rewritten in place (same size, but modified \
                             since the last run); refusing to trust recorded progress — \
                             clear it from the pipeline state or restore the file",
                            meta.path
                        )));
                    }
                    if progress.done < meta.size {
                        if !progress.eol {
                            return Err(SourceError::fatal(format!(
                                "file `{}` grew after a run that consumed an unterminated \
                                 final line; the recorded offset {} points mid-record — \
                                 clear it from the pipeline state or restore the file",
                                meta.path, progress.done
                            )));
                        }
                        tasks.push(FileTask {
                            path: meta.path.clone(),
                            start: progress.done,
                            mtime_ms: meta.mtime_ms,
                        });
                    }
                    // done == size (+ same mtime): complete and unchanged → skip.
                }
            }
        }
        Ok(tasks)
    }

    pub fn record(&mut self, path: &str, progress: FileProgress) {
        self.files.insert(path.to_owned(), progress);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(path: &str, size: u64) -> FileMeta {
        FileMeta {
            path: path.into(),
            size,
            mtime_ms: None,
        }
    }

    fn done(done: u64, size: u64) -> FileProgress {
        FileProgress {
            done,
            size,
            eol: true,
            mtime_ms: None,
        }
    }

    #[test]
    fn plan_skips_complete_reads_tails_and_rejects_shrunk() {
        let mut cursor = FileCursor::default();
        cursor.record("a", done(10, 10)); // complete
        cursor.record("b", done(10, 10)); // will have grown
        cursor.record("c", done(10, 10)); // will have shrunk

        let plan = cursor.plan(&[meta("a", 10), meta("b", 15)]).expect("plan");
        assert_eq!(
            plan,
            vec![FileTask {
                path: "b".into(),
                start: 10,
                mtime_ms: None,
            }]
        );

        let err = cursor.plan(&[meta("c", 5)]).expect_err("shrunk");
        assert!(err.to_string().contains('c'));
    }

    #[test]
    fn same_size_mtime_change_is_a_rewrite_error() {
        let mut cursor = FileCursor::default();
        cursor.record(
            "a",
            FileProgress {
                done: 10,
                size: 10,
                eol: true,
                mtime_ms: Some(1_000),
            },
        );
        // Same size, same mtime: skip.
        let plan = cursor
            .plan(&[FileMeta {
                path: "a".into(),
                size: 10,
                mtime_ms: Some(1_000),
            }])
            .expect("unchanged");
        assert!(plan.is_empty());
        // Same size, moved mtime: rewritten in place → loud failure.
        let err = cursor
            .plan(&[FileMeta {
                path: "a".into(),
                size: 10,
                mtime_ms: Some(2_000),
            }])
            .expect_err("rewritten");
        assert!(err.to_string().contains("rewritten in place"));
    }

    #[test]
    fn growth_after_unterminated_tail_is_an_error() {
        let mut cursor = FileCursor::default();
        cursor.record(
            "a",
            FileProgress {
                done: 10,
                size: 10,
                eol: false, // previous run swallowed an unterminated final line
                mtime_ms: None,
            },
        );
        let err = cursor.plan(&[meta("a", 20)]).expect_err("mid-record");
        assert!(err.to_string().contains("unterminated"));
        // Unchanged file stays skippable — the tripwire only arms on growth.
        assert!(cursor.plan(&[meta("a", 10)]).expect("plan").is_empty());
    }

    #[test]
    fn round_trips_through_cursor() {
        let mut cursor = FileCursor::default();
        cursor.record("x", done(3, 9));
        let decoded = FileCursor::decode(Some(&cursor.encode())).expect("decode");
        assert_eq!(decoded, cursor);
    }

    #[test]
    fn decodes_pre_tripwire_cursors_with_defaults() {
        // Cursors written before the eol/mtime fields existed must still decode
        // (serde defaults: eol=true, mtime=None).
        let old = serde_json::json!({
            "format_version": 1,
            "files": {"a": {"done": 5, "size": 5}}
        });
        let decoded = FileCursor::decode(Some(&Cursor::new(old))).expect("decode");
        let progress = decoded.files.get("a").expect("entry");
        assert!(progress.eol);
        assert_eq!(progress.mtime_ms, None);
    }
}
