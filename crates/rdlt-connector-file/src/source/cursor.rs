//! Per-file progress cursor: `path → {done, size, eol, mtime}`.
//!
//! Complete ⇔ `done_units == size_units`. Resume rules: grown file → read the tail
//! from `done_units` (only if the consumed range ended at a record boundary); shrunk
//! file, `done_units > current size`, or a same-size file whose mtime moved → fatal,
//! naming the file — never read from a stale offset.
//!
//! The value types themselves (`FileMeta`, `FileTask`, `FileProgress`) live under
//! `location/` so the shared lower layer never imports upward from `source/`; this
//! module owns the PLANNING logic over them.

use std::collections::BTreeMap;

use rdlt_connector::{Cursor, SourceError};
use serde::{Deserialize, Serialize};

pub use crate::location::types::{FileMeta, FileProgress, FileTask, ResumeCheck};

pub(crate) const CURSOR_FORMAT_VERSION: u32 = 1;

/// Tail-verification window: the cursor records a blake3 hash of the last
/// `min(done_units, TAIL_WINDOW)` consumed bytes; a resumed read re-fetches exactly
/// that window and compares BEFORE trusting the offset — a grown REWRITE (changed
/// prefix) fails loudly on both location kinds, while a genuine append (identical
/// prefix) resumes.
pub(crate) const TAIL_WINDOW: u64 = 4096;

/// Per-file progress for one stream.
///
/// **Entries are retained for the life of the pipeline state — deliberately.**
/// Every path this stream has ever consumed stays in the document, which is
/// serialized into every checkpoint and every committed state document. For a
/// glob over rotating files (daily drops, a log shipper) that grows without
/// bound: roughly 150–250 bytes per path, so ~10k files is a few megabytes
/// rewritten on each commit.
///
/// Pruning is NOT free, which is why it is not done. A pruned entry whose file
/// later reappears — a re-uploaded day, a restored archive, a producer that
/// re-creates a name — reads from zero and DUPLICATES its rows under Append.
/// Trading unbounded metadata for silent duplication is the wrong trade to make
/// on the user's behalf.
///
/// The operator's levers are to narrow the pattern so old paths stop matching,
/// or to clear the pipeline state and accept a full re-read.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FileCursor {
    #[serde(default = "default_version")]
    pub format_version: u32,
    pub files: BTreeMap<String, FileProgress>,
}

fn default_version() -> u32 {
    CURSOR_FORMAT_VERSION
}

impl FileProgress {
    /// The same-size rewrite tripwire, shared by both planners: an object whose
    /// etag changed, or a file whose mtime moved, was rewritten in place even
    /// though its size did not — trusting the recorded offset would read stale
    /// or mismatched content, so this is a typed Fatal naming the file. `None`
    /// means the recorded progress is safe to trust.
    fn rewritten_in_place(&self, meta: &FileMeta) -> Option<SourceError> {
        if meta.size_units != self.size_units {
            return None;
        }
        if let (Some(then), Some(now)) = (self.etag.as_deref(), meta.etag.as_deref())
            && then != now
        {
            return Some(SourceError::fatal(format!(
                "file `{}` was rewritten in place (same size, different etag); \
                 refusing to trust recorded progress — clear it from the \
                 pipeline state or restore the object",
                meta.path
            )));
        }
        if let (Some(then), Some(now)) = (self.mtime_ms, meta.mtime_ms)
            && then != now
        {
            return Some(SourceError::fatal(format!(
                "file `{}` was rewritten in place (same size, but modified \
                 since the last run); refusing to trust recorded progress — \
                 clear it from the pipeline state or restore the file",
                meta.path
            )));
        }
        None
    }
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
            let Some(progress) = self.files.get(&meta.path) else {
                tasks.push(FileTask::fresh(meta));
                continue;
            };
            if meta.size_units < progress.size_units || progress.done_units > meta.size_units {
                return Err(SourceError::fatal(format!(
                    "file `{}` shrank or was rewritten (recorded {} of {} \
                     bytes, now {}); refusing to read from a stale \
                     offset — clear it from the pipeline state or restore the file",
                    meta.path, progress.done_units, progress.size_units, meta.size_units
                )));
            }
            if let Some(err) = progress.rewritten_in_place(meta) {
                return Err(err);
            }
            if progress.done_units < meta.size_units {
                if !progress.ended_at_record_boundary {
                    return Err(SourceError::fatal(format!(
                        "file `{}` grew after a run that consumed an unterminated \
                         final line; the recorded offset {} points mid-record — \
                         clear it from the pipeline state or restore the file",
                        meta.path, progress.done_units
                    )));
                }
                // Armed only for a genuine resume. `done_units == 0` carries
                // no consumed prefix to describe, and a check built from one
                // would compute a window (or a last-group index) below zero.
                // jsonl has always filtered this way; parquet now shares it.
                let resume_check = (progress.done_units > 0)
                    .then(|| Self::resume_check_for(progress))
                    .flatten();
                tasks.push(FileTask::from_meta(meta, progress.done_units, resume_check));
            }
            // done == size (+ same mtime): complete and unchanged → skip.
        }
        Ok(tasks)
    }

    /// Plan for WHOLE-FILE formats (csv, compressed files): no tail
    /// resume — complete+unchanged skips, a size change is a typed error
    /// (these formats never grow in place; new data arrives as new
    /// files), an incomplete file re-reads whole (crash re-delivery,
    /// exactly-once under keyed merge/dedup — documented).
    pub fn plan_whole(&self, matched: &[FileMeta]) -> Result<Vec<FileTask>, SourceError> {
        let mut tasks = Vec::new();
        for meta in matched {
            let Some(progress) = self.files.get(&meta.path) else {
                tasks.push(FileTask::fresh(meta));
                continue;
            };
            if meta.size_units != progress.size_units {
                return Err(SourceError::fatal(format!(
                    "file `{}` changed size ({} → {}) — whole-file formats \
                     (csv, compressed) never grow in place; deliver new data \
                     as a new file, or clear this file from the pipeline state",
                    meta.path, progress.size_units, meta.size_units
                )));
            }
            if let Some(err) = progress.rewritten_in_place(meta) {
                return Err(err);
            }
            if progress.done_units < progress.size_units {
                // Crash mid-file: re-read whole (re-delivery documented).
                tasks.push(FileTask::fresh(meta));
            }
            // done == size: complete → skip.
        }
        Ok(tasks)
    }

    /// The expectation a resumed read must satisfy, derived from what the
    /// recorded progress actually holds. The two hashes are mutually exclusive
    /// in practice — a stream has one cursor unit — so the record decides,
    /// never the caller.
    fn resume_check_for(progress: &FileProgress) -> Option<ResumeCheck> {
        match (&progress.tail_hash, &progress.row_groups_hash) {
            // A record stream records one, a row-group stream the other. Holding
            // BOTH means the entry was written by two different readers, so
            // neither value describes what this run will read: preferring either
            // would silently verify the wrong thing.
            (Some(_), Some(_)) => None,
            (Some(hash), None) => Some(ResumeCheck::TailBytes {
                window: progress.done_units.min(TAIL_WINDOW),
                hash: hash.clone(),
            }),
            (None, Some(hash)) => Some(ResumeCheck::RowGroupPrefix {
                groups: progress.done_units,
                hash: hash.clone(),
            }),
            (None, None) => None,
        }
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
            size_units: size,
            mtime_ms: None,
            etag: None,
        }
    }

    /// The retention rule, asserted so an accidental prune fails loudly instead
    /// of silently duplicating rows: every path ever recorded is still present
    /// after later rounds record disjoint sets.
    #[test]
    fn every_path_ever_recorded_is_retained() {
        let mut cursor = FileCursor::default();
        for round in 0..3 {
            for i in 0..4 {
                cursor.record(&format!("day-{round}/part-{i}.jsonl"), done(10, 10));
            }
        }
        assert_eq!(
            cursor.files.len(),
            12,
            "entries are retained for the life of the state — pruning one whose file \
             reappears would re-read it from zero and duplicate under Append"
        );
    }

    fn done(done: u64, size: u64) -> FileProgress {
        FileProgress {
            done_units: done,
            size_units: size,
            ended_at_record_boundary: true,
            mtime_ms: None,
            etag: None,
            tail_hash: None,
            row_groups_hash: None,
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
                etag: None,
                size_units: 15,
                read_path: None,
                resume_check: None,
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
                done_units: 10,
                size_units: 10,
                ended_at_record_boundary: true,
                mtime_ms: Some(1_000),
                etag: None,
                tail_hash: None,
                row_groups_hash: None,
            },
        );
        // Same size, same mtime: skip.
        let plan = cursor
            .plan(&[FileMeta {
                path: "a".into(),
                size_units: 10,
                mtime_ms: Some(1_000),
                etag: None,
            }])
            .expect("unchanged");
        assert!(plan.is_empty());
        // Same size, moved mtime: rewritten in place → loud failure.
        let err = cursor
            .plan(&[FileMeta {
                path: "a".into(),
                size_units: 10,
                mtime_ms: Some(2_000),
                etag: None,
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
                done_units: 10,
                size_units: 10,
                ended_at_record_boundary: false, // previous run swallowed an unterminated final line
                mtime_ms: None,
                etag: None,
                tail_hash: None,
                row_groups_hash: None,
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
        // (serde defaults: eol=true, mtime=None). The wire keys stay `done`/`size`/
        // `eol` regardless of the Rust field renames.
        let old = serde_json::json!({
            "format_version": 1,
            "files": {"a": {"done": 5, "size": 5}}
        });
        let decoded = FileCursor::decode(Some(&Cursor::new(old))).expect("decode");
        let progress = decoded.files.get("a").expect("entry");
        assert!(progress.ended_at_record_boundary);
        assert_eq!(progress.mtime_ms, None);
    }
}
