//! Write-ahead log: Arrow IPC file segments + append-only JSONL manifest.
//!
//! The WAL is a *replayable buffer*, never the source of truth. Commit ordering:
//! (1) fsync pending segments + manifest, (2) destination commit, (3) append
//! `Committed` + GC segment files. A crash between (1) and (3) leaves an
//! uncommitted-looking span whose replay re-commits with the SAME
//! `(load_id, commit_seq)` — idempotence absorbs the ambiguity.
//!
//! [`mod@format`] is the frozen on-disk vocabulary; [`dir`] the directory's
//! ownership and file-type boundary; [`writer`] the live run's `Wal`;
//! [`segment`] the segment container's writer and gated reader; [`scan`] the
//! forward manifest scan into an outcome; [`replay`] the two-pass replay of an
//! uncommitted span.

pub(crate) mod dir;
pub(crate) mod format;
pub(crate) mod replay;
pub(crate) mod scan;
pub(crate) mod segment;
pub(crate) mod writer;
