//! Write-ahead log: Arrow IPC file segments + append-only JSONL manifest.
//!
//! The WAL is a *replayable buffer*, never the source of truth. Commit ordering:
//! (1) fsync pending segments + manifest, (2) destination commit, (3) append
//! `Committed` + GC segment files. A crash between (1) and (3) leaves an
//! uncommitted-looking span whose replay re-commits with the SAME
//! `(load_id, commit_seq)` — idempotence absorbs the ambiguity.

mod record;
pub(crate) mod resume;
mod writer;

pub(crate) use record::{WAL_FORMAT_VERSION, WalRecord};
pub(crate) use writer::{RULES_SIDECAR, Wal, clear, create_private_dir, dir_in};

#[cfg(test)]
pub(crate) use writer::write_segment;
