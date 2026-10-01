//! Limits the reference connectors enforce on what they read back from disk and keep there, and
//! the defaults of those a configuration sets.

use std::time::Duration;

/// Bytes: bounds one file the files source reads, unless its configuration sets another.
pub(crate) const FILE_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// Bytes: bounds one line of a JSON lines file, its line ending apart: the lines the files
/// source reads, unless its configuration sets another, and those the files destination writes
/// and reads back.
pub(crate) const LINE_BYTES: u64 = 32 * 1024 * 1024;

/// Bytes: bounds one manifest of the files destination, written or read.
pub(crate) const MANIFEST_BYTES: u64 = 128 * 1024 * 1024;

/// Bytes: bounds one catalog version of a table of the files destination, written or read.
pub(crate) const CATALOG_BYTES: u64 = 16 * 1024 * 1024;

/// Bytes: bounds the file naming the pipeline that owns a table of the files destination.
pub(crate) const OWNER_BYTES: u64 = 128;

/// Bytes: bounds the name of a table of the files destination, an identifier of lower-case ASCII
/// letters, digits and underscores.
pub(crate) const TABLE_NAME_BYTES: u16 = 128;

/// Versions: how many manifests of a pipeline, and catalog versions of a table, stay on disk
/// besides the latest, so a reader that listed a version still finds it when it reads it; the
/// files an older manifest lists may be gone.
pub(crate) const KEPT_VERSIONS: u64 = 8;

/// Loads: bounds the loads whose receipts a manifest keeps, the most recent ones; of each it keeps
/// every receipt.
pub(crate) const RECEIPT_LOADS: usize = 16;

/// Attempts: bounds how often an open or a schema change is worked out again when another
/// session's lands first.
pub(crate) const PUBLISH_ATTEMPTS: u32 = 64;

/// How long a schema change, a writer or a release waits for a table's lock, unless the
/// destination's configuration sets another wait.
pub(crate) const LOCK_WAIT: Duration = Duration::from_secs(30);

/// How long ago a temporary file was last written for an open to remove it as one its writer
/// left behind.
pub(crate) const TEMPORARY_AGE: Duration = Duration::from_hours(1);

/// Bytes: the size one batch of a written Arrow file aims for; a table's rows are written as
/// batches of about this size, each within the frame limit a reader accepts.
pub(crate) const CHUNK_BYTES: u64 = 8 * 1024 * 1024;

/// Rows: bounds one batch read back from a JSON lines file of the files destination.
pub(crate) const READ_BATCH_ROWS: usize = 1024;

/// Bytes: the most a merge of an append table's files reads into one file; files that would
/// together be larger are merged with no other.
pub(crate) const COMPACT_BYTES: u64 = 64 * 1024 * 1024;

/// Directories: how deep beneath the directory it starts at a tree is entered to remove it or
/// to discard what it holds; a tree deeper than any the connectors make is refused.
pub(crate) const TREE_DEPTH: usize = 32;

/// Bytes: bounds the file a keeper of positions is kept in.
pub(crate) const KEEPER_BYTES: u64 = 4 * 1024 * 1024;

/// Positions: bounds the partitions one keeper holds a position for.
pub(crate) const KEEPER_POSITIONS: usize = 4096;
