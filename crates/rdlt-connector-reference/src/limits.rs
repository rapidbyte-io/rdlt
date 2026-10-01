//! Limits the reference connectors enforce on what they read back from disk and keep there, and
//! the defaults of those a configuration sets.

/// Bytes: bounds the file a keeper of positions is kept in.
pub(crate) const KEEPER_BYTES: u64 = 4 * 1024 * 1024;

/// Positions: bounds the partitions one keeper holds a position for.
pub(crate) const KEEPER_POSITIONS: usize = 4096;
