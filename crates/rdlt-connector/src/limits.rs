//! Limits on what a connector may send or receive; each is checked where the data enters rdlt.

#[cfg(test)]
mod tests;

/// Bytes: bounds one JSON push.
pub const MAX_JSON_PUSH_BYTES: u64 = 64 * 1024 * 1024;

/// Rows: bounds one Arrow or change batch.
pub const MAX_BATCH_ROWS: u64 = 1024 * 1024;

/// Columns: bounds the width of one batch.
pub const MAX_COLUMNS: u64 = 10_000;

/// Bytes: bounds one encoded cursor.
pub const MAX_CURSOR_BYTES: u64 = 4 * 1024 * 1024;

/// Bytes: bounds one connector configuration document.
///
/// Factories receive configuration already parsed, so the code that reads it as bytes checks this
/// before parsing.
pub const MAX_CONFIG_BYTES: u64 = 8 * 1024 * 1024;
