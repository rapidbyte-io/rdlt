//! Limits on what a connector may send; each is checked where data enters the SDK.

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
pub const MAX_CONFIG_BYTES: u64 = 8 * 1024 * 1024;
