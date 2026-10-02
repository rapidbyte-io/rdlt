//! Limits on what a connector may send or receive; each is checked where the data enters rdlt.

mod listen;
#[cfg(test)]
mod tests;

pub use listen::{ListenLimits, TooFewDescriptors, UnfairSessions};

/// Bytes: bounds one JSON push.
pub const MAX_JSON_PUSH_BYTES: u64 = 64 * 1024 * 1024;

/// Rows: bounds one Arrow or change batch.
pub const MAX_BATCH_ROWS: u64 = 1024 * 1024;

/// Values: bounds what a frame holding one batch's rows would hold, as the wire weighs it.
///
/// Nested values and the items list views name count, whether or not they take bytes. Each
/// dictionary's values are bounded apart, as the frame of their own they would go in.
pub const MAX_BATCH_VALUES: u64 = 64 * MAX_BATCH_ROWS;

/// Bytes: bounds what the views of one batch name in their data buffers, counted once a view.
pub const MAX_VIEW_BYTES: u64 = 64 * 1024 * 1024;

/// Bytes: bounds one batch twice: the bytes a frame holding its rows would hold, as the wire
/// weighs it, and the allocations it keeps alive, each counted once, where a slice counts the
/// whole buffer it shares.
pub const MAX_BATCH_BYTES: u64 = 64 * 1024 * 1024;

/// Columns: bounds the width of one batch, counting nested fields.
pub const MAX_COLUMNS: u64 = 10_000;

/// Levels: bounds how deep a batch's types or a JSON value nest, counting a top-level column or
/// the record itself as the first.
pub const MAX_NESTING_DEPTH: u64 = 64;

/// Streams: bounds one catalog.
pub const MAX_CATALOG_STREAMS: usize = 65_536;

/// Partitions: bounds the partitions one plan of a stream names.
///
/// The engine tracks each partition a plan names until its read ends, and starts a task to read
/// it: a partition waiting for its turn costs about a kilobyte, outside the memory budget.
pub const MAX_PLAN_PARTITIONS: usize = 16_384;

/// Words: bounds the words a destination reserves.
pub const MAX_RESERVED_WORDS: usize = 4096;

/// Prefixes: bounds the table prefixes a destination reserves.
pub const MAX_RESERVED_PREFIXES: usize = 64;

/// Bytes: bounds one word or table prefix a destination reserves.
pub const MAX_RESERVED_BYTES: usize = 256;

/// Bytes: the least identifier length a destination may declare, which holds an identifier's
/// hash, the `_` before it, and a character of its name.
pub const MIN_IDENTIFIER_LEN: u16 = 16;

/// Bytes: bounds one encoded cursor.
pub const MAX_CURSOR_BYTES: u64 = 4 * 1024 * 1024;

/// Positions: how many checkpoints its reads sent one host a served source remembers, to hear
/// that host report one of them committed, and for how many partitions where a read started.
///
/// Beyond them the oldest checkpoint is forgotten, and a report of it refused as transient; the
/// starts are counted apart, so no number of checkpoints forgets where a read started. Each
/// costs about forty bytes, so a host costs the connector some twenty megabytes at most.
pub const MAX_ACKNOWLEDGEABLE: usize = 1 << 18;

/// Bytes: bounds the message, and each cause, of a connector's error as a host keeps it.
pub const MAX_ERROR_TEXT_BYTES: usize = 4096;

/// Bytes: bounds the machine code of a connector's error as a host keeps it.
pub const MAX_ERROR_CODE_BYTES: usize = 128;

/// Causes: bounds the chain of causes of a connector's error a host keeps.
pub const MAX_ERROR_CAUSES: usize = 8;

/// Bytes: bounds the JSON schema of a connector's configuration, as its handshake answers it.
pub const MAX_CONFIG_SCHEMA_BYTES: usize = 1024 * 1024;

/// Bytes: bounds one connector configuration document.
///
/// Factories receive configuration already parsed, so the code that reads it as bytes checks this
/// before parsing.
pub const MAX_CONFIG_BYTES: u64 = 8 * 1024 * 1024;
