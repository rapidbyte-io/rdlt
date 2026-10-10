//! Limits the connector SDK keeps beside the protocol's own in `rdlt_wire::limits`: what a
//! catalog, a plan, a destination's reserved names, a served source's acknowledgeable positions,
//! a connector's errors and its configuration's schema may hold, how short a destination's
//! identifiers may be, and how many connections a listening connector holds.

mod listen;
#[cfg(test)]
mod tests;

pub use listen::{ListenLimits, TooFewDescriptors, UnfairSessions};

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
