//! The engine's metadata columns: what every loaded row carries besides the source's columns.

/// The column holding the load that wrote each row: `FixedSizeBinary(16)`, the load id's UUID.
pub const LOAD_ID_COLUMN: &str = "_rdlt_load_id";

/// The column holding when each row's load started: `Timestamp(Microsecond, "UTC")`.
pub const LOADED_AT_COLUMN: &str = "_rdlt_loaded_at";

/// The prefix every metadata column name starts with.
pub const META_PREFIX: &str = "_rdlt_";
