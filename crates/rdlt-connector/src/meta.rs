//! The engine's metadata columns: what every loaded row carries besides the source's columns.

/// The column holding the load that wrote each row: `FixedSizeBinary(16)`, the load id's UUID.
pub const LOAD_ID_COLUMN: &str = "_rdlt_load_id";

/// The column holding when each row's load started: `Timestamp(Microsecond, "UTC")`.
pub const LOADED_AT_COLUMN: &str = "_rdlt_loaded_at";

/// The column holding each row's id in the tables of a normalized stream: 16 bytes of `Binary`,
/// the xxh3-128 of its key, of the whole row, or of its parent's id and its position.
pub const ID_COLUMN: &str = "_rdlt_id";

/// The column holding the id of each child row's parent row: 16 bytes of `Binary`.
pub const PARENT_ID_COLUMN: &str = "_rdlt_parent_id";

/// The column holding the id of each child row's root row: 16 bytes of `Binary`.
pub const ROOT_ID_COLUMN: &str = "_rdlt_root_id";

/// The column holding each child row's position in its parent's array: `Int64`.
pub const IDX_COLUMN: &str = "_rdlt_idx";

/// The prefix every metadata column name starts with.
pub const META_PREFIX: &str = "_rdlt_";
