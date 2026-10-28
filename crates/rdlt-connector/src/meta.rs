//! The engine's metadata columns: what every loaded row carries besides the source's columns.

/// The column holding the epoch of the session that wrote each row: `Int64`.
pub const LOAD_EPOCH_COLUMN: &str = "_rdlt_load_epoch";

/// The column holding when each row's load started: `Timestamp(Microsecond, "UTC")`.
pub const LOADED_AT_COLUMN: &str = "_rdlt_loaded_at";

/// The column holding each row's id in the tables of a normalized stream, `Binary`: for a merge
/// stream's root row the 32 bytes of the BLAKE3 hash of its key, and for every other row its
/// position, its session's epoch, segment, piece, part and place, as numbers whose bytes compare
/// as they do.
pub const ID_COLUMN: &str = "_rdlt_id";

/// The column holding the id of each child row's parent row: `Binary`, as `_rdlt_id` holds it.
pub const PARENT_ID_COLUMN: &str = "_rdlt_parent_id";

/// The column holding the id of each child row's root row: `Binary`, as `_rdlt_id` holds it.
pub const ROOT_ID_COLUMN: &str = "_rdlt_root_id";

/// The column holding each child row's position in its parent's array: `Int32`.
pub const IDX_COLUMN: &str = "_rdlt_idx";

/// The column recording when a change stream's soft delete removed each row:
/// `Timestamp(Microsecond, "UTC")`, null for rows not deleted.
pub const DELETED_AT_COLUMN: &str = "_rdlt_deleted_at";

/// The column holding when each version of a history table's key begins:
/// `Timestamp(Microsecond, "UTC")`.
pub const VALID_FROM_COLUMN: &str = "_rdlt_valid_from";

/// The column holding when a later change closed each version of a history table's key:
/// `Timestamp(Microsecond, "UTC")`, null while it is current.
pub const VALID_TO_COLUMN: &str = "_rdlt_valid_to";

/// The column holding whether each version of a history table's key is current: `Boolean`.
pub const IS_CURRENT_COLUMN: &str = "_rdlt_is_current";

/// The column holding the hash of each version's data columns in a history table: 32 bytes of
/// `Binary`, the BLAKE3 hash of their values.
pub const ROW_HASH_COLUMN: &str = "_rdlt_row_hash";

/// The prefix every metadata column name starts with.
pub const META_PREFIX: &str = "_rdlt_";
