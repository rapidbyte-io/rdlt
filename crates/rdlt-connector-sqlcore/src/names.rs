//! The SQL-destination protocol naming vocabulary — the `_rdlt_` naming
//! conventions for persisted state, defined ONCE so no destination
//! hand-types a prefix.
//!
//! These names are PERSISTED-FORMAT IDENTITY, not configuration: `read_state`
//! must find the table a previous run wrote, so a runtime-configurable prefix
//! would orphan state (cursor loss → double loading) the moment it changed.
//! A product-wide rename (e.g. platform branding) is a deliberate one-time
//! compile-time decision made HERE, before production data exists — never a
//! per-pipeline option. Tests deliberately keep literal spellings: they pin
//! the on-disk contract and catch an accidental rename.
//!
//! (System COLUMN names — `_rdlt_id` etc. — live in
//! `rdlt_core::schema::system_columns`: the engine writes those. scd2
//! validity defaults live in [`crate::options`] and ARE user-overridable —
//! they name user-visible data columns, not protocol state.)

/// State document table — state commits atomically with the data it describes.
pub const STATE_TABLE: &str = "_rdlt_state";

/// Commit receipt table — idempotence key is (load_id, commit_seq).
pub const COMMITS_TABLE: &str = "_rdlt_commits";

/// Stage table name prefix; destinations append their own scoping/hash
/// suffixes.
pub const STAGE_PREFIX: &str = "_rdlt_stage_";

/// Merge-identity index prefixes (auto-ensured per load): plain and unique.
pub const INDEX_PREFIX: &str = "rdlt_ix";
pub const UNIQUE_INDEX_PREFIX: &str = "rdlt_ux";

/// Deterministic supporting-index name — the ONE hash formula every SQL
/// destination shares: the unique/plain prefix plus a bounded hash of
/// `table:col,col`. The hash bounds the identifier under each destination's
/// length limit, and the determinism makes `CREATE INDEX IF NOT EXISTS`
/// idempotent across sessions.
pub fn index_name(unique: bool, table: &str, columns: &[String]) -> String {
    let prefix = if unique {
        UNIQUE_INDEX_PREFIX
    } else {
        INDEX_PREFIX
    };
    format!(
        "{prefix}_{}",
        rdlt_connector::core::naming::ident_hash(&format!("{table}:{}", columns.join(",")), 16)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_names_are_deterministic_and_distinct() {
        let a = index_name(false, "orders", &["id".into()]);
        let b = index_name(false, "orders", &["id".into()]);
        assert_eq!(a, b, "same inputs, same name (idempotency)");
        assert_ne!(
            a,
            index_name(true, "orders", &["id".into()]),
            "unique differs"
        );
        assert_ne!(a, index_name(false, "orders", &["other".into()]));
        assert_ne!(a, index_name(false, "other", &["id".into()]));
        assert!(a.len() <= 63, "under the identifier limit");
        assert!(a.starts_with("rdlt_ix_"));
        assert!(index_name(true, "t", &["k".into()]).starts_with("rdlt_ux_"));
    }
}
