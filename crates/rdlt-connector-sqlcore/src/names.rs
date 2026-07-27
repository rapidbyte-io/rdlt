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

/// Targets a load has already cleared — the durable half of the
/// once-per-(load, target) Replace guard, for destinations that write full
/// loads STRAIGHT INTO the target.
///
/// A staged destination does not need it: there the clear is planned inside
/// the publish transaction for every Replace table at once, so "did this load
/// clear this target" is answered by "did any unit of this load commit".
/// Writing directly, the clear happens at a target's FIRST WRITE, and those
/// firsts are spread across commit units — so the question becomes per-target
/// and needs its own record.
///
/// The record is written in the SAME transaction as the TRUNCATE it describes,
/// so the two cannot disagree: either both are durable or neither happened.
pub const CLEARED_TABLE: &str = "_rdlt_cleared";

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

/// The supporting-index DDL, built once for every SQL destination.
///
/// The name formula already lived here; the STATEMENT around it was copied into
/// both executors, differing only in which `quote` they reached for. Two copies
/// of one statement is how the emitted SQL drifts apart without anyone deciding
/// that it should — and index DDL is exactly where that matters, because
/// `IF NOT EXISTS` makes a drifted name silently create a SECOND index rather
/// than fail.
///
/// `quote` is passed in because identifier quoting is the one thing a
/// destination genuinely owns.
pub fn create_index_sql(
    unique: bool,
    table: &str,
    columns: &[String],
    quote: impl Fn(&str) -> String,
) -> String {
    let name = index_name(unique, table, columns);
    let cols = columns
        .iter()
        .map(|c| quote(c))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CREATE {}INDEX IF NOT EXISTS {} ON {} ({cols})",
        if unique { "UNIQUE " } else { "" },
        quote(&name),
        quote(table),
    )
}

/// The diagnosis for a unique index that cannot be created because the target
/// already holds duplicate keys.
///
/// Shared because it is ADVICE, not a message: it names the strategy that
/// requires the index, the columns that collide, and the two ways out. Both
/// destinations had their own copy of that sentence, so a correction to one
/// would have left the other telling operators something different about the
/// same failure.
pub fn duplicate_merge_key_diagnosis(table: &str, columns: &[String], cause: &str) -> String {
    format!(
        "table `{table}`: cannot create the unique index the upsert strategy \
         requires — existing rows duplicate the merge key ({}); deduplicate the \
         table or use delete_insert: {cause}",
        columns.join(", ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The golden pin these statements never had.
    ///
    /// Index DDL was the one piece of shared SQL with no byte-level pin, in both
    /// destinations, while every merge arm around it had one. `IF NOT EXISTS`
    /// is what makes that dangerous: a drifted name does not fail, it creates a
    /// SECOND index — so the failure is silent duplication rather than an error.
    #[test]
    fn index_ddl_is_pinned_byte_for_byte() {
        let q = |i: &str| format!("\"{i}\"");
        let cols = vec!["id".to_string(), "day".to_string()];

        let unique = create_index_sql(true, "orders", &cols, q);
        assert_eq!(
            unique,
            format!(
                "CREATE UNIQUE INDEX IF NOT EXISTS \"{}\" ON \"orders\" (\"id\", \"day\")",
                index_name(true, "orders", &cols)
            )
        );

        let plain = create_index_sql(false, "orders", &cols, q);
        assert_eq!(
            plain,
            format!(
                "CREATE INDEX IF NOT EXISTS \"{}\" ON \"orders\" (\"id\", \"day\")",
                index_name(false, "orders", &cols)
            )
        );

        // The two differ in more than the keyword: the NAME differs too, so a
        // plain and a unique index over the same columns can coexist.
        assert_ne!(unique, plain);
        assert_ne!(
            index_name(true, "orders", &cols),
            index_name(false, "orders", &cols)
        );

        // Quoting is the destination's, and it reaches every identifier —
        // the index name, the table, and each column.
        let backtick = create_index_sql(true, "orders", &cols, |i| format!("`{i}`"));
        assert!(backtick.contains("`orders`"), "{backtick}");
        assert!(backtick.contains("(`id`, `day`)"), "{backtick}");
        assert!(
            backtick.contains(&format!("`{}`", index_name(true, "orders", &cols))),
            "{backtick}"
        );
    }

    /// The duplicate-key diagnosis is ADVICE, and its shape is the contract:
    /// which strategy needs the index, which columns collide, and the two ways
    /// out.
    #[test]
    fn the_duplicate_key_diagnosis_names_columns_and_both_remedies() {
        let msg = duplicate_merge_key_diagnosis(
            "orders",
            &["id".to_string(), "day".to_string()],
            "SQLSTATE 23505",
        );
        assert!(msg.contains("`orders`"), "{msg}");
        assert!(
            msg.contains("id, day"),
            "names the colliding columns: {msg}"
        );
        assert!(
            msg.contains("upsert"),
            "names the strategy that requires it: {msg}"
        );
        assert!(
            msg.contains("delete_insert"),
            "offers the alternative: {msg}"
        );
        assert!(
            msg.contains("SQLSTATE 23505"),
            "keeps the driver's cause: {msg}"
        );
    }

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
