//! The dialect seam (contract SM2): destinations own SQL TEXT only. Every
//! hook receives the shape's full semantic inputs and returns SQL; shapes,
//! ordering, survivorship, and unit rules live in [`crate::plan`].
//!
//! Defaults are the postgres dialect's text (the extraction source, golden-
//! pinned there). A destination that cannot honor a shape's exact semantics
//! must surface a TYPED capability gap at validation time — never override a
//! hook with an approximation (SM3).

/// SQL-text generation hooks for the shared merge shapes.
///
/// `Send + Sync`: plans borrow dialects across await points in async sessions.
pub trait MergeDialect: Send + Sync {
    /// Identifier quoting. Both current destinations double-quote with `"` doubling.
    fn quote(&self, ident: &str) -> String {
        format!("\"{}\"", ident.replace('"', "\"\""))
    }

    /// The stage's arrival-order expression (a quoted real column or a
    /// pseudo-column) — the deterministic last-wins tie-breaker.
    fn arrival_order(&self) -> String;

    /// Transaction-stable timestamp expression (the scd2 boundary, S5): every
    /// statement in one commit unit must observe the same instant.
    fn tx_timestamp(&self) -> &'static str {
        "now()"
    }

    /// The dedup/survivor subquery (MR1/MR2): one surviving row per identity
    /// group. `sort_prefix` is either empty (arrival last-wins) or
    /// `"col" DIR NULLS LAST, ` from a declared dedup_sort — values beat
    /// NULL, arrival stays the trailing tie-breaker.
    fn dedup_subquery(&self, identity: &str, sort_prefix: &str, stage: &str) -> String {
        format!(
            "(SELECT DISTINCT ON ({identity}) * FROM {stage} ORDER BY {identity}, {sort_prefix}{} DESC)",
            self.arrival_order()
        )
    }

    /// The upsert statement (M2): insert the surviving rows, updating matched
    /// keys in place. `action` is `DO UPDATE SET …` or `DO NOTHING`.
    fn upsert_stmt(
        &self,
        target: &str,
        cols: &str,
        deduped: &str,
        keep: &str,
        key_list: &str,
        action: &str,
    ) -> String {
        format!(
            "INSERT INTO {target} ({cols}) SELECT {cols} FROM {deduped} deduped{keep} \
             ON CONFLICT ({key_list}) {action}"
        )
    }
}
