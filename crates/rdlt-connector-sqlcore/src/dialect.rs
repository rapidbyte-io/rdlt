//! The dialect seam: destinations own SQL TEXT only. Every hook receives the
//! shape's full semantic inputs and returns SQL; shapes, ordering,
//! survivorship, and unit rules live in [`crate::plan`].
//!
//! The default hook bodies emit standard SQL that both current destinations
//! accept. A destination that cannot honor a shape's exact semantics must
//! surface a TYPED capability gap at validation time — never override a hook
//! with an approximation that silently changes the result.

/// Identifier quoting — the ONE injection-safety implementation for every SQL
/// destination: wrap in double quotes and double any embedded quote. Both
/// current destinations quote identically; the [`MergeDialect::quote`] hook
/// defaults to this, and each destination's own DDL/publish quoting delegates
/// here too, so the escaping rule exists in exactly one place.
pub fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// SQL-text generation hooks for the shared merge shapes.
///
/// `Send + Sync`: plans borrow dialects across await points in async sessions.
pub trait MergeDialect: Send + Sync {
    /// Identifier quoting. Both current destinations double-quote with `"`
    /// doubling — the shared [`quote_ident`] rule.
    fn quote(&self, ident: &str) -> String {
        quote_ident(ident)
    }

    /// The stage's arrival-order expression (a quoted real column or a
    /// pseudo-column) — the deterministic last-wins tie-breaker.
    fn arrival_order(&self) -> String;

    /// Transaction-stable timestamp expression (the scd2 boundary): every
    /// statement in one commit unit must observe the same instant.
    fn tx_timestamp(&self) -> &'static str {
        "now()"
    }

    /// Remove every row of a table — used to clear a Replace target (first unit
    /// of a load) and to truncate a stage after publish. `table` is already
    /// quoted. The two current destinations differ only here: Postgres
    /// `TRUNCATE TABLE`, DuckDB `DELETE FROM` (temp tables reject TRUNCATE).
    fn clear_table(&self, table: &str) -> String;

    /// The dedup/survivor subquery: one surviving row per identity
    /// group. `sort_prefix` is either empty (arrival last-wins) or
    /// `"col" DIR NULLS LAST, ` from a declared dedup_sort — values beat
    /// NULL, arrival stays the trailing tie-breaker.
    fn dedup_subquery(&self, identity: &str, sort_prefix: &str, stage: &str) -> String {
        format!(
            "(SELECT DISTINCT ON ({identity}) * FROM {stage} ORDER BY {identity}, {sort_prefix}{} DESC)",
            self.arrival_order()
        )
    }

    /// The upsert statement: insert the surviving rows, updating matched
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
