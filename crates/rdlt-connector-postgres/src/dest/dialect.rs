//! The postgres [`MergeDialect`]: the EXTRACTION SOURCE — every
//! hook keeps the trait default (the defaults ARE this destination's text,
//! golden-pinned in tests/golden_sql.rs) except the arrival column, which is
//! the stage's real `__rdlt_arrival` identity column.

use rdlt_connector_sqlcore::MergeDialect;

#[derive(Debug, Clone, Copy)]
pub struct PgDialect;

impl MergeDialect for PgDialect {
    fn arrival_order(&self) -> String {
        quote(super::commit::ARRIVAL_COL)
    }

    fn clear_table(&self, table: &str) -> String {
        format!("TRUNCATE TABLE {table}")
    }

    /// `ON COMMIT DROP` ties the relation's life to the publish transaction,
    /// which is exactly its useful span: it exists for the arm's statements
    /// and is gone before anything else could observe or collide with it —
    /// including a retry of this same unit.
    fn materialize_dedup(&self, name: &str, subquery: &str) -> Option<String> {
        Some(format!(
            "CREATE TEMP TABLE {name} ON COMMIT DROP AS SELECT * FROM {subquery} deduped"
        ))
    }
}

pub(crate) fn quote(ident: &str) -> String {
    // The one injection-safe quoting rule, shared with every SQL destination
    // (and the dialect seam's default). Kept as a thin local alias so the many
    // DDL/publish call sites read `quote(...)`.
    rdlt_connector_sqlcore::quote_identifier(ident)
}
