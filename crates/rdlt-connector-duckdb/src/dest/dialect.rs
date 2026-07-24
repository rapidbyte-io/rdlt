//! The DuckDB [`MergeDialect`]: every capability probe passed
//! (tests/probes.rs), so every hook keeps the shared trait default —
//! DISTINCT ON survivor selection, ON CONFLICT upsert against the
//! auto-ensured unique index, transaction-stable `now()`. The only
//! divergence is the arrival order: DuckDB temp-table stages have no
//! arrival column, so `rowid` (which reflects append order) is the
//! deterministic last-wins tie-breaker.

use rdlt_connector_sqlcore::MergeDialect;

#[derive(Debug, Clone, Copy)]
pub struct DuckDialect;

impl MergeDialect for DuckDialect {
    fn arrival_order(&self) -> String {
        "rowid".into()
    }
}
