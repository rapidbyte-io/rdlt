//! The DuckDB [`MergeDialect`] (feature 013): every probe passed
//! (tests/probes.rs), so every hook keeps the shared trait default —
//! DISTINCT ON survivor selection, ON CONFLICT upsert against the
//! auto-ensured unique index, transaction-stable `now()`. The only
//! divergence is the arrival order: DuckDB temp-table stages have no
//! arrival column; `rowid` reflects append order (the 006 finding-#7
//! determinism decision, unchanged).

use rdlt_connector_sqlcore::MergeDialect;

#[derive(Debug, Clone, Copy)]
pub struct DuckDialect;

impl MergeDialect for DuckDialect {
    fn arrival_order(&self) -> String {
        "rowid".into()
    }
}
