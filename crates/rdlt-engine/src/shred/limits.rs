//! The caps every shred seat consults, and the one refusal both assembly
//! seats share.

use rdlt_core::error::Error;
use rdlt_core::id::TableName;

/// Cumulative source columns retained for one logical table. System columns
/// sit outside this count.
pub(crate) const MAX_SOURCE_COLUMNS_PER_TABLE: usize = 4096;

/// Distinct child-table source keys retained beneath one parent table.
pub(crate) const MAX_CHILD_TABLES_PER_PARENT: usize = 1024;

/// Total tables (root plus every discovered child, at any depth) retained for
/// one stream. The per-parent cap alone leaves the TOTAL unbounded — nesting
/// multiplies parents — and every push does per-table bookkeeping, so
/// unbounded tables turn one crafted frame into unbounded work and memory.
pub(crate) const MAX_TABLES_PER_STREAM: usize = 64 * 1024;

/// The most CELLS one outgoing batch may assemble by default — `columns ×
/// rows`. The row cap and the column cap each bound one axis, but batch
/// assembly pays their PRODUCT: every schema column is built for every row,
/// absent columns null-filled and the load id stamped per row, so a wide
/// registry bootstrapped from a ~50 KB empty batch plus one 1M-row push would
/// otherwise assemble ~16 GiB of engine-side expansion from ~175 KB of wire —
/// before any downstream byte metering, which prices inputs, not expansions.
/// 2²⁸ cells bounds the null-fill transient at ≈1 GiB; a ~260-column table at
/// the 1M-row cap sits just under it, and wider honest shapes raise the budget
/// through the engine config, whose default this aliases.
pub(crate) const MAX_BATCH_CELLS: usize = crate::DEFAULT_MAX_BATCH_CELLS;

/// The typed refusal both assembly seats (the Arrow path and the resolve
/// pipeline's build call) share, so the cap speaks with one voice.
pub(crate) fn refuse_over_cell_budget(
    table: &TableName,
    columns: usize,
    rows: usize,
    budget: usize,
) -> Result<(), Error> {
    let cells = columns.saturating_mul(rows);
    if cells > budget {
        return Err(Error::config(format!(
            "table `{table}`: assembling {columns} columns × {rows} rows is {cells} cells, \
             over the {budget}-cell per-batch budget — assembly null-fills absent columns \
             and stamps lineage per cell, so the product is bounded independently of the \
             input's encoded size (what this refuses: ~16 GiB of null-fill assembled from \
             ~175 KB of wire). Push smaller batches, or raise the budget with \
             `EngineConfig::with_max_batch_cells` (the facade's \
             `pipeline::Builder::max_batch_cells` plumbs the same knob)"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The product, not the axes, is what's bounded: at the cap exactly it
    /// passes (the refusal is `>`, not `>=` — an off-by-one here rejects a
    /// legitimate maximal batch), one cell over refuses, and a saturated
    /// product refuses instead of wrapping.
    #[test]
    fn the_cell_budget_is_inclusive_at_its_boundary() {
        let table = TableName::new("t");
        refuse_over_cell_budget(&table, 1 << 14, MAX_BATCH_CELLS >> 14, MAX_BATCH_CELLS)
            .expect("exactly the cap assembles");
        let error = refuse_over_cell_budget(
            &table,
            1 << 14,
            (MAX_BATCH_CELLS >> 14) + 1,
            MAX_BATCH_CELLS,
        )
        .expect_err("one cell over the cap refuses");
        assert!(
            error.to_string().contains("cell"),
            "names the budget: {error}"
        );
        refuse_over_cell_budget(&table, usize::MAX, 2, MAX_BATCH_CELLS)
            .expect_err("a saturating product refuses, never wraps");
    }
}
