//! The caps every shred seat consults, and the one refusal both assembly
//! seats share.

use rdlt_core::error::Error;
use rdlt_core::id::TableName;

/// Cumulative source columns retained for one logical table. System columns
/// sit outside this count. The SPI's shared width — the wire's batch decode
/// and the ensure seat hold the same number, so a table legal in-process is
/// legal on the wire and one raise moves every seat together.
pub(crate) const MAX_SOURCE_COLUMNS_PER_TABLE: usize =
    rdlt_connector::gate::MAX_SOURCE_COLUMNS_PER_TABLE;

/// Distinct child-table source keys retained beneath one parent table.
pub(crate) const MAX_CHILD_TABLES_PER_PARENT: usize = 1024;

/// Total tables (root plus every discovered child, at any depth) retained for
/// one stream. The per-parent cap alone leaves the TOTAL unbounded — nesting
/// multiplies parents — and every push does per-table bookkeeping, so
/// unbounded tables turn one crafted frame into unbounded work and memory.
pub(crate) const MAX_TABLES_PER_STREAM: usize = 64 * 1024;

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
             `config::Config::with_max_batch_cells` (the facade's \
             `pipeline::Builder::max_batch_cells` plumbs the same knob)"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    /// The product, not the axes, is what's bounded: at the cap exactly it
    /// passes (the refusal is `>`, not `>=` — an off-by-one here rejects a
    /// legitimate maximal batch), one cell over refuses, and a saturated
    /// product refuses instead of wrapping.
    #[test]
    fn the_cell_budget_is_inclusive_at_its_boundary() {
        let table = TableName::new("t");
        refuse_over_cell_budget(
            &table,
            1 << 14,
            Config::DEFAULT_MAX_BATCH_CELLS >> 14,
            Config::DEFAULT_MAX_BATCH_CELLS,
        )
        .expect("exactly the cap assembles");
        let error = refuse_over_cell_budget(
            &table,
            1 << 14,
            (Config::DEFAULT_MAX_BATCH_CELLS >> 14) + 1,
            Config::DEFAULT_MAX_BATCH_CELLS,
        )
        .expect_err("one cell over the cap refuses");
        assert!(
            error.to_string().contains("cell"),
            "names the budget: {error}"
        );
        refuse_over_cell_budget(&table, usize::MAX, 2, Config::DEFAULT_MAX_BATCH_CELLS)
            .expect_err("a saturating product refuses, never wraps");
    }
}
