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

/// A schema's PHYSICAL width: every leaf the builders allocate, at
/// every depth. A struct column is one logical column but its fields
/// are each an array — null-filled for every row that lacks them,
/// concatenated by the coalescer, cast by the Arrow seat — so a budget
/// counting logical columns lets a compact batch declare a struct of
/// thousands of fields and assemble a multiple of the budget it was
/// held to. A list is one leaf: its items are as many as the rows
/// carry, bounded by the value budget, not by the width.
pub(crate) fn physical_width(columns: &[rdlt_core::schema::Column]) -> usize {
    columns
        .iter()
        .map(|column| match &column.column_type {
            rdlt_core::schema::ColumnType::Struct { fields } => physical_width(fields),
            rdlt_core::schema::ColumnType::Scalar { .. }
            | rdlt_core::schema::ColumnType::ScalarList { .. } => 1,
        })
        .sum()
}

/// [`physical_width`] of an Arrow schema — the coalescer's seat, which
/// sees batches rather than the registry's columns.
pub(crate) fn physical_width_of(fields: &arrow::datatypes::Fields) -> usize {
    fields
        .iter()
        .map(|field| match field.data_type() {
            arrow::datatypes::DataType::Struct(children) => physical_width_of(children),
            _ => 1,
        })
        .sum()
}

/// The typed refusal both assembly seats (the Arrow path and the resolve
/// pipeline's build call) share, so the cap speaks with one voice.
/// `columns` is the PHYSICAL width ([`physical_width`]).
pub(crate) fn refuse_over_cell_budget(
    table: &TableName,
    columns: usize,
    rows: usize,
    budget: usize,
) -> Result<(), Error> {
    let cells = columns.saturating_mul(rows);
    if cells > budget {
        return Err(Error::config(format!(
            "table `{table}`: assembling {columns} columns (nested fields counted) × {rows} rows \
             is {cells} cells, \
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

    /// The physical width counts every leaf at every depth — a struct
    /// of a thousand fields is a thousand columns to the budget, a list
    /// is one — and the logical and Arrow spellings agree.
    #[test]
    fn physical_width_counts_nested_leaves_in_both_spellings() {
        use rdlt_core::schema::{Column, ColumnType, Provenance};
        use rdlt_core::types::LogicalType;
        let leaf = |name: &str| Column {
            name: name.into(),
            column_type: ColumnType::scalar(LogicalType::Int64),
            nullable: true,
            provenance: Provenance::Inferred,
        };
        let columns = vec![
            leaf("a"),
            Column {
                name: "s".into(),
                column_type: ColumnType::Struct {
                    fields: vec![
                        leaf("x"),
                        Column {
                            name: "inner".into(),
                            column_type: ColumnType::Struct {
                                fields: (0..1000).map(|i| leaf(&format!("f{i}"))).collect(),
                            },
                            ..leaf("inner")
                        },
                    ],
                },
                ..leaf("s")
            },
            Column {
                name: "l".into(),
                column_type: ColumnType::ScalarList {
                    item: LogicalType::Int64,
                },
                ..leaf("l")
            },
        ];
        assert_eq!(physical_width(&columns), 1 + 1 + 1000 + 1);
        let arrow = crate::shred::types::arrow_schema(&rdlt_core::schema::TableSchema {
            table: TableName::new("t"),
            parent: None,
            columns,
        });
        assert_eq!(physical_width_of(arrow.fields()), 1 + 1 + 1000 + 1);
    }

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
