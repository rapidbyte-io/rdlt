//! Folding the batches a merge gives back, for a destination that pays for each of them.
//!
//! A merge gives its rows back as one batch for each set of columns rows hold, and makes no
//! cell. A destination that writes a file for each batch pays for each: rows of many sets, as
//! writes whose batches each lack other columns leave, would be a file each, rewritten by every
//! commit. Past [`MAX_SHAPES`] batches the smallest are joined under the columns any of them
//! holds, in groups that each hold at most [`FOLD_CELLS`] cells without a value.
//!
//! A cell without a value is counted whether a join made it or a row was written with it, so
//! a batch joined by one commit is measured by the next as what it is, and what a table holds
//! of such cells does not grow commit by commit. The fold is a function of its batches and
//! their order alone: the same rows fold into the same batches.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow_array::{Array as _, ArrayRef, RecordBatch};
use arrow_schema::{ArrowError, Schema, SchemaRef};

use super::sparse::{Nulls, every_column};
use crate::limits::{FOLD_CELLS, MAX_SHAPES};

/// `batches`, each of the columns of `schema` its rows hold, as at most [`MAX_SHAPES`] batches
/// where the smallest join into one group of at most [`FOLD_CELLS`] cells without a value, and
/// as few more as that limit asks for where they do not.
pub(crate) fn folded(
    schema: &SchemaRef,
    mut batches: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>, ArrowError> {
    if batches.len() <= MAX_SHAPES {
        return Ok(batches);
    }
    // The largest stay as they are; the others join, the smallest first.
    batches.sort_by_key(|batch| std::cmp::Reverse(batch.num_rows()));
    let small = batches.split_off(MAX_SHAPES - 1);
    let places: HashMap<&str, usize> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(place, field)| (field.name().as_str(), place))
        .collect();
    let mut group = Group::default();
    for batch in small.into_iter().rev() {
        let columns = held(&batch, &places);
        if !group.batches.is_empty() && group.absent_with(&batch, &columns) > FOLD_CELLS {
            batches.push(std::mem::take(&mut group).joined(schema)?);
        }
        group.add(batch, &columns);
    }
    batches.push(group.joined(schema)?);
    Ok(batches)
}

/// Batches joining into one.
#[derive(Default)]
struct Group {
    batches: Vec<RecordBatch>,
    /// The columns any of the batches holds, by place in the table's schema.
    columns: BTreeSet<usize>,
    rows: u64,
    /// The cells of the batches that hold a value.
    cells: u64,
}

impl Group {
    /// The cells without a value the group would hold with `batch`, which holds `columns`.
    fn absent_with(&self, batch: &RecordBatch, columns: &[usize]) -> u64 {
        let more = columns
            .iter()
            .filter(|column| !self.columns.contains(column))
            .count();
        let width = count(self.columns.len() + more);
        let (rows, cells) = sized(batch);
        let all = self.rows.saturating_add(rows).saturating_mul(width);
        all.saturating_sub(self.cells.saturating_add(cells))
    }

    fn add(&mut self, batch: RecordBatch, columns: &[usize]) {
        let (rows, cells) = sized(&batch);
        self.rows = self.rows.saturating_add(rows);
        self.cells = self.cells.saturating_add(cells);
        self.columns.extend(columns);
        self.batches.push(batch);
    }

    /// The group's batches as one, of the columns any of them holds, in the schema's order; a
    /// group of one batch is that batch.
    fn joined(mut self, schema: &SchemaRef) -> Result<RecordBatch, ArrowError> {
        if let [_] = self.batches[..] {
            return self.batches.pop().ok_or_else(|| unjoined("no batch"));
        }
        // A column one of the batches lacks holds nulls for its rows, whatever the table says.
        let fields: Vec<_> = self
            .columns
            .iter()
            .map(|column| schema.field(*column).clone().with_nullable(true))
            .collect();
        let held = Arc::new(Schema::new(fields));
        let whole = every_column(&held, &self.batches, &mut Nulls::default())?;
        arrow_select::concat::concat_batches(&held, &whole)
    }
}

fn unjoined(what: &str) -> ArrowError {
    ArrowError::InvalidArgumentError(format!("a group of {what} joins into nothing"))
}

/// The places in the table's schema of the columns `batch` holds.
fn held(batch: &RecordBatch, places: &HashMap<&str, usize>) -> Vec<usize> {
    let fields = batch.schema_ref().fields().iter();
    fields
        .filter_map(|field| places.get(field.name().as_str()).copied())
        .collect()
}

/// The rows of `batch` and how many of its cells hold a value.
fn sized(batch: &RecordBatch) -> (u64, u64) {
    let valued = |column: &ArrayRef| column.len() - column.logical_null_count();
    let cells: usize = batch.columns().iter().map(valued).sum();
    (count(batch.num_rows()), count(cells))
}

fn count(of: usize) -> u64 {
    u64::try_from(of).unwrap_or(u64::MAX)
}
