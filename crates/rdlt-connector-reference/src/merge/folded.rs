//! Folding the batches a merge gives back, so rows of many shapes do not become as many batches.
//!
//! A merge gives its rows back as one batch for each set of columns rows hold. A table whose
//! rows hold a few sets is a few batches, and a destination keeps each as it is. Rows of many
//! sets, as writes whose batches each lack other columns leave, would be a batch each, and a
//! file each where a file holds one set of columns: past [`MAX_SHAPES`] batches, the smallest
//! are joined under the columns any of them holds, in groups that each make at most
//! [`FOLD_CELLS`] cells no row of theirs had.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, Schema, SchemaRef};

use super::sparse::{Nulls, every_column};
use crate::limits::{FOLD_CELLS, MAX_SHAPES};

/// `batches`, each of the columns of `schema` its rows hold, as at most [`MAX_SHAPES`] batches
/// where joining the smallest costs no group more than [`FOLD_CELLS`] absent cells, and as few
/// more as that limit asks for where it does.
pub(super) fn folded(
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
        let columns: Vec<usize> = batch
            .schema_ref()
            .fields()
            .iter()
            .filter_map(|field| places.get(field.name().as_str()).copied())
            .collect();
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
    /// The cells the batches hold: each one's rows times its columns.
    cells: u64,
}

impl Group {
    /// The cells no row had that the group would make with `batch`, which holds `columns`.
    fn absent_with(&self, batch: &RecordBatch, columns: &[usize]) -> u64 {
        let more = columns
            .iter()
            .filter(|column| !self.columns.contains(column))
            .count();
        let width = count(self.columns.len() + more);
        let (rows, cells) = sized(batch, columns);
        let all = self.rows.saturating_add(rows).saturating_mul(width);
        all.saturating_sub(self.cells.saturating_add(cells))
    }

    fn add(&mut self, batch: RecordBatch, columns: &[usize]) {
        let (rows, cells) = sized(&batch, columns);
        self.rows = self.rows.saturating_add(rows);
        self.cells = self.cells.saturating_add(cells);
        self.columns.extend(columns);
        self.batches.push(batch);
    }

    /// The group's batches as one, of the columns any of them holds, in the schema's order.
    fn joined(self, schema: &SchemaRef) -> Result<RecordBatch, ArrowError> {
        let fields: Vec<_> = self
            .columns
            .iter()
            .map(|column| Arc::clone(&schema.fields()[*column]))
            .collect();
        let held = Arc::new(Schema::new(fields));
        let whole = every_column(&held, &self.batches, &mut Nulls::default())?;
        arrow_select::concat::concat_batches(&held, &whole)
    }
}

/// The rows of `batch`, which holds `columns`, and the cells they are.
fn sized(batch: &RecordBatch, columns: &[usize]) -> (u64, u64) {
    let rows = count(batch.num_rows());
    (rows, rows.saturating_mul(count(columns.len())))
}

fn count(of: usize) -> u64 {
    u64::try_from(of).unwrap_or(u64::MAX)
}
