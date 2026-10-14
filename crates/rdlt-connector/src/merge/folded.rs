//! Folding the batches a merge gives back, for a destination that pays for each of them.
//!
//! A merge gives its rows back as one batch for each set of columns rows hold, and makes no
//! cell. A destination that writes a file for each batch pays for each: rows of many sets, as
//! writes whose batches each lack other columns leave, would be a file each, rewritten by every
//! commit. Past a destination's number of batches the smallest are joined under the columns any
//! of them holds, in groups that each hold at most a destination's number of cells without a
//! value.
//!
//! Rows whose text names the types it was rendered from join only rows naming the same: a join
//! keeps one name a column, which the merge reads where the column widens to `Json`. Each set of
//! names the joining batches hold takes a group of its own, and fewer of the largest batches stay
//! as they are, so the fold still gives back at most its number of batches where the sets are no
//! more than it.
//!
//! A cell without a value is counted whether a join made it or a row was written with it, so
//! a batch joined by one commit is measured by the next as what it is, and what a table holds
//! of such cells does not grow commit by commit. The fold is a function of its batches and
//! their order alone: the same rows fold into the same batches.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::{Array as _, ArrayRef, RecordBatch};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use rdlt_wire::limits::count;

use super::rendered::rendered_from;
use super::sparse::{Nulls, every_column};

/// The names a batch's text-stored columns give the types their text was rendered from, by
/// place in the table's schema.
type Names = BTreeMap<usize, String>;

/// `batches`, each of the columns of `schema` its rows hold, as at most `shapes` batches, the
/// largest as they are and the others joined, smallest first, into groups of rows of
/// one set of names that each hold at most `cells` cells without a value.
///
/// Where that cell limit splits a group, or the joining batches hold more sets of names than
/// `shapes`, there are as many more batches as that asks for.
pub fn folded(
    schema: &SchemaRef,
    mut batches: Vec<RecordBatch>,
    shapes: NonZeroUsize,
    cells: u64,
) -> Result<Vec<RecordBatch>, ArrowError> {
    let shapes = shapes.get();
    if batches.len() <= shapes {
        return Ok(batches);
    }
    let places: HashMap<&str, usize> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(place, field)| (field.name().as_str(), place))
        .collect();
    batches.sort_by_key(|batch| std::cmp::Reverse(batch.num_rows()));
    let mut names: Vec<Names> = batches.iter().map(|batch| named(batch, &places)).collect();
    // The largest stay as they are, as many as leave a group to each set of names the others hold.
    let sets = |from: usize| names[from..].iter().collect::<BTreeSet<_>>().len();
    let mut kept = shapes;
    while kept > 0 && sets(kept) > shapes - kept {
        kept -= 1;
    }
    let small = batches.split_off(kept);
    let mut by_names: BTreeMap<Names, Vec<RecordBatch>> = BTreeMap::new();
    for (batch, names) in small.into_iter().zip(names.split_off(kept)) {
        by_names.entry(names).or_default().push(batch);
    }
    for (names, alike) in by_names {
        let mut group = Group::default();
        for batch in alike.into_iter().rev() {
            let columns = held(&batch, &places);
            if !group.batches.is_empty() && group.absent_with(&batch, &columns) > cells {
                batches.push(std::mem::take(&mut group).joined(schema, &names)?);
            }
            group.add(batch, &columns);
        }
        batches.push(group.joined(schema, &names)?);
    }
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

    /// The group's batches as one, of the columns any of them holds, in the schema's order, each
    /// naming what `names` gives it; a group of one batch is that batch.
    fn joined(mut self, schema: &SchemaRef, names: &Names) -> Result<RecordBatch, ArrowError> {
        if let [_] = self.batches[..] {
            return self.batches.pop().ok_or_else(|| unjoined("no batch"));
        }
        // A column one of the batches lacks holds nulls for its rows, whatever the table says.
        let fields: Vec<_> = self
            .columns
            .iter()
            .map(|column| {
                let field = schema.field(*column).clone().with_nullable(true);
                match names.get(column) {
                    Some(rendered) => {
                        let mut metadata = field.metadata().clone();
                        metadata.insert(crate::LOGICAL_TYPE_KEY.to_owned(), rendered.clone());
                        field.with_metadata(metadata)
                    }
                    None => field,
                }
            })
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

/// The names `batch`'s text-stored columns give the types their text was rendered from.
fn named(batch: &RecordBatch, places: &HashMap<&str, usize>) -> Names {
    let fields = batch.schema_ref().fields().iter();
    fields
        .filter_map(|field| {
            let place = places.get(field.name().as_str())?;
            Some((*place, rendered_from(field)?.clone()))
        })
        .collect()
}

/// The rows of `batch` and how many of its cells hold a value.
fn sized(batch: &RecordBatch) -> (u64, u64) {
    let valued = |column: &ArrayRef| column.len() - column.logical_null_count();
    let cells: usize = batch.columns().iter().map(valued).sum();
    (count(batch.num_rows()), count(cells))
}
