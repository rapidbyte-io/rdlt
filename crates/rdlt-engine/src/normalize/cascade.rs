//! Rows dropped from a unit of a normalized stream, which take their descendants with them: a
//! unit's parts are lowered parents first, so each part loses the rows whose parent went before the
//! schema policy meets its own.
//!
//! Rows are known by their part and position, not their ids, which rows sharing a key share: a
//! bit for each of a part's rows as normalized, set where the row went.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::UInt32Type;
use arrow_array::{Array, ArrayRef, BooleanArray};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use arrow_schema::ArrowError;
use arrow_select::filter::FilterBuilder;

use super::{Lineage, Parent, Part};

/// The rows dropped from a unit so far: by their part's path, a bit for each of its rows as
/// normalized, set where the row was dropped.
#[derive(Debug, Default)]
pub(crate) struct Dropped {
    rows: BTreeMap<Vec<Arc<str>>, Bits>,
}

/// A part without its dropped rows.
#[derive(Debug)]
pub(crate) struct Pruned {
    pub(crate) part: Part,
    /// How many rows it lost.
    pub(crate) count: u64,
    /// Where it lost any, which of the part's rows as normalized it kept.
    kept: Option<BooleanBuffer>,
}

impl Dropped {
    /// Whether no row was dropped.
    pub(crate) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Records the parent rows of `part`'s rows as dropped.
    pub(crate) fn parents_of(&mut self, part: &Part) {
        if let Some(parent) = &part.lineage.parent {
            let dropped = self.rows.entry(parent.path.clone()).or_default();
            for row in parent.row.as_primitive::<UInt32Type>().values() {
                dropped.set(at(*row));
            }
        }
    }

    /// `part` without its rows that were dropped, or whose parent was, which it records as
    /// dropped.
    pub(crate) fn prune(&mut self, part: Part) -> Result<Pruned, ArrowError> {
        let rows = part.batch.num_rows();
        let kept = {
            let own = self.rows.get(&part.path);
            let parents = part.lineage.parent.as_ref().and_then(|parent| {
                let dropped = self.rows.get(&parent.path)?;
                Some((dropped, parent.row.as_primitive::<UInt32Type>().values()))
            });
            if own.is_none() && parents.is_none() {
                return Ok(Pruned::whole(part));
            }
            BooleanBuffer::collect_bool(rows, |row| {
                let gone = own.is_some_and(|own| own.get(row))
                    || parents.is_some_and(|(dropped, parents)| dropped.get(at(parents[row])));
                !gone
            })
        };
        let count = rows - kept.count_set_bits();
        if count == 0 {
            return Ok(Pruned::whole(part));
        }
        let own = self.rows.entry(part.path.clone()).or_default();
        for row in (0..rows).filter(|row| !kept.value(*row)) {
            own.set(row);
        }
        Ok(Pruned {
            count: u64::try_from(count).expect("a part's rows are counted in 64 bits"),
            part: retain(part, &kept)?,
            kept: Some(kept),
        })
    }

    /// Records the rows of `pruned` that `kept` does not keep as dropped.
    pub(crate) fn unkept(&mut self, pruned: &Pruned, kept: &BooleanArray) {
        let own = self.rows.entry(pruned.part.path.clone()).or_default();
        match &pruned.kept {
            // Pruning keeps rows in order: the n-th row left is the n-th it kept.
            Some(original) => {
                for (row, original) in original.set_indices().enumerate() {
                    if !kept.value(row) {
                        own.set(original);
                    }
                }
            }
            None => {
                for row in (0..kept.len()).filter(|row| !kept.value(*row)) {
                    own.set(row);
                }
            }
        }
    }
}

impl Pruned {
    /// `part` with every row.
    pub(crate) fn whole(part: Part) -> Self {
        Self {
            part,
            count: 0,
            kept: None,
        }
    }
}

/// A bit for each of a part's rows as normalized, set where the row was dropped; a row past the
/// last bit set was not.
#[derive(Debug)]
struct Bits(BooleanBufferBuilder);

impl Default for Bits {
    fn default() -> Self {
        Self(BooleanBufferBuilder::new(0))
    }
}

impl Bits {
    /// Records the row at `row` as dropped.
    fn set(&mut self, row: usize) {
        if row >= self.0.len() {
            self.0.resize(row + 1);
        }
        self.0.set_bit(row, true);
    }

    /// Whether the row at `row` was dropped.
    fn get(&self, row: usize) -> bool {
        row < self.0.len() && self.0.get_bit(row)
    }
}

/// `row`, a position among a part's rows, as an index.
fn at(row: u32) -> usize {
    usize::try_from(row).expect("a 32-bit position fits in an index")
}

/// `part` with only the rows `kept` keeps, and without the columns only dropped rows held values
/// in, so dropped rows change no schema.
fn retain(part: Part, kept: &BooleanBuffer) -> Result<Part, ArrowError> {
    // One predicate filters the batch and every array of its lineage.
    let predicate = FilterBuilder::new(&BooleanArray::new(kept.clone(), None))
        .optimize()
        .build();
    let filter = |array: &ArrayRef| predicate.filter(array.as_ref());
    let parent = part
        .lineage
        .parent
        .map(|parent| {
            Ok::<_, ArrowError>(Parent {
                id: filter(&parent.id)?,
                root: filter(&parent.root)?,
                idx: filter(&parent.idx)?,
                row: filter(&parent.row)?,
                path: parent.path,
            })
        })
        .transpose()?;
    let batch = predicate.filter_record_batch(&part.batch)?;
    let held = |array: &ArrayRef| array.logical_null_count() < array.len();
    let columns: Vec<usize> = (0..batch.num_columns())
        .filter(|column| held(batch.column(*column)) || !held(part.batch.column(*column)))
        .collect();
    Ok(Part {
        path: part.path,
        columns: columns
            .iter()
            .map(|column| part.columns[*column].clone())
            .collect(),
        batch: batch.project(&columns)?,
        lineage: Lineage {
            id: filter(&part.lineage.id)?,
            root_row: filter(&part.lineage.root_row)?,
            parent,
        },
    })
}

#[cfg(test)]
mod tests;
