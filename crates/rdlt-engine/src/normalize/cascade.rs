//! Rows dropped from a unit of a normalized stream, which take their descendants with them
//! (spec §8.7): a unit's parts are lowered parents first, so each part loses the rows whose
//! parent went before the schema policy meets its own.
//!
//! Rows are known by their part and position, not their ids, which rows sharing a key share.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::UInt32Type;
use arrow_array::{ArrayRef, BooleanArray};
use arrow_schema::ArrowError;

use super::{Lineage, Parent, Part};

/// The rows dropped from a unit so far: by their part's path, their positions among its rows as
/// normalized.
#[derive(Debug, Default)]
pub(crate) struct Dropped {
    rows: BTreeMap<Vec<Arc<str>>, BTreeSet<u32>>,
}

/// A part without its dropped rows.
#[derive(Debug)]
pub(crate) struct Pruned {
    pub(crate) part: Part,
    /// How many rows it lost.
    pub(crate) count: u64,
    /// Where it lost any, each remaining row's position among the part's rows as normalized.
    positions: Option<Vec<u32>>,
}

impl Dropped {
    /// Records the parent rows of `part`'s rows as dropped.
    pub(crate) fn parents_of(&mut self, part: &Part) {
        if let Some(parent) = &part.lineage.parent {
            let rows = parent.row.as_primitive::<UInt32Type>();
            self.rows
                .entry(parent.path.clone())
                .or_default()
                .extend(rows.values().iter().copied());
        }
    }

    /// `part` without its rows that were dropped, or whose parent was, which it records as
    /// dropped.
    pub(crate) fn prune(&mut self, part: Part) -> Result<Pruned, ArrowError> {
        let kept = {
            let own = self.rows.get(&part.path);
            let parents = part.lineage.parent.as_ref().and_then(|parent| {
                let dropped = self.rows.get(&parent.path)?;
                Some((dropped, parent.row.as_primitive::<UInt32Type>()))
            });
            if own.is_none() && parents.is_none() {
                return Ok(Pruned::whole(part));
            }
            (0..part.batch.num_rows())
                .map(|row| {
                    let dropped = own.is_some_and(|own| own.contains(&position(row)))
                        || parents
                            .is_some_and(|(dropped, rows)| dropped.contains(&rows.value(row)));
                    Some(!dropped)
                })
                .collect::<BooleanArray>()
        };
        let count = kept.len() - kept.true_count();
        if count == 0 {
            return Ok(Pruned::whole(part));
        }
        let dropped = self.rows.entry(part.path.clone()).or_default();
        dropped.extend(
            (0..kept.len())
                .filter(|row| !kept.value(*row))
                .map(position),
        );
        let positions = (0..kept.len()).filter(|row| kept.value(*row)).map(position);
        Ok(Pruned {
            positions: Some(positions.collect()),
            count: count as u64,
            part: retain(part, &kept)?,
        })
    }

    /// Records the rows of `pruned` that `kept` does not keep as dropped.
    pub(crate) fn unkept(&mut self, pruned: &Pruned, kept: &BooleanArray) {
        let original = |row: usize| {
            pruned
                .positions
                .as_ref()
                .map_or(position(row), |positions| positions[row])
        };
        let dropped = self.rows.entry(pruned.part.path.clone()).or_default();
        dropped.extend(
            (0..kept.len())
                .filter(|row| !kept.value(*row))
                .map(original),
        );
    }
}

impl Pruned {
    /// `part` with every row.
    fn whole(part: Part) -> Self {
        Self {
            part,
            count: 0,
            positions: None,
        }
    }
}

/// `row` as a position among a part's rows, which a batch's row count bounds.
fn position(row: usize) -> u32 {
    u32::try_from(row).unwrap_or(u32::MAX)
}

/// `part` with only the rows `kept` keeps.
fn retain(part: Part, kept: &BooleanArray) -> Result<Part, ArrowError> {
    let filter = |array: &ArrayRef| arrow_select::filter::filter(array.as_ref(), kept);
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
    Ok(Part {
        path: part.path,
        columns: part.columns,
        batch: arrow_select::filter::filter_record_batch(&part.batch, kept)?,
        lineage: Lineage {
            id: filter(&part.lineage.id)?,
            root_row: filter(&part.lineage.root_row)?,
            parent,
        },
    })
}
