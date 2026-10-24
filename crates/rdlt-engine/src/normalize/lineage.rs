//! Where a normalized row comes from, and the lineage columns lowering builds from it.
//!
//! A merge stream's root row is identified by its key's hash, which the merge matches it by.
//! Every other row, a child row of any stream and a root row of a stream that does not merge, is
//! identified by its position: the session's epoch, the segment its piece was written to, the
//! position of the piece's first root row among the rows the segment received, its part's
//! ordinal among the parts the piece normalized into and its own position in that part, each a
//! variable-length integer whose bytes compare as it does. Each open of a destination takes an
//! epoch no earlier session of the pipeline had, an attempt numbers its segments apart and a
//! segment its pieces, so no two rows a pipeline loads share an id.

pub(crate) mod varint;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::builder::BinaryBuilder;
use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, BinaryArray, UInt32Array};
use arrow_schema::ArrowError;
use arrow_select::filter::FilterPredicate;
use rdlt_connector::{Epoch, SegmentId};

/// Where the rows of a part come from.
#[derive(Clone, Debug)]
pub(crate) struct Lineage {
    /// The part's ordinal among the parts its batch normalized into: 0 for the stream's own table.
    pub(crate) part: u32,
    /// Each row's position among its part's rows as normalized, before any was dropped: a
    /// `UInt32` array.
    pub(crate) row: ArrayRef,
    /// A merge stream's root rows' ids, by their position among the batch's: one array every part
    /// of the batch shares, which no cut or drop of a part changes.
    pub(crate) keys: Option<ArrayRef>,
    /// For a child table, each row's parent and root.
    pub(crate) parent: Option<Parent>,
}

/// The parents and roots of a child table's rows.
#[derive(Clone, Debug)]
pub(crate) struct Parent {
    /// The path of the parent rows' part.
    pub(crate) path: Vec<Arc<str>>,
    /// The parent rows' part's ordinal.
    pub(crate) part: u32,
    /// The position of each row's parent among its part's rows as normalized: a `UInt32` array.
    pub(crate) row: ArrayRef,
    /// The position of each row's root among the stream's rows the batch holds: a `UInt32`
    /// array.
    pub(crate) root_row: ArrayRef,
    /// Each row's position in its parent's array: an `Int32` array.
    pub(crate) idx: ArrayRef,
}

/// Where a piece of a stream's rows was received: what the positional ids of its rows begin with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Received {
    /// The epoch of the session that wrote it.
    pub(crate) epoch: Epoch,
    /// The segment it was written to.
    pub(crate) segment: SegmentId,
    /// The position of its first root row among the rows the segment received.
    pub(crate) first_row: u64,
}

/// A part's lineage columns: each row's id, and a child row's parent's id, root's id and
/// position in its parent's array.
pub(crate) type Columns = (ArrayRef, Option<[ArrayRef; 3]>);

impl Lineage {
    /// The position of each row's root among the stream's rows the batch holds: a root row's own.
    pub(crate) fn root_row(&self) -> &ArrayRef {
        self.parent
            .as_ref()
            .map_or(&self.row, |parent| &parent.root_row)
    }

    /// The arrays holding a value for each of the part's rows.
    pub(crate) fn arrays(&self) -> Vec<&ArrayRef> {
        let mut arrays = vec![&self.row];
        if let Some(parent) = &self.parent {
            arrays.extend([&parent.row, &parent.root_row, &parent.idx]);
        }
        arrays
    }

    /// The lineage of the `count` rows from `first`.
    pub(crate) fn slice(&self, first: usize, count: usize) -> Self {
        self.each(|array| Ok(array.slice(first, count)))
            .expect("slicing an array within its rows fails nothing")
    }

    /// The lineage of the rows `predicate` keeps.
    pub(crate) fn filter(&self, predicate: &FilterPredicate) -> Result<Self, ArrowError> {
        self.each(|array| predicate.filter(array.as_ref()))
    }

    /// This lineage with each array holding a value for each of its rows made anew by `each`.
    fn each(
        &self,
        each: impl Fn(&ArrayRef) -> Result<ArrayRef, ArrowError>,
    ) -> Result<Self, ArrowError> {
        let parent = match &self.parent {
            Some(parent) => Some(Parent {
                path: parent.path.clone(),
                part: parent.part,
                row: each(&parent.row)?,
                root_row: each(&parent.root_row)?,
                idx: each(&parent.idx)?,
            }),
            None => None,
        };
        Ok(Self {
            part: self.part,
            row: each(&self.row)?,
            keys: self.keys.clone(),
            parent,
        })
    }

    /// The lineage columns of the rows, of a piece received at `received`.
    ///
    /// A root row's id is its key's, where a merge identifies roots by key, and else that of its
    /// position, as every other row's is.
    pub(crate) fn columns(&self, received: &Received) -> Result<Columns, ArrowError> {
        let prefix = received.prefix();
        let ids = |part: u32, rows: &ArrayRef| -> Result<ArrayRef, ArrowError> {
            match (&self.keys, part) {
                (Some(keys), 0) => arrow_select::take::take(keys.as_ref(), rows.as_ref(), None),
                _ => Ok(Arc::new(positional(&prefix, part, rows.as_primitive()))),
            }
        };
        let id = ids(self.part, &self.row)?;
        let parent = match &self.parent {
            Some(parent) => Some([
                ids(parent.part, &parent.row)?,
                ids(0, &parent.root_row)?,
                Arc::clone(&parent.idx),
            ]),
            None => None,
        };
        Ok((id, parent))
    }
}

impl Received {
    /// The bytes every positional id of the piece's rows begins with.
    fn prefix(&self) -> Vec<u8> {
        let mut prefix = Vec::with_capacity(3 * varint::len(u64::MAX));
        for value in [self.epoch.0, self.segment.0, self.first_row] {
            varint::write(value, &mut prefix);
        }
        prefix
    }
}

/// The ids of the rows at `rows` of the part `part`, of a piece whose ids begin with `prefix`.
fn positional(prefix: &[u8], part: u32, rows: &UInt32Array) -> BinaryArray {
    let mut id = prefix.to_vec();
    varint::write(part.into(), &mut id);
    let head = id.len();
    let bytes = rows
        .values()
        .iter()
        .map(|row| head + varint::len((*row).into()))
        .sum();
    let mut ids = BinaryBuilder::with_capacity(rows.len(), bytes);
    for row in rows.values() {
        id.truncate(head);
        varint::write((*row).into(), &mut id);
        ids.append_value(&id);
    }
    ids.finish()
}
