//! What building a chunk's records against the push's shape takes, from what the chunk was
//! observed to hold, before anything is built: its cells, and the bytes its batch holds.
//!
//! Each level of a nested column has its own rows: a struct's fields the rows of the struct, a
//! list's items as many as the chunk's arrays held. A cell is a row under a column holding values,
//! at any level.
//!
//! What grows with the rows, the items and the text is reckoned from the counts; a column's fixed
//! parts, its entry in a shape, its builder or array and its buffers' rounding to 64 bytes, are
//! reckoned a column a chunk.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use super::meter::{ARRAY, KEY, Meter, OBJECT_SHAPE, RECORD};
use super::observe::{Observed, Shape};

/// What a chunk's batch takes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Size {
    /// Rows under columns holding values, at every level.
    pub(crate) cells: u64,
    /// Bytes the batch holds, but for the bytes of its text.
    pub(crate) bytes: u64,
}

impl Size {
    fn add(self, other: Self) -> Self {
        Self {
            cells: self.cells.saturating_add(other.cells),
            bytes: self.bytes.saturating_add(other.bytes),
        }
    }
}

/// What `rows` records observed as `local` take built against `joined`.
pub(crate) fn built(joined: &Shape, local: &Shape, rows: u64) -> Size {
    joined
        .fields()
        .iter()
        .map(|(name, observed)| node(observed, local.get(name), rows))
        .fold(Size::default(), Size::add)
}

/// Bytes the arrays fitting `rows` records' columns, built as `local`, to `joined` makes beside
/// them: the columns they lack or held only nulls in, built as nulls, and integers cast.
pub(crate) fn fitted(joined: &Shape, local: &Shape, rows: u64) -> u64 {
    joined
        .fields()
        .iter()
        .map(|(name, observed)| fitting(observed, local.get(name), rows))
        .fold(0, u64::saturating_add)
}

/// Bytes the fixed parts of a batch's columns of `joined` take, but for those of the columns
/// `local` already holds built: each column's array, or a struct's.
pub(crate) fn arrays(joined: &Shape, local: Option<&Shape>) -> u64 {
    joined
        .fields()
        .iter()
        .map(|(name, observed)| {
            let local = local.and_then(|local| local.get(name));
            arrays_node(observed, local)
        })
        .fold(0, u64::saturating_add)
}

fn arrays_node(joined: &Observed, local: Option<&Observed>) -> u64 {
    let own = |bytes: u64| if local.is_some() { 0 } else { bytes };
    match joined {
        Observed::Object(shape) => {
            let local = match local {
                Some(Observed::Object(local)) => Some(local),
                _ => None,
            };
            own(RECORD).saturating_add(arrays(shape, local))
        }
        Observed::Array(item, _) => {
            let local = match local {
                Some(Observed::Array(local, _)) => Some(local.as_ref()),
                _ => None,
            };
            own(ARRAY).saturating_add(arrays_node(item, local))
        }
        _ => own(ARRAY),
    }
}

/// Bytes a shape of `joined` takes: an entry a column, and a shape of its own an object.
pub(crate) fn shape(joined: &Shape) -> u64 {
    joined
        .fields()
        .iter()
        .map(|(name, observed)| Meter::key(name).saturating_add(shape_node(observed)))
        .fold(0, u64::saturating_add)
}

fn shape_node(observed: &Observed) -> u64 {
    match observed {
        Observed::Object(fields) => OBJECT_SHAPE.saturating_add(shape(fields)),
        Observed::Array(item, _) => KEY.saturating_add(shape_node(item)),
        _ => 0,
    }
}

/// Whether a column of `joined` that is JSON holds, in the records observed as `local`, a value
/// with a float in it: rendered as JSON text, it needs the float as it was written.
pub(crate) fn floats_in_json(joined: &Shape, local: &Shape) -> bool {
    joined.fields().iter().any(|(name, observed)| {
        local
            .get(name)
            .is_some_and(|local| node_floats_in_json(observed, local))
    })
}

/// Whether `joined` holds a column of text or of JSON at any depth.
pub(crate) fn holds_text(joined: &Shape) -> bool {
    joined
        .fields()
        .iter()
        .any(|(_, observed)| node_holds_text(observed))
}

/// `joined`, its lists sized for the items `local` held: the shape a chunk observed as `local`
/// is built again against.
pub(crate) fn sized(joined: &Shape, local: &Shape) -> Shape {
    let mut sized = Shape::default();
    for (name, observed) in joined.fields() {
        sized.push(Arc::clone(name), sized_node(observed, local.get(name)));
    }
    sized
}

fn sized_node(joined: &Observed, local: Option<&Observed>) -> Observed {
    match (joined, local) {
        (Observed::Object(joined), Some(Observed::Object(local))) => {
            Observed::Object(sized(joined, local))
        }
        (Observed::Object(joined), _) => Observed::Object(sized(joined, &Shape::default())),
        (Observed::Array(item, _), Some(Observed::Array(local, items))) => {
            Observed::Array(Box::new(sized_node(item, Some(local))), *items)
        }
        (Observed::Array(item, _), _) => Observed::Array(Box::new(sized_node(item, None)), 0),
        (joined, _) => joined.clone(),
    }
}

/// What `rows` values of a column of `joined`, observed as `local`, take built.
fn node(joined: &Observed, local: Option<&Observed>, rows: u64) -> Size {
    let validity = rows.div_ceil(8);
    let leaf = |slot: u64| Size {
        cells: rows,
        bytes: rows.saturating_mul(slot).saturating_add(validity),
    };
    match joined {
        Observed::Null => Size::default(),
        Observed::Bool => leaf(0).add(Size {
            cells: 0,
            bytes: validity,
        }),
        Observed::Int { .. } | Observed::Float => leaf(8),
        Observed::Wide | Observed::Huge => leaf(16),
        Observed::Vast => leaf(32),
        // Offsets; the text is the chunk's, counted once for all its columns.
        Observed::Text | Observed::Json => leaf(4).add(Size { cells: 0, bytes: 4 }),
        Observed::Object(shape) => {
            let empty = Shape::default();
            let local = match local {
                Some(Observed::Object(local)) => local,
                _ => &empty,
            };
            built(shape, local, rows).add(Size {
                cells: 0,
                bytes: validity,
            })
        }
        Observed::Array(item, _) => {
            let (local, items) = match local {
                Some(Observed::Array(local, items)) => (Some(local.as_ref()), *items),
                _ => (None, 0),
            };
            node(item, local, items).add(Size {
                cells: 0,
                bytes: rows
                    .saturating_add(1)
                    .saturating_mul(4)
                    .saturating_add(validity),
            })
        }
    }
}

fn fitting(joined: &Observed, local: Option<&Observed>, rows: u64) -> u64 {
    match (joined, local) {
        (_, None | Some(Observed::Null)) => node(joined, None, rows).bytes,
        (Observed::Object(joined), Some(Observed::Object(local))) => fitted(joined, local, rows),
        (Observed::Array(joined, _), Some(Observed::Array(local, items))) => {
            fitting(joined, Some(local), *items)
        }
        (Observed::Float | Observed::Wide, Some(Observed::Int { .. })) => {
            node(joined, local, rows).bytes
        }
        _ => 0,
    }
}

fn node_floats_in_json(joined: &Observed, local: &Observed) -> bool {
    match (joined, local) {
        (Observed::Json, local) => local.floats(),
        (Observed::Object(joined), Observed::Object(local)) => floats_in_json(joined, local),
        (Observed::Array(joined, _), Observed::Array(local, _)) => {
            node_floats_in_json(joined, local)
        }
        _ => false,
    }
}

fn node_holds_text(joined: &Observed) -> bool {
    match joined {
        Observed::Text | Observed::Json => true,
        Observed::Object(shape) => holds_text(shape),
        Observed::Array(item, _) => node_holds_text(item),
        _ => false,
    }
}
