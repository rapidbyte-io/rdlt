//! The slab arena: the JSON path's representation. [`slab`] is the flat
//! node store and the borrowed [`view::JsonView`](super::view::JsonView) over
//! it; [`parse`] lands a pushed slab into it through serde's own parser under
//! the row, value and depth budgets.

pub(crate) mod parse;
pub(crate) mod slab;

pub(crate) use parse::ParseRowsError;
pub(crate) use slab::{Arena, Node, NodeId};
