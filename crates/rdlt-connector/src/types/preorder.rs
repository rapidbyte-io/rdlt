//! A logical type as its nodes in preorder, the form both its stored form and the wire carry: a
//! struct's node says how many fields follow it, and a list's is followed by its item's.
//!
//! Writing walks the type without recursion. Reading recurses a level a node, refused before it
//! passes the nesting limit, counting the root as the first level, and reads a struct's fields
//! only while nodes are left, so a node claiming more fields than follow reserves nothing for
//! them.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use rdlt_wire::limits::NESTING_DEPTH;

use super::{Field, Fields, LogicalType, TypeError};

/// A field's name and whether it may hold nulls; none for a type's root.
pub(crate) type Named<'a> = Option<(&'a Arc<str>, bool)>;

/// What one node read says of its type, without the types nested in it.
pub(crate) enum Shape {
    /// A type holding no other.
    Leaf(LogicalType),
    /// A struct, and how many fields follow its node.
    Struct(usize),
    /// A list, whose item's node follows.
    List,
}

/// One node read: its field's name and nullability, none for a type's root, and its shape.
pub(crate) type Node = (Option<(Arc<str>, bool)>, Shape);

/// A field's name and nullability, none for a type's root, and the type its nodes describe.
pub(crate) type Read = (Option<(Arc<str>, bool)>, LogicalType);

/// Why nodes in preorder are no one type, or the error the reader of a node gave.
#[derive(Debug)]
pub(crate) enum Unread<E> {
    /// A node sits deeper than the nesting limit.
    Deep,
    /// The nodes end before those a node names.
    Ended,
    /// Nodes follow the type's own.
    Beyond,
    /// A field's node names no field.
    Unnamed,
    /// A struct's fields are no fields of one struct.
    Fields(TypeError),
    /// The reader refused a node.
    Node(E),
}

impl<E: std::fmt::Display> std::fmt::Display for Unread<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Deep => write!(f, "a type nests beyond the limit of {NESTING_DEPTH} levels"),
            Self::Ended => f.write_str("a type ends before the nodes it names"),
            Self::Beyond => f.write_str("a type has nodes beyond its own"),
            Self::Unnamed => f.write_str("a type's field has no name"),
            Self::Fields(error) => error.fmt(f),
            Self::Node(error) => error.fmt(f),
        }
    }
}

/// The nodes of `logical`, its root named as `root` says, in preorder.
pub(crate) fn nodes<'a>(
    root: Named<'a>,
    logical: &'a LogicalType,
) -> impl Iterator<Item = (Named<'a>, &'a LogicalType)> + 'a {
    let mut pending = vec![(root, logical)];
    std::iter::from_fn(move || {
        let (named, logical) = pending.pop()?;
        match logical {
            LogicalType::Struct(fields) => pending.extend(fields.0.iter().rev().map(below)),
            LogicalType::List(item) => pending.push(below(item)),
            _ => {}
        }
        Some((named, logical))
    })
}

/// Reads one type, and its root's naming, from the whole of `nodes`, each node by `read`.
///
/// # Errors
///
/// An [`Unread`] for nodes that are no one type, or the first error `read` gives.
pub(crate) fn whole<N, E>(
    nodes: Vec<N>,
    read: impl Fn(N) -> Result<Node, E>,
) -> Result<Read, Unread<E>> {
    let mut nodes = nodes.into_iter();
    let whole = next(&mut nodes, &read, 1)?;
    if nodes.len() > 0 {
        return Err(Unread::Beyond);
    }
    Ok(whole)
}

/// A field as a node's naming and the type its nodes describe.
fn below(field: &Field) -> (Named<'_>, &LogicalType) {
    (Some((&field.name, field.nullable)), &field.logical_type)
}

/// Reads the field or type the next node of `nodes` begins, at `depth` levels of nesting.
fn next<N, E>(
    nodes: &mut std::vec::IntoIter<N>,
    read: &impl Fn(N) -> Result<Node, E>,
    depth: u64,
) -> Result<Read, Unread<E>> {
    if depth > NESTING_DEPTH {
        return Err(Unread::Deep);
    }
    let node = nodes.next().ok_or(Unread::Ended)?;
    let (named, shape) = read(node).map_err(Unread::Node)?;
    let field = |nodes: &mut std::vec::IntoIter<N>| match next(nodes, read, depth + 1)? {
        (Some((name, nullable)), logical) => Ok(Field::new(name, logical, nullable)),
        (None, _) => Err(Unread::Unnamed),
    };
    let logical = match shape {
        Shape::Leaf(logical) => logical,
        Shape::Struct(count) => {
            // Each field takes a node at least, so no more are read than are there.
            let fields = (0..count.min(nodes.len()))
                .map(|_| field(nodes))
                .collect::<Result<Vec<_>, _>>()?;
            if fields.len() < count {
                return Err(Unread::Ended);
            }
            LogicalType::Struct(Fields::new(fields).map_err(Unread::Fields)?)
        }
        Shape::List => LogicalType::List(Box::new(field(nodes)?)),
    };
    Ok((named, logical))
}
