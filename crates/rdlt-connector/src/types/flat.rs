//! The stored form of logical types: a type's nodes in preorder, as the wire's type nodes are, so
//! the form nests no deeper however deep the type.
//!
//! A struct's node says how many fields follow it, and a list's is followed by its item's. A
//! field's node carries its name and nullability; a type's own root carries neither. A form is
//! read with recursion bounded by the nesting limit, counting the root as the first level.

use std::sync::Arc;

use serde::de::Error as _;
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{DecimalType, Field, Fields, LogicalType, TimeUnit};
use rdlt_wire::limits::NESTING_DEPTH;

#[cfg(test)]
mod tests;

/// One node of a stored type.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Node {
    /// The field's name; none for a type's root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    name: Option<Arc<str>>,
    /// Whether the field may hold nulls; none for a type's root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    nullable: Option<bool>,
    #[serde(rename = "type")]
    kind: Kind,
}

/// A node's type, without the types nested in it.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Kind {
    Null,
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal(DecimalType),
    Utf8,
    Binary,
    Date,
    Time(TimeUnit),
    Timestamp(TimeUnit, Option<Arc<str>>),
    Duration(TimeUnit),
    Uuid,
    Json,
    /// A struct, and how many fields follow its node.
    Struct(usize),
    /// A list, whose item's node follows.
    List,
}

impl Kind {
    fn of(logical: &LogicalType) -> Self {
        use LogicalType as T;
        match logical {
            T::Null => Self::Null,
            T::Bool => Self::Bool,
            T::Int8 => Self::Int8,
            T::Int16 => Self::Int16,
            T::Int32 => Self::Int32,
            T::Int64 => Self::Int64,
            T::Float32 => Self::Float32,
            T::Float64 => Self::Float64,
            T::Decimal(decimal) => Self::Decimal(*decimal),
            T::Utf8 => Self::Utf8,
            T::Binary => Self::Binary,
            T::Date => Self::Date,
            T::Time(unit) => Self::Time(*unit),
            T::Timestamp(unit, zone) => Self::Timestamp(*unit, zone.clone()),
            T::Duration(unit) => Self::Duration(*unit),
            T::Uuid => Self::Uuid,
            T::Json => Self::Json,
            T::Struct(fields) => Self::Struct(fields.len()),
            T::List(_) => Self::List,
        }
    }
}

/// Serializes the nodes of `logical`, its root named as `root` says, without recursion.
fn nodes<S: Serializer>(
    root: Option<(&Arc<str>, bool)>,
    logical: &LogicalType,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let mut seq = serializer.serialize_seq(None)?;
    let mut pending = vec![(root, logical)];
    while let Some((named, logical)) = pending.pop() {
        seq.serialize_element(&Node {
            name: named.map(|(name, _)| Arc::clone(name)),
            nullable: named.map(|(_, nullable)| nullable),
            kind: Kind::of(logical),
        })?;
        match logical {
            LogicalType::Struct(fields) => pending.extend(fields.0.iter().rev().map(below)),
            LogicalType::List(item) => pending.push(below(item)),
            _ => {}
        }
    }
    seq.end()
}

/// What a node and the nodes below it describe: a field's name and nullability, where the root is
/// a field's, and its type.
type Read = (Option<(Arc<str>, bool)>, LogicalType);

/// A field as a node's naming and the type its nodes describe.
fn below(field: &Field) -> (Option<(&Arc<str>, bool)>, &LogicalType) {
    (Some((&field.name, field.nullable)), &field.logical_type)
}

/// Reads the field or type the next node of `nodes` begins, at `depth` levels of nesting.
fn read<E: serde::de::Error>(nodes: &mut std::vec::IntoIter<Node>, depth: u64) -> Result<Read, E> {
    use LogicalType as T;
    if depth > NESTING_DEPTH {
        return Err(E::custom(format!(
            "a stored type nests beyond the limit of {NESTING_DEPTH} levels"
        )));
    }
    let node = nodes
        .next()
        .ok_or_else(|| E::custom("a stored type ends before the nodes it names"))?;
    let named = match (node.name, node.nullable) {
        (Some(name), Some(nullable)) => Some((name, nullable)),
        (None, None) => None,
        _ => return Err(E::custom("a stored type's node names a field by half")),
    };
    let field = |nodes: &mut _| match read::<E>(nodes, depth.saturating_add(1))? {
        (Some((name, nullable)), logical) => Ok(Field::new(name, logical, nullable)),
        (None, _) => Err(E::custom("a stored type's field has no name")),
    };
    let logical = match node.kind {
        Kind::Null => T::Null,
        Kind::Bool => T::Bool,
        Kind::Int8 => T::Int8,
        Kind::Int16 => T::Int16,
        Kind::Int32 => T::Int32,
        Kind::Int64 => T::Int64,
        Kind::Float32 => T::Float32,
        Kind::Float64 => T::Float64,
        Kind::Decimal(decimal) => T::Decimal(decimal),
        Kind::Utf8 => T::Utf8,
        Kind::Binary => T::Binary,
        Kind::Date => T::Date,
        Kind::Time(unit) => T::Time(unit),
        Kind::Timestamp(unit, zone) => T::Timestamp(unit, zone),
        Kind::Duration(unit) => T::Duration(unit),
        Kind::Uuid => T::Uuid,
        Kind::Json => T::Json,
        Kind::Struct(count) => {
            // Each field takes a node at least, so no more are read than are there.
            let fields = (0..count.min(nodes.len()))
                .map(|_| field(nodes))
                .collect::<Result<Vec<_>, E>>()?;
            if fields.len() < count {
                return Err(E::custom("a stored type ends before the nodes it names"));
            }
            T::Struct(Fields::new(fields).map_err(E::custom)?)
        }
        Kind::List => T::List(Box::new(field(nodes)?)),
    };
    Ok((named, logical))
}

/// Reads one field or type from the whole of `nodes`, refusing nodes left over.
fn whole<E: serde::de::Error>(nodes: Vec<Node>) -> Result<Read, E> {
    let mut nodes = nodes.into_iter();
    let read = read::<E>(&mut nodes, 1)?;
    if !nodes.as_slice().is_empty() {
        return Err(E::custom("a stored type has nodes beyond its own"));
    }
    Ok(read)
}

impl Serialize for LogicalType {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        nodes(None, self, serializer)
    }
}

impl<'de> Deserialize<'de> for LogicalType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match whole(Vec::<Node>::deserialize(deserializer)?)? {
            (None, logical) => Ok(logical),
            (Some(_), _) => Err(D::Error::custom("a stored type's root names a field")),
        }
    }
}

impl Serialize for Field {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        nodes(
            Some((&self.name, self.nullable)),
            &self.logical_type,
            serializer,
        )
    }
}

impl<'de> Deserialize<'de> for Field {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match whole(Vec::<Node>::deserialize(deserializer)?)? {
            (Some((name, nullable)), logical) => Ok(Self::new(name, logical, nullable)),
            (None, _) => Err(D::Error::custom("a stored field has no name")),
        }
    }
}
