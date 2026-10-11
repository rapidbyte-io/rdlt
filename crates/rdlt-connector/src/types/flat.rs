//! The stored form of logical types: a type's nodes in preorder, as the wire's type nodes are, so
//! the form nests no deeper however deep the type.
//!
//! A struct's node says how many fields follow it, and a list's is followed by its item's. A
//! field's node carries its name and nullability; a type's own root carries neither.

use std::sync::Arc;

use serde::de::Error as _;
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::preorder::{self, Shape, Unread};
use super::{DecimalType, Field, LogicalType, TimeUnit};

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

/// A stored node that gives a name or a nullability, but not both.
#[derive(Debug, thiserror::Error)]
#[error("a stored type's node names a field by half")]
struct HalfNamed;

impl Node {
    /// The node's naming and shape, as the shared reader reads them.
    fn read(self) -> Result<preorder::Node, HalfNamed> {
        use LogicalType as T;
        let named = match (self.name, self.nullable) {
            (Some(name), Some(nullable)) => Some((name, nullable)),
            (None, None) => None,
            _ => return Err(HalfNamed),
        };
        let shape = match self.kind {
            Kind::Null => Shape::Leaf(T::Null),
            Kind::Bool => Shape::Leaf(T::Bool),
            Kind::Int8 => Shape::Leaf(T::Int8),
            Kind::Int16 => Shape::Leaf(T::Int16),
            Kind::Int32 => Shape::Leaf(T::Int32),
            Kind::Int64 => Shape::Leaf(T::Int64),
            Kind::Float32 => Shape::Leaf(T::Float32),
            Kind::Float64 => Shape::Leaf(T::Float64),
            Kind::Decimal(decimal) => Shape::Leaf(T::Decimal(decimal)),
            Kind::Utf8 => Shape::Leaf(T::Utf8),
            Kind::Binary => Shape::Leaf(T::Binary),
            Kind::Date => Shape::Leaf(T::Date),
            Kind::Time(unit) => Shape::Leaf(T::Time(unit)),
            Kind::Timestamp(unit, zone) => Shape::Leaf(T::Timestamp(unit, zone)),
            Kind::Duration(unit) => Shape::Leaf(T::Duration(unit)),
            Kind::Uuid => Shape::Leaf(T::Uuid),
            Kind::Json => Shape::Leaf(T::Json),
            Kind::Struct(count) => Shape::Struct(count),
            Kind::List => Shape::List,
        };
        Ok((named, shape))
    }
}

/// Serializes the nodes of `logical`, its root named as `root` says.
fn serialized<S: Serializer>(
    root: preorder::Named<'_>,
    logical: &LogicalType,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let mut seq = serializer.serialize_seq(None)?;
    for (named, logical) in preorder::nodes(root, logical) {
        seq.serialize_element(&Node {
            name: named.map(|(name, _)| Arc::clone(name)),
            nullable: named.map(|(_, nullable)| nullable),
            kind: Kind::of(logical),
        })?;
    }
    seq.end()
}

/// Reads one field or type from stored `nodes`.
fn deserialized<E: serde::de::Error>(nodes: Vec<Node>) -> Result<preorder::Read, E> {
    preorder::whole(nodes, Node::read).map_err(|unread: Unread<HalfNamed>| E::custom(unread))
}

impl Serialize for LogicalType {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serialized(None, self, serializer)
    }
}

impl<'de> Deserialize<'de> for LogicalType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match deserialized(Vec::<Node>::deserialize(deserializer)?)? {
            (None, logical) => Ok(logical),
            (Some(_), _) => Err(D::Error::custom("a stored type's root names a field")),
        }
    }
}

impl Serialize for Field {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serialized(
            Some((&self.name, self.nullable)),
            &self.logical_type,
            serializer,
        )
    }
}

impl<'de> Deserialize<'de> for Field {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match deserialized(Vec::<Node>::deserialize(deserializer)?)? {
            (Some((name, nullable)), logical) => Ok(Self::new(name, logical, nullable)),
            (None, _) => Err(D::Error::custom("a stored field has no name")),
        }
    }
}
