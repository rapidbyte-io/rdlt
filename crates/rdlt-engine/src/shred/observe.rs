//! What a push's values say about each column's type, joined as values are seen, and what they
//! take to build: a list's items are counted, so each level of a nested column has its rows.

use std::collections::BTreeMap;
use std::sync::Arc;

use rdlt_connector::{DecimalType, Field, Fields, LogicalType};

use crate::table::EXACT_IN_FLOAT;

/// The values a column held, as a type the lattice then names.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Observed {
    /// Only nulls, or no value yet.
    Null,
    /// Booleans.
    Bool,
    /// Integers that fit a signed 64-bit integer; `exact` while every one is exact as a float.
    Int {
        /// Whether every integer seen is exact as a 64-bit float.
        exact: bool,
    },
    /// Integers some of which only fit an unsigned 64-bit integer.
    Wide,
    /// Integers some of which are beyond 64 bits, all within 38 digits.
    Huge,
    /// Integers some of which are beyond 38 digits, all within 76.
    Vast,
    /// Floats, with integers exact as floats.
    Float,
    /// Strings.
    Text,
    /// Objects, with the fields seen in any of them.
    Object(Shape),
    /// Arrays: the join of their items, and how many items they held together.
    Array(Box<Observed>, u64),
    /// Values of kinds no narrower type holds together.
    Json,
}

/// The fields objects of one column held, in the order first seen.
#[derive(Clone, Debug, Default)]
pub(crate) struct Shape {
    fields: Vec<(Arc<str>, Observed)>,
    /// Each field's position, and the object of the observing parse that last named it; 0 for
    /// none.
    index: BTreeMap<Arc<str>, (usize, u64)>,
}

impl PartialEq for Shape {
    fn eq(&self, other: &Self) -> bool {
        self.fields == other.fields
    }
}

impl Shape {
    /// The fields, in the order first seen.
    pub(crate) fn fields(&self) -> &[(Arc<str>, Observed)] {
        &self.fields
    }

    /// What the field `name` held, if the shape has it.
    pub(crate) fn get(&self, name: &str) -> Option<&Observed> {
        self.position(name).map(|position| &self.fields[position].1)
    }

    /// The position of the field `name`, if the shape has it.
    pub(crate) fn position(&self, name: &str) -> Option<usize> {
        self.index.get(name).map(|&(position, _)| position)
    }

    /// Adds the field `name`, new to the shape, observed as `observed`.
    pub(crate) fn push(&mut self, name: Arc<str>, observed: Observed) {
        self.index.insert(Arc::clone(&name), (self.fields.len(), 0));
        self.fields.push((name, observed));
    }

    /// Notes that `object`, numbered by the parse observing the shape, names the field `name`:
    /// the field's position, if the shape has it, and whether `object` named it before.
    pub(crate) fn name(&mut self, name: &str, object: u64) -> Option<(usize, bool)> {
        let (position, named) = self.index.get_mut(name)?;
        Some((*position, std::mem::replace(named, object) == object))
    }

    /// Adds the field `name`, new to the shape, as `object` names it: its position.
    pub(crate) fn push_named(&mut self, name: Arc<str>, object: u64) -> usize {
        let position = self.fields.len();
        self.index.insert(Arc::clone(&name), (position, object));
        self.fields.push((name, Observed::Null));
        position
    }

    /// What the field at `position` held, to observe more values in.
    pub(crate) fn field_mut(&mut self, position: usize) -> &mut Observed {
        &mut self.fields[position].1
    }

    /// Joins `other` into this shape; fields new to it follow its own, in `other`'s order.
    pub(crate) fn join(&mut self, other: &Self) {
        for (name, observed) in &other.fields {
            match self.index.get(name) {
                Some(&(position, _)) => self.fields[position].1.join(observed),
                None => self.push(Arc::clone(name), observed.clone()),
            }
        }
    }

    /// How many columns the shape has, as a schema's columns are counted: each field at any
    /// depth, and each list's items.
    pub(crate) fn columns(&self) -> u64 {
        self.fields
            .iter()
            .map(|(_, observed)| 1 + observed.below())
            .fold(0, u64::saturating_add)
    }

    /// The shape's fields as logical fields, every one nullable.
    pub(crate) fn logical_fields(&self) -> Vec<Field> {
        self.fields
            .iter()
            .map(|(name, observed)| Field::new(Arc::clone(name), observed.logical_type(), true))
            .collect()
    }

    /// Whether any field holds a float, at any depth.
    pub(crate) fn floats(&self) -> bool {
        self.fields.iter().any(|(_, observed)| observed.floats())
    }
}

impl Observed {
    /// What the integer `value` is observed as.
    pub(crate) fn integer(value: i64) -> Self {
        Self::Int {
            exact: value.unsigned_abs() <= EXACT_IN_FLOAT,
        }
    }

    /// Joins `other` into this observation.
    pub(crate) fn join(&mut self, other: &Self) {
        let joined = match (&mut *self, other) {
            (_, Self::Null) => return,
            (Self::Null, _) => other.clone(),
            (Self::Object(shape), Self::Object(more)) => {
                shape.join(more);
                return;
            }
            (Self::Array(item, items), Self::Array(more, count)) => {
                item.join(more);
                *items = items.saturating_add(*count);
                return;
            }
            (Self::Int { exact }, Self::Int { exact: more }) => Self::Int {
                exact: *exact && *more,
            },
            (Self::Int { exact: true }, Self::Float)
            | (Self::Float, Self::Int { exact: true } | Self::Float) => Self::Float,
            (Self::Int { .. } | Self::Wide, Self::Int { .. } | Self::Wide) => Self::Wide,
            (
                Self::Int { .. } | Self::Wide | Self::Huge,
                Self::Int { .. } | Self::Wide | Self::Huge,
            ) => Self::Huge,
            (
                Self::Int { .. } | Self::Wide | Self::Huge | Self::Vast,
                Self::Int { .. } | Self::Wide | Self::Huge | Self::Vast,
            ) => Self::Vast,
            (current, _) if current == other => return,
            _ => Self::Json,
        };
        *self = joined;
    }

    /// How many columns the values hold below their own: an object's fields, a list's items.
    fn below(&self) -> u64 {
        match self {
            Self::Object(shape) => shape.columns(),
            Self::Array(item, _) => 1 + item.below(),
            _ => 0,
        }
    }

    /// Whether the values hold a float, at any depth.
    pub(crate) fn floats(&self) -> bool {
        match self {
            Self::Float => true,
            Self::Object(shape) => shape.floats(),
            Self::Array(item, _) => item.floats(),
            _ => false,
        }
    }

    /// The logical type naming the values observed.
    pub(crate) fn logical_type(&self) -> LogicalType {
        match self {
            Self::Null => LogicalType::Null,
            Self::Bool => LogicalType::Bool,
            Self::Int { .. } => LogicalType::Int64,
            Self::Wide => {
                LogicalType::Decimal(DecimalType::new(20, 0).expect("20 digits fit a decimal"))
            }
            Self::Huge => {
                LogicalType::Decimal(DecimalType::new(38, 0).expect("38 digits fit a decimal"))
            }
            Self::Vast => {
                LogicalType::Decimal(DecimalType::new(76, 0).expect("76 digits fit a decimal"))
            }
            Self::Float => LogicalType::Float64,
            Self::Text => LogicalType::Utf8,
            Self::Object(shape) => LogicalType::Struct(
                Fields::new(shape.logical_fields()).expect("a shape's field names are distinct"),
            ),
            Self::Array(item, _) => {
                LogicalType::List(Box::new(Field::new("item", item.logical_type(), true)))
            }
            Self::Json => LogicalType::Json,
        }
    }
}
