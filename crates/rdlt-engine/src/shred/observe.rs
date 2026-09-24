//! What a push's values say about each column's type, joined as values are seen.

use std::collections::BTreeMap;
use std::sync::Arc;

use rdlt_connector::{DecimalType, Field, Fields, LogicalType};

/// Integers whose magnitude is at most this are exact as a 64-bit float.
const EXACT_IN_FLOAT: u64 = 1 << 53;

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
    /// Floats, with integers exact as floats.
    Float,
    /// Strings.
    Text,
    /// Objects, with the fields seen in any of them.
    Object(Shape),
    /// Arrays, with the join of their items.
    Array(Box<Observed>),
    /// Values of kinds no narrower type holds together.
    Json,
}

/// The fields objects of one column held, in the order first seen.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Shape {
    fields: Vec<(Arc<str>, Observed)>,
    index: BTreeMap<Arc<str>, usize>,
}

impl Shape {
    /// The fields, in the order first seen.
    pub(crate) fn fields(&self) -> &[(Arc<str>, Observed)] {
        &self.fields
    }

    /// How many fields the shape has.
    pub(crate) fn len(&self) -> usize {
        self.fields.len()
    }

    /// Adds the field `name`, new to the shape, observed as `observed`.
    pub(crate) fn push(&mut self, name: Arc<str>, observed: Observed) {
        self.index.insert(Arc::clone(&name), self.fields.len());
        self.fields.push((name, observed));
    }

    /// Joins `other` into this shape; fields new to it follow its own, in `other`'s order.
    pub(crate) fn join(&mut self, other: &Self) {
        for (name, observed) in &other.fields {
            match self.index.get(name) {
                Some(&position) => self.fields[position].1.join(observed),
                None => self.push(Arc::clone(name), observed.clone()),
            }
        }
    }

    /// The shape's fields as logical fields, every one nullable.
    pub(crate) fn logical_fields(&self) -> Vec<Field> {
        self.fields
            .iter()
            .map(|(name, observed)| Field::new(Arc::clone(name), observed.logical_type(), true))
            .collect()
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
            (Self::Array(item), Self::Array(more)) => {
                item.join(more);
                return;
            }
            (Self::Int { exact }, Self::Int { exact: more }) => Self::Int {
                exact: *exact && *more,
            },
            (Self::Int { exact: true }, Self::Float)
            | (Self::Float, Self::Int { exact: true } | Self::Float) => Self::Float,
            (Self::Int { .. } | Self::Wide, Self::Int { .. } | Self::Wide) => Self::Wide,
            (current, _) if current == other => return,
            _ => Self::Json,
        };
        *self = joined;
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
            Self::Float => LogicalType::Float64,
            Self::Text => LogicalType::Utf8,
            Self::Object(shape) => LogicalType::Struct(
                Fields::new(shape.logical_fields()).expect("a shape's field names are distinct"),
            ),
            Self::Array(item) => {
                LogicalType::List(Box::new(Field::new("item", item.logical_type(), true)))
            }
            Self::Json => LogicalType::Json,
        }
    }
}
