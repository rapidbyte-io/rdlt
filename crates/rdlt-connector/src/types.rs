//! Logical types: what a value is, independent of how Arrow or a destination stores it.

mod arrow;
mod lattice;
#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

pub use arrow::UnsupportedType;

/// The largest decimal precision, in digits.
pub const MAX_DECIMAL_PRECISION: u8 = 76;

/// A value's logical type.
///
/// Types form a lattice under [`LogicalType::join`]: `Null` is the bottom and `Json` the top.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogicalType {
    /// Only nulls seen so far.
    Null,
    /// `true` or `false`.
    Bool,
    /// Signed 8-bit integer.
    Int8,
    /// Signed 16-bit integer.
    Int16,
    /// Signed 32-bit integer.
    Int32,
    /// Signed 64-bit integer.
    Int64,
    /// 32-bit float.
    Float32,
    /// 64-bit float.
    Float64,
    /// Exact decimal.
    Decimal(DecimalType),
    /// UTF-8 text.
    Utf8,
    /// Bytes.
    Binary,
    /// Calendar date.
    Date,
    /// Time of day.
    Time(TimeUnit),
    /// Instant, with an optional time zone; no zone means a wall-clock timestamp.
    Timestamp(TimeUnit, Option<Arc<str>>),
    /// Elapsed time.
    Duration(TimeUnit),
    /// A 16-byte UUID.
    Uuid,
    /// Any JSON value.
    Json,
    /// Named fields.
    Struct(Fields),
    /// A list of values of one type.
    List(Box<Field>),
}

/// A logical type without its parameters, for declaring what a destination stores natively.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TypeKind {
    /// [`LogicalType::Null`].
    Null,
    /// [`LogicalType::Bool`].
    Bool,
    /// [`LogicalType::Int8`].
    Int8,
    /// [`LogicalType::Int16`].
    Int16,
    /// [`LogicalType::Int32`].
    Int32,
    /// [`LogicalType::Int64`].
    Int64,
    /// [`LogicalType::Float32`].
    Float32,
    /// [`LogicalType::Float64`].
    Float64,
    /// [`LogicalType::Decimal`].
    Decimal,
    /// [`LogicalType::Utf8`].
    Utf8,
    /// [`LogicalType::Binary`].
    Binary,
    /// [`LogicalType::Date`].
    Date,
    /// [`LogicalType::Time`].
    Time,
    /// [`LogicalType::Timestamp`].
    Timestamp,
    /// [`LogicalType::Duration`].
    Duration,
    /// [`LogicalType::Uuid`].
    Uuid,
    /// [`LogicalType::Json`].
    Json,
    /// [`LogicalType::Struct`].
    Struct,
    /// [`LogicalType::List`].
    List,
}

impl LogicalType {
    /// The type's kind.
    pub fn kind(&self) -> TypeKind {
        match self {
            Self::Null => TypeKind::Null,
            Self::Bool => TypeKind::Bool,
            Self::Int8 => TypeKind::Int8,
            Self::Int16 => TypeKind::Int16,
            Self::Int32 => TypeKind::Int32,
            Self::Int64 => TypeKind::Int64,
            Self::Float32 => TypeKind::Float32,
            Self::Float64 => TypeKind::Float64,
            Self::Decimal(_) => TypeKind::Decimal,
            Self::Utf8 => TypeKind::Utf8,
            Self::Binary => TypeKind::Binary,
            Self::Date => TypeKind::Date,
            Self::Time(_) => TypeKind::Time,
            Self::Timestamp(..) => TypeKind::Timestamp,
            Self::Duration(_) => TypeKind::Duration,
            Self::Uuid => TypeKind::Uuid,
            Self::Json => TypeKind::Json,
            Self::Struct(_) => TypeKind::Struct,
            Self::List(_) => TypeKind::List,
        }
    }
}

/// The resolution of a time-based type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeUnit {
    /// Seconds.
    Second,
    /// Milliseconds.
    Millisecond,
    /// Microseconds.
    Microsecond,
    /// Nanoseconds.
    Nanosecond,
}

/// Precision and scale of an exact decimal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "RawDecimal", into = "RawDecimal")]
pub struct DecimalType {
    precision: u8,
    scale: u8,
}

#[derive(Serialize, Deserialize)]
struct RawDecimal {
    precision: u8,
    scale: u8,
}

/// Why a type could not be built.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TypeError {
    /// A decimal's precision or scale is out of range.
    #[error("decimal({precision}, {scale}) needs 1 <= precision <= 76 and scale <= precision")]
    Decimal {
        /// The requested precision.
        precision: u8,
        /// The requested scale.
        scale: u8,
    },
    /// Two fields of one struct or schema share a name.
    #[error("field {name:?} appears more than once")]
    DuplicateField {
        /// The repeated name.
        name: String,
    },
}

impl DecimalType {
    /// A decimal with `precision` digits, `scale` of them after the point.
    pub fn new(precision: u8, scale: u8) -> Result<Self, TypeError> {
        if precision == 0 || precision > MAX_DECIMAL_PRECISION || scale > precision {
            return Err(TypeError::Decimal { precision, scale });
        }
        Ok(Self { precision, scale })
    }

    /// Total digits.
    pub fn precision(self) -> u8 {
        self.precision
    }

    /// Digits after the decimal point.
    pub fn scale(self) -> u8 {
        self.scale
    }
}

impl TryFrom<RawDecimal> for DecimalType {
    type Error = TypeError;

    fn try_from(raw: RawDecimal) -> Result<Self, Self::Error> {
        Self::new(raw.precision, raw.scale)
    }
}

impl From<DecimalType> for RawDecimal {
    fn from(decimal: DecimalType) -> Self {
        Self {
            precision: decimal.precision,
            scale: decimal.scale,
        }
    }
}

/// A named, typed, possibly nullable value slot.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Field {
    name: Arc<str>,
    #[serde(rename = "type")]
    logical_type: LogicalType,
    nullable: bool,
}

impl Field {
    /// A field called `name`.
    pub fn new(name: impl Into<Arc<str>>, logical_type: LogicalType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            logical_type,
            nullable,
        }
    }

    /// The field's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The field's type.
    pub fn logical_type(&self) -> &LogicalType {
        &self.logical_type
    }

    /// Whether the field may hold nulls.
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }
}

/// Fields with distinct names, in order.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "Vec<Field>", into = "Vec<Field>")]
pub struct Fields(Vec<Field>);

impl Fields {
    /// Validates that `fields` have distinct names.
    pub fn new(fields: Vec<Field>) -> Result<Self, TypeError> {
        let mut seen = BTreeSet::new();
        if let Some(duplicate) = fields.iter().find(|field| !seen.insert(field.name())) {
            return Err(TypeError::DuplicateField {
                name: duplicate.name().to_owned(),
            });
        }
        Ok(Self(fields))
    }

    /// The fields, in order.
    pub fn iter(&self) -> impl Iterator<Item = &Field> {
        self.0.iter()
    }

    /// The field called `name`.
    pub fn get(&self, name: &str) -> Option<&Field> {
        self.0.iter().find(|field| field.name() == name)
    }

    /// The number of fields.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether there are no fields.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl TryFrom<Vec<Field>> for Fields {
    type Error = TypeError;

    fn try_from(fields: Vec<Field>) -> Result<Self, Self::Error> {
        Self::new(fields)
    }
}

impl From<Fields> for Vec<Field> {
    fn from(fields: Fields) -> Self {
        fields.0
    }
}

impl fmt::Display for LogicalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decimal(decimal) => {
                write!(f, "decimal({}, {})", decimal.precision, decimal.scale)
            }
            Self::Time(unit) => write!(f, "time({unit:?})"),
            Self::Timestamp(unit, Some(zone)) => write!(f, "timestamp({unit:?}, {zone})"),
            Self::Timestamp(unit, None) => write!(f, "timestamp({unit:?})"),
            Self::Duration(unit) => write!(f, "duration({unit:?})"),
            Self::Struct(fields) => {
                let names: Vec<String> = fields
                    .iter()
                    .map(|field| format!("{}: {}", field.name(), field.logical_type()))
                    .collect();
                write!(f, "struct<{}>", names.join(", "))
            }
            Self::List(item) => write!(f, "list<{}>", item.logical_type()),
            other => write!(f, "{}", format!("{other:?}").to_lowercase()),
        }
    }
}
