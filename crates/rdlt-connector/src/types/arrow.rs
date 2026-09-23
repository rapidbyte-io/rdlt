//! Mapping between logical types and Arrow types.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field as ArrowField, TimeUnit as ArrowTimeUnit};

use super::lattice::LIST_ITEM;
use super::{DecimalType, Field, Fields, LogicalType, TimeUnit};

/// The Arrow field metadata key naming an extension type.
const EXTENSION_NAME: &str = "ARROW:extension:name";
const UUID_EXTENSION: &str = "arrow.uuid";
const JSON_EXTENSION: &str = "arrow.json";

/// An Arrow type with no logical equivalent.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("field {field:?}: arrow type {data_type} has no logical equivalent")]
pub struct UnsupportedType {
    /// The field holding the type.
    pub field: String,
    /// The Arrow type, rendered.
    pub data_type: String,
}

impl Field {
    /// The Arrow field for this field; `Uuid` and `Json` carry Arrow's canonical extension names.
    pub fn to_arrow(&self) -> ArrowField {
        let field = ArrowField::new(
            self.name(),
            self.logical_type().to_arrow(),
            self.is_nullable(),
        );
        let extension = match self.logical_type() {
            LogicalType::Uuid => Some(UUID_EXTENSION),
            LogicalType::Json => Some(JSON_EXTENSION),
            _ => None,
        };
        match extension {
            Some(name) => field.with_metadata(HashMap::from([(
                EXTENSION_NAME.to_owned(),
                name.to_owned(),
            )])),
            None => field,
        }
    }

    /// The logical field for an Arrow field.
    ///
    /// Large, view, dictionary and run-end encoded types map to their plain logical type, maps map
    /// to lists of key/value structs, and unsigned integers map to the next wider signed type.
    pub fn from_arrow(field: &ArrowField) -> Result<Self, UnsupportedType> {
        let extension = field.metadata().get(EXTENSION_NAME).map(String::as_str);
        let logical_type = match (extension, field.data_type()) {
            (Some(UUID_EXTENSION), DataType::FixedSizeBinary(16)) => LogicalType::Uuid,
            (Some(JSON_EXTENSION), DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => {
                LogicalType::Json
            }
            (_, data_type) => from_data_type(field.name(), data_type)?,
        };
        Ok(Self::new(
            field.name().as_str(),
            logical_type,
            field.is_nullable(),
        ))
    }
}

impl LogicalType {
    /// The Arrow type that stores this logical type.
    pub fn to_arrow(&self) -> DataType {
        match self {
            Self::Null => DataType::Null,
            Self::Bool => DataType::Boolean,
            Self::Int8 => DataType::Int8,
            Self::Int16 => DataType::Int16,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Decimal(decimal) => decimal_to_arrow(*decimal),
            Self::Utf8 | Self::Json => DataType::Utf8,
            Self::Binary => DataType::Binary,
            Self::Date => DataType::Date32,
            Self::Time(unit @ (TimeUnit::Second | TimeUnit::Millisecond)) => {
                DataType::Time32(unit_to_arrow(*unit))
            }
            Self::Time(unit) => DataType::Time64(unit_to_arrow(*unit)),
            Self::Timestamp(unit, zone) => DataType::Timestamp(unit_to_arrow(*unit), zone.clone()),
            Self::Duration(unit) => DataType::Duration(unit_to_arrow(*unit)),
            Self::Uuid => DataType::FixedSizeBinary(16),
            Self::Struct(fields) => DataType::Struct(fields.iter().map(Field::to_arrow).collect()),
            Self::List(item) => DataType::List(Arc::new(item.to_arrow())),
        }
    }
}

fn decimal_to_arrow(decimal: DecimalType) -> DataType {
    let scale = i8::try_from(decimal.scale()).expect("scales are at most 76");
    if decimal.precision() <= 38 {
        DataType::Decimal128(decimal.precision(), scale)
    } else {
        DataType::Decimal256(decimal.precision(), scale)
    }
}

fn unit_to_arrow(unit: TimeUnit) -> ArrowTimeUnit {
    match unit {
        TimeUnit::Second => ArrowTimeUnit::Second,
        TimeUnit::Millisecond => ArrowTimeUnit::Millisecond,
        TimeUnit::Microsecond => ArrowTimeUnit::Microsecond,
        TimeUnit::Nanosecond => ArrowTimeUnit::Nanosecond,
    }
}

fn unit_from_arrow(unit: ArrowTimeUnit) -> TimeUnit {
    match unit {
        ArrowTimeUnit::Second => TimeUnit::Second,
        ArrowTimeUnit::Millisecond => TimeUnit::Millisecond,
        ArrowTimeUnit::Microsecond => TimeUnit::Microsecond,
        ArrowTimeUnit::Nanosecond => TimeUnit::Nanosecond,
    }
}

fn from_data_type(name: &str, data_type: &DataType) -> Result<LogicalType, UnsupportedType> {
    let unsupported = || UnsupportedType {
        field: name.to_owned(),
        data_type: data_type.to_string(),
    };
    let decimal = |precision: u8, scale: i8| {
        let scale = u8::try_from(scale).map_err(|_| unsupported())?;
        DecimalType::new(precision, scale)
            .map(LogicalType::Decimal)
            .map_err(|_| unsupported())
    };
    Ok(match data_type {
        DataType::Null => LogicalType::Null,
        DataType::Boolean => LogicalType::Bool,
        DataType::Int8 => LogicalType::Int8,
        DataType::Int16 | DataType::UInt8 => LogicalType::Int16,
        DataType::Int32 | DataType::UInt16 => LogicalType::Int32,
        DataType::Int64 | DataType::UInt32 => LogicalType::Int64,
        DataType::UInt64 => decimal(20, 0)?,
        DataType::Float16 | DataType::Float32 => LogicalType::Float32,
        DataType::Float64 => LogicalType::Float64,
        DataType::Decimal32(p, s)
        | DataType::Decimal64(p, s)
        | DataType::Decimal128(p, s)
        | DataType::Decimal256(p, s) => decimal(*p, *s)?,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => LogicalType::Utf8,
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => LogicalType::Binary,
        DataType::Date32 | DataType::Date64 => LogicalType::Date,
        DataType::Time32(unit) | DataType::Time64(unit) => {
            LogicalType::Time(unit_from_arrow(*unit))
        }
        DataType::Timestamp(unit, zone) => {
            LogicalType::Timestamp(unit_from_arrow(*unit), zone.clone())
        }
        DataType::Duration(unit) => LogicalType::Duration(unit_from_arrow(*unit)),
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::ListView(item)
        | DataType::LargeListView(item)
        | DataType::FixedSizeList(item, _) => list_of(item)?,
        DataType::Map(entries, _) => list_of(entries)?,
        DataType::Struct(fields) => {
            let fields = fields
                .iter()
                .map(|field| Field::from_arrow(field))
                .collect::<Result<Vec<_>, _>>()?;
            LogicalType::Struct(Fields::new(fields).map_err(|_| unsupported())?)
        }
        DataType::Dictionary(_, values) => from_data_type(name, values)?,
        DataType::RunEndEncoded(_, values) => from_data_type(name, values.data_type())?,
        _ => return Err(unsupported()),
    })
}

fn list_of(item: &ArrowField) -> Result<LogicalType, UnsupportedType> {
    let item = Field::from_arrow(item)?;
    Ok(LogicalType::List(Box::new(Field::new(
        LIST_ITEM,
        item.logical_type().clone(),
        item.is_nullable(),
    ))))
}
