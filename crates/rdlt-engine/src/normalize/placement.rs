//! Where normalizing puts a value: an object within depth flattens into its fields, an array
//! within depth becomes a child table, and anything else is a column, including an object or
//! array held in a dictionary or runs.

use arrow_schema::DataType;
use rdlt_connector::LogicalType;

/// What a value is, as normalizing tells containers apart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Container {
    /// A struct.
    Object,
    /// A list of any layout, or a map.
    Array,
    /// Anything else, including an object or array held in a dictionary or runs.
    Other,
}

/// Where normalizing puts a value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Placement {
    /// Its fields are placed, one level deeper, in its table.
    Fields,
    /// Its items, one level deeper, are a child table's rows.
    Items,
    /// It is a column of its table.
    Column,
}

impl Container {
    /// What a value of the Arrow type `data_type` is.
    pub(crate) fn of_arrow(data_type: &DataType) -> Self {
        match data_type {
            DataType::Struct(_) => Self::Object,
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(..)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::Map(..) => Self::Array,
            _ => Self::Other,
        }
    }

    /// What a value of `logical` is: a logical type names no encoding, so it is what the plain
    /// Arrow type of `logical` is.
    pub(crate) fn of_logical(logical: &LogicalType) -> Self {
        match logical {
            LogicalType::Struct(_) => Self::Object,
            LogicalType::List(_) => Self::Array,
            _ => Self::Other,
        }
    }
}

/// Where a value that is `container`, at `depth`, goes, where containers nested deeper than
/// `max_depth` stay whole.
pub(crate) fn placement(container: Container, depth: u8, max_depth: u8) -> Placement {
    match container {
        _ if depth > max_depth => Placement::Column,
        Container::Object => Placement::Fields,
        Container::Array => Placement::Items,
        Container::Other => Placement::Column,
    }
}

#[cfg(test)]
mod tests;
