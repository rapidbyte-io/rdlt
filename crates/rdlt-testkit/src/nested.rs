//! Columns nested to a given depth, each level a struct or a list, for tests of what is stored,
//! logged and read back at the nesting limit.

use rdlt_connector::{Field, Fields, LogicalType};
use serde_json::{Value, json};

/// How each level below a nested column is made.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Nesting {
    /// A struct of one field, `a`, at every level.
    Structs,
    /// A list at every level.
    Lists,
    /// Structs and lists taking turns, a struct first.
    StructsThenLists,
    /// Lists and structs taking turns, a list first.
    ListsThenStructs,
}

/// Every way of nesting.
pub const NESTINGS: [Nesting; 4] = [
    Nesting::Structs,
    Nesting::Lists,
    Nesting::StructsThenLists,
    Nesting::ListsThenStructs,
];

impl Nesting {
    /// Whether `level` below the column, the first being 1, is a list.
    fn list(self, level: usize) -> bool {
        match self {
            Self::Structs => false,
            Self::Lists => true,
            Self::StructsThenLists => level.is_multiple_of(2),
            Self::ListsThenStructs => !level.is_multiple_of(2),
        }
    }
}

/// The type of a column nested `depth` levels deep, counting the column as the first, whose
/// deepest level holds 64-bit integers.
pub fn logical(depth: usize, nesting: Nesting) -> LogicalType {
    let mut logical = LogicalType::Int64;
    for level in (1..depth).rev() {
        logical = if nesting.list(level) {
            LogicalType::List(Box::new(Field::new("item", logical, true)))
        } else {
            let fields = Fields::new(vec![Field::new("a", logical, true)]).expect("one field");
            LogicalType::Struct(fields)
        };
    }
    logical
}

/// A value of the column [`logical`] types: one integer at its deepest level.
pub fn value(depth: usize, nesting: Nesting) -> Value {
    let mut value = json!(1);
    for level in (1..depth).rev() {
        value = if nesting.list(level) {
            json!([value])
        } else {
            json!({ "a": value })
        };
    }
    value
}
