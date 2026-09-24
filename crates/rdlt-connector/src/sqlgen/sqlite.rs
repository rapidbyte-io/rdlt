//! The SQLite dialect.

use super::{SqlDialect, SqlValue, Statement};
use crate::types::LogicalType;

/// SQLite: numbered `?` placeholders and one declared type per storage class.
///
/// Every integer width is stored as `INTEGER` and both float widths as `REAL`, so widening
/// within those families changes nothing; SQLite cannot change a column's type otherwise.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Sqlite;

impl SqlDialect for Sqlite {
    fn placeholder(&self, index: usize) -> String {
        format!("?{index}")
    }

    fn column_type(&self, logical: &LogicalType) -> Option<String> {
        let declared = match logical {
            LogicalType::Bool => "BOOLEAN",
            LogicalType::Int8 | LogicalType::Int16 | LogicalType::Int32 | LogicalType::Int64 => {
                "INTEGER"
            }
            LogicalType::Float32 | LogicalType::Float64 => "REAL",
            LogicalType::Utf8 => "TEXT",
            LogicalType::Binary => "BLOB",
            _ => return None,
        };
        Some(declared.to_owned())
    }

    fn columns(&self, table: &str) -> Statement {
        Statement {
            sql: "SELECT name, type FROM pragma_table_info(?1) ORDER BY cid".to_owned(),
            params: vec![SqlValue::Text(table.to_owned())],
        }
    }
}
