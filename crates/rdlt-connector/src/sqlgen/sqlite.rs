//! The SQLite dialect.

use super::{SqlDialect, SqlValue, Statement, Upserts};
use crate::types::LogicalType;

/// SQLite: numbered `?` placeholders and one declared type per storage class.
///
/// Every integer width is stored as `INTEGER` and both float widths as `REAL`, so widening
/// within those families changes nothing; SQLite cannot change a column's type otherwise.
///
/// SQLite keeps the names under `sqlite_` for itself and reads those under `pragma_` as its
/// table-valued functions, and it matches table names without ASCII case, so a destination table
/// takes none of those, and only a name that is its own lower case.
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

    fn transactional_ddl(&self) -> bool {
        true
    }

    fn upserts(&self) -> Upserts {
        Upserts::OnConflict
    }

    fn reserves_table(&self, name: &str) -> bool {
        let under = |prefix: &str| {
            name.get(..prefix.len())
                .is_some_and(|start| start.eq_ignore_ascii_case(prefix))
        };
        under("sqlite_") || under("pragma_") || name.bytes().any(|byte| byte.is_ascii_uppercase())
    }

    fn resolves(&self, name: &str) -> Statement {
        // SQLite keeps tables, views and indexes under one set of names, matched without ASCII
        // case.
        Statement {
            sql: "SELECT name FROM sqlite_schema WHERE name = ?1 COLLATE NOCASE AND type IN \
                  ('table', 'view', 'index') ORDER BY name"
                .to_owned(),
            params: vec![SqlValue::Text(name.to_owned())],
        }
    }

    fn byte_at(&self, bytes: &str, position: usize) -> String {
        // SQLite finds a byte of a blob by its position, whatever the blob's length.
        format!("SUBSTR({bytes}, {position}, 1)")
    }

    fn folds(&self, name: &str) -> String {
        name.to_ascii_lowercase()
    }

    fn columns(&self, table: &str) -> Statement {
        // Only a table the schema holds has columns: the pragma alone would also answer for the
        // virtual tables SQLite has without anyone creating them.
        Statement {
            sql: "SELECT _rdlt_c.name, _rdlt_c.type FROM sqlite_schema _rdlt_t, \
                  pragma_table_info(_rdlt_t.name) _rdlt_c WHERE _rdlt_t.type = 'table' AND \
                  _rdlt_t.name = ?1 ORDER BY _rdlt_c.cid"
                .to_owned(),
            params: vec![SqlValue::Text(table.to_owned())],
        }
    }
}
