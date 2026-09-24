//! The statements a SQL destination runs: pipeline state, staging, publishing and schema changes,
//! planned once for every SQL database.
//!
//! A destination implements [`SqlDialect`] for its database, or uses [`Sqlite`], and runs what
//! [`SqlPlanner`] plans inside its own transactions.
//!
//! ```
//! use rdlt_connector::PipelineId;
//! use rdlt_connector::sqlgen::{SqlPlanner, Sqlite};
//!
//! let planner = SqlPlanner::try_new(Sqlite)?;
//! let pipeline = PipelineId::parse("orders").expect("valid pipeline id");
//! let fence = planner.fence(&pipeline, rdlt_connector::Epoch(3));
//! assert!(fence.sql.starts_with("UPDATE"));
//! # Ok::<(), rdlt_connector::ConnectorError>(())
//! ```

mod catalog;
mod publish;
mod sqlite;
mod tables;
#[cfg(test)]
mod tests;

use crate::commit::SegmentSet;
use crate::error::{ConnectorError, ConnectorErrorKind, Result};
use crate::id::{Epoch, PipelineId};
use crate::types::LogicalType;

pub use catalog::{CATALOG_TABLES, micros, receipt};
pub use publish::{Staged, merge_key};
pub use sqlite::Sqlite;
pub use tables::{STAGING_COLUMNS, TABLE_PREFIX, generation_table, staging_table};

/// A value a statement binds to one of its placeholders.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SqlValue {
    /// `NULL`.
    Null,
    /// A 64-bit integer.
    Integer(i64),
    /// Text.
    Text(String),
    /// Bytes.
    Blob(Vec<u8>),
}

/// One statement and the values of its placeholders, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Statement {
    /// The SQL text.
    pub sql: String,
    /// The value of each placeholder, the first placeholder's first.
    pub params: Vec<SqlValue>,
}

/// A column of an existing table, as the database declares it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    /// The column's identifier.
    pub name: String,
    /// The type the column is declared with.
    pub declared: String,
}

/// What differs between SQL databases: quoting, placeholders, column types and catalog queries.
pub trait SqlDialect: Send + Sync {
    /// `name` quoted as an identifier.
    fn quote(&self, name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    /// The placeholder of the parameter at `index`, counting from 1.
    fn placeholder(&self, index: usize) -> String;

    /// The type a column storing `logical` is declared with, if the database stores it.
    fn column_type(&self, logical: &LogicalType) -> Option<String>;

    /// Whether a column declared `declared` holds every value of `logical`.
    fn holds(&self, declared: &str, logical: &LogicalType) -> bool {
        self.column_type(logical)
            .is_some_and(|wanted| wanted.eq_ignore_ascii_case(declared))
    }

    /// The statement declaring `column` of `table` as `declared`, if the database changes a column
    /// in place; `None` makes a widen the column does not hold a conflict.
    fn widen(&self, _table: &str, _column: &str, _declared: &str) -> Option<String> {
        None
    }

    /// The query listing `table`'s columns as rows of identifier and declared type, in order; it
    /// returns no rows when the table does not exist.
    fn columns(&self, table: &str) -> Statement;
}

/// Plans the statements of a SQL destination for dialect `D`.
#[derive(Clone, Debug)]
pub struct SqlPlanner<D> {
    dialect: D,
    text: String,
    integer: String,
    blob: String,
}

impl<D: SqlDialect> SqlPlanner<D> {
    /// A planner for `dialect`, which must store text, 64-bit integers and bytes.
    pub fn try_new(dialect: D) -> Result<Self> {
        let declared = |logical: LogicalType| {
            dialect.column_type(&logical).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    format!("the SQL dialect does not store {logical:?}, which the catalog needs"),
                )
            })
        };
        Ok(Self {
            text: declared(LogicalType::Utf8)?,
            integer: declared(LogicalType::Int64)?,
            blob: declared(LogicalType::Binary)?,
            dialect,
        })
    }

    /// The dialect the planner writes.
    pub fn dialect(&self) -> &D {
        &self.dialect
    }

    fn quote(&self, name: &str) -> String {
        self.dialect.quote(name)
    }

    fn sql(&self) -> Sql<'_, D> {
        Sql {
            dialect: &self.dialect,
            text: String::new(),
            params: Vec::new(),
        }
    }

    /// A condition on the staging columns: rows `pipeline` staged at `epoch` in `segments`.
    fn staged_by(
        &self,
        sql: &mut Sql<'_, D>,
        pipeline: &PipelineId,
        epoch: Epoch,
        segments: &SegmentSet,
        [pipeline_column, epoch_column, segment_column]: [&str; 3],
    ) {
        let pipeline = sql.bind(SqlValue::Text(pipeline.to_string()));
        let epoch = sql.bind(integer(epoch.0));
        sql.push(&format!(
            "{} = {pipeline} AND {} = {epoch} AND (",
            self.quote(pipeline_column),
            self.quote(epoch_column)
        ));
        if segments.is_empty() {
            sql.push("1 = 0");
        }
        for (index, range) in segments.ranges().iter().enumerate() {
            if index > 0 {
                sql.push(" OR ");
            }
            let first = sql.bind(integer(range.first.0));
            let last = sql.bind(integer(range.last.0));
            sql.push(&format!(
                "{} BETWEEN {first} AND {last}",
                self.quote(segment_column)
            ));
        }
        sql.push(")");
    }
}

/// A statement being written, numbering its placeholders as values are bound.
struct Sql<'a, D> {
    dialect: &'a D,
    text: String,
    params: Vec<SqlValue>,
}

impl<D: SqlDialect> Sql<'_, D> {
    fn push(&mut self, text: &str) {
        self.text.push_str(text);
    }

    /// Binds `value` and returns its placeholder.
    fn bind(&mut self, value: SqlValue) -> String {
        self.params.push(value);
        self.dialect.placeholder(self.params.len())
    }

    fn finish(self) -> Statement {
        Statement {
            sql: self.text,
            params: self.params,
        }
    }
}

/// `value` as a SQL integer with the same bits, so every id survives.
///
/// Read it back with [`unsigned`]. Segment ranges compare correctly below 2⁶³, which counters
/// never reach.
fn integer(value: u64) -> SqlValue {
    SqlValue::Integer(i64::from_ne_bytes(value.to_ne_bytes()))
}

/// The unsigned value a planner stored as the integer `stored`.
pub fn unsigned(stored: i64) -> u64 {
    u64::from_ne_bytes(stored.to_ne_bytes())
}
