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
mod changes;
mod owned;
mod publish;
mod sqlite;
mod tables;
#[cfg(test)]
mod tests;
mod upsert;

use crate::commit::SegmentSet;
use crate::error::{ConnectorError, ConnectorErrorKind, Result};
use crate::id::{Epoch, PipelineId};
use crate::types::LogicalType;

pub use catalog::{micros, receipt};
pub use changes::staged_changes;
pub use owned::Owned;
pub use publish::{Staged, merge_key};
pub use sqlite::Sqlite;
pub use tables::{STAGING_COLUMNS, TABLE_PREFIX};
pub use upsert::Upserts;

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

    /// The dialect's own rendering of the type an existing column is declared with as `declared`,
    /// if it is a type the dialect declares columns with; by default, a match without case among
    /// what [`SqlDialect::column_type`] renders for the types that take no parameter.
    ///
    /// The planner copies a column's type into the tables it derives only as this renders it,
    /// never as the database reports it, so a dialect whose types take parameters reads them here.
    fn declares(&self, declared: &str) -> Option<String> {
        use crate::types::TimeUnit as Unit;
        let units = [
            Unit::Second,
            Unit::Millisecond,
            Unit::Microsecond,
            Unit::Nanosecond,
        ];
        let plain = [
            LogicalType::Bool,
            LogicalType::Int8,
            LogicalType::Int16,
            LogicalType::Int32,
            LogicalType::Int64,
            LogicalType::Float32,
            LogicalType::Float64,
            LogicalType::Utf8,
            LogicalType::Binary,
            LogicalType::Date,
            LogicalType::Uuid,
            LogicalType::Json,
        ];
        let timed = units.into_iter().flat_map(|unit| {
            [
                LogicalType::Time(unit),
                LogicalType::Duration(unit),
                LogicalType::Timestamp(unit, None),
                LogicalType::Timestamp(unit, Some("UTC".into())),
            ]
        });
        plain
            .into_iter()
            .chain(timed)
            .filter_map(|logical| self.column_type(&logical))
            .find(|rendered| rendered.eq_ignore_ascii_case(declared))
    }

    /// The statement declaring `column` of `table`, both quoted, as `declared`, if the database
    /// changes a column in place; `None` makes a widen the column does not hold a conflict.
    fn widen(&self, _table: &str, _column: &str, _declared: &str) -> Option<String> {
        None
    }

    /// The query listing `table`'s columns as rows of identifier and declared type, in order; it
    /// returns no rows when the table does not exist.
    fn columns(&self, table: &str) -> Statement;

    /// Whether schema changes a transaction makes commit or roll back with it, as SQLite's and
    /// PostgreSQL's do.
    ///
    /// Swapping a replace generation in renames tables, so the planner refuses the swap where a
    /// failed commit would leave them half renamed.
    fn transactional_ddl(&self) -> bool;

    /// The statement creating the index `name` of `table` on `columns`, a list, all quoted,
    /// unless it exists.
    fn create_index(&self, name: &str, table: &str, columns: &str) -> String {
        format!("CREATE INDEX IF NOT EXISTS {name} ON {table} ({columns})")
    }

    /// The statement dropping the index `name` of `table`, both quoted, where it exists.
    fn drop_index(&self, name: &str, table: &str) -> String {
        let _ = table;
        format!("DROP INDEX IF EXISTS {name}")
    }

    /// The table a `SELECT` of bound values alone reads, as Oracle's `DUAL`, where the database
    /// needs one; none by default.
    fn values_table(&self) -> Option<&str> {
        None
    }

    /// How the dialect writes a row whose key a row may already hold: in standard SQL unless it
    /// says otherwise.
    fn upserts(&self) -> Upserts {
        Upserts::Guarded
    }

    /// The most bytes an identifier may have, where the database limits them, at least
    /// [`MIN_IDENTIFIER`]; the tables and indexes the planner derives from a table's name keep
    /// within it.
    fn max_identifier(&self) -> Option<usize> {
        None
    }

    /// Whether no destination table may take the name `name`: the database keeps it for itself,
    /// or would take it for another table's, as one matching names without case takes another
    /// case of a name; no name by default.
    fn reserves_table(&self, name: &str) -> bool {
        let _ = name;
        false
    }
}

/// Plans the statements of a SQL destination for dialect `D`.
#[derive(Clone, Debug)]
pub struct SqlPlanner<D> {
    dialect: D,
    text: String,
    integer: String,
    blob: String,
}

/// Bytes: the fewest a dialect's identifiers may hold, PostgreSQL's limit, which fits the name a
/// derived table too long for the dialect takes: a reserved prefix and the hash of the whole.
pub const MIN_IDENTIFIER: usize = 63;

impl<D: SqlDialect> SqlPlanner<D> {
    /// A planner for `dialect`, which must store text, 64-bit integers and bytes, and whose
    /// identifiers, where it limits them, hold at least [`MIN_IDENTIFIER`] bytes.
    pub fn try_new(dialect: D) -> Result<Self> {
        if let Some(max) = dialect.max_identifier().filter(|max| *max < MIN_IDENTIFIER) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                format!(
                    "the SQL dialect's identifiers hold {max} bytes; derived names need \
                     {MIN_IDENTIFIER}"
                ),
            ));
        }
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
