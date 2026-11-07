//! Stateless SQL transforms for rdlt streams: one query of the table `input`, projected and
//! optionally filtered, planned by DataFusion and evaluated by the engine, batch by batch.
//!
//! ```
//! use rdlt_sql::{Sql, SqlErrorKind};
//!
//! let refused = Sql::parse("SELECT id FROM input ORDER BY id").unwrap_err();
//! assert_eq!(refused.kind(), SqlErrorKind::Shape);
//! ```

#![forbid(unsafe_code)]

mod depth;
mod dialect;
mod error;
mod functions;
mod limits;
mod shape;
mod sql;
mod stack;

pub use error::{SqlError, SqlErrorKind};
pub use functions::function_names;
pub use limits::{REGEX_BYTES, SQL_BYTES, SQL_DEPTH, SQL_LITERAL_BYTES};
pub use sql::Sql;
