//! Stateless SQL transforms for rdlt streams: one query of the table `input`, projected and
//! optionally filtered, planned by DataFusion and evaluated by the engine, batch by batch.
//!
//! ```
//! use rdlt_connector::StreamName;
//! use rdlt_engine::StreamPlan;
//! use rdlt_sql::Sql;
//!
//! let plan = StreamPlan::new(StreamName::new("people")?).transform(Sql::parse(
//!     "SELECT id, lower(email) AS email FROM input WHERE status <> 'test'",
//! )?);
//! assert_eq!(plan.transforms().len(), 1);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

#![forbid(unsafe_code)]

#[cfg(panic = "abort")]
compile_error!(
    "rdlt-sql refuses a query whose parsing or planning panics by unwinding: build with \
     panic = \"unwind\""
);

#[cfg(feature = "bench")]
pub mod bench;
mod contain;
mod depth;
mod dialect;
mod error;
mod functions;
mod limits;
mod planned;
mod provider;
mod shape;
mod size;
mod sql;
mod stack;

pub use error::{SqlError, SqlErrorKind};
pub use functions::function_names;
pub use limits::{
    REGEX_BYTES, SQL_BYTES, SQL_CONDITION_DEPTH, SQL_DEPTH, SQL_LITERAL_BYTES,
    SQL_NESTED_CONDITIONS,
};
pub use sql::Sql;
