//! Stateless SQL transforms for rdlt streams: one query of the table `input`, projected and
//! optionally filtered, planned by DataFusion and evaluated by the engine, batch by batch.
//!
//! ```
//! assert_eq!(rdlt_sql::SQL_BYTES, 64 << 10);
//! ```

#![forbid(unsafe_code)]

mod limits;

pub use limits::{REGEX_BYTES, SQL_BYTES, SQL_DEPTH, SQL_LITERAL_BYTES};
