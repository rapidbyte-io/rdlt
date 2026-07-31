//! Destination-side test access: the golden-SQL pin seams — DDL rendering,
//! the merge dialect, the unit-transaction literals, and stage naming — so
//! the pin suites compare emitted SQL as data without a server.

pub use crate::destination::catalog::{
    EnsureStatement, column_definition, merge_ensure_statements, sql_type, stage_name,
    stage_prefix, table_ddl_statements,
};
pub use crate::destination::dialect::Dialect;
pub use crate::destination::unit::{UNIT_BEGIN, UNIT_COMMIT, UNIT_ROLLBACK, UNIT_WORK_MEM};
