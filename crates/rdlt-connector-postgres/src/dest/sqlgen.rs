//! SQL-generation seam, exposed ONLY for the golden-SQL pin suite: the pins
//! bind the exact statement text across the sqlcore extraction. Not a public
//! API.

pub use super::commit::{ARRIVAL_COL, UNIT_BEGIN, UNIT_COMMIT, UNIT_ROLLBACK, UNIT_WORK_MEM};
pub use super::dialect::PgDialect;
pub use rdlt_connector_sqlcore::plan::{
    identity_delete_insert_sql, keyed_delete_insert_sql, keyed_upsert_sql, scd2_merge_sql,
    scope_replace_sql,
};
pub use rdlt_connector_sqlcore::{HardDelete, MergePlan};

use rdlt_connector::core::{PipelineId, TableSchema, WriteMode};
use rdlt_connector_sqlcore::plan::ValidateError;

use super::config::DestinationOptions;

/// The table-ensure statements, in emission order.
pub fn ensure_table_sql(
    pipeline: &PipelineId,
    schema: &TableSchema,
    mode: &WriteMode,
    previous: Option<&TableSchema>,
) -> Vec<String> {
    super::ddl::table_ddl_stmts(pipeline, schema, mode, previous)
}

/// One ensure statement and, when it creates a unique index, that index's
/// key columns — the distinction the duplicate-key diagnosis depends on.
pub type EnsureStatement = (String, Option<Vec<String>>);

/// The post-table ensure statements, in emission order.
pub fn ensure_merge_sql(
    options: &DestinationOptions,
    schema: &TableSchema,
    mode: &WriteMode,
) -> Result<Vec<EnsureStatement>, ValidateError> {
    Ok(super::ddl::merge_ensure_stmts(options, schema, mode)?
        .into_iter()
        .map(|s| (s.sql, s.unique_index))
        .collect())
}
