//! Whose table a session may change: the names a destination table may take, who owns each, and
//! the witness every statement changing a table is planned from.

use std::sync::Arc;

use super::tables::TABLE_PREFIX;
use super::upsert::Row;
use super::{SqlDialect, SqlPlanner, SqlValue, Statement};
use crate::error::{ConnectorError, Result};
use crate::id::PipelineId;

pub(super) const OWNERS: &str = "_rdlt_owners";

/// A destination table a session may change: its name is none the planner or the database
/// keeps, and its owner record names the session's pipeline.
///
/// Only [`SqlPlanner::owned`] makes one, from the owner record the destination read in the
/// transaction that uses it, so no statement changing a table is planned for a table the
/// pipeline does not own.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Owned {
    name: Arc<str>,
    pipeline: PipelineId,
}

impl Owned {
    /// The table's identifier.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The pipeline that owns the table.
    pub fn pipeline(&self) -> &PipelineId {
        &self.pipeline
    }

    /// Refuses a statement planned for the table `name` from the witness of another table.
    pub(super) fn is(&self, name: &str) -> Result<()> {
        if *self.name == *name {
            return Ok(());
        }
        Err(ConnectorError::internal(format!(
            "a statement for table {name} was planned from the owner check of table {}",
            self.name
        )))
    }
}

impl<D: SqlDialect> SqlPlanner<D> {
    /// Refuses `name` as a destination table's where the planner keeps it, as every name under
    /// [`TABLE_PREFIX`] in any case, or the dialect does: a `Config` error coded
    /// `table_name_reserved`.
    pub fn named(&self, name: &str) -> Result<()> {
        let kept = name
            .get(..TABLE_PREFIX.len())
            .is_some_and(|start| start.eq_ignore_ascii_case(TABLE_PREFIX));
        if name.is_empty() || kept || self.dialect.reserves_table(name) {
            return Err(ConnectorError::config(format!(
                "{name:?} is a name the destination keeps for itself, which no table may take"
            ))
            .with_code("table_name_reserved"));
        }
        Ok(())
    }

    /// The statements claiming the table `name` for `pipeline` where no pipeline owns it yet;
    /// [`SqlPlanner::owner`] then reads who does.
    pub fn claim(&self, pipeline: &PipelineId, name: &str) -> Result<Vec<Statement>> {
        self.named(name)?;
        let key = [("name", SqlValue::Text(name.to_owned()))];
        let values = [("pipeline", SqlValue::Text(pipeline.to_string()))];
        let row = Row {
            table: OWNERS,
            key: &key,
            values: &values,
        };
        Ok(self.upsert(&row, false))
    }

    /// The query returning the pipeline that owns the table `name`: the first to claim it.
    pub fn owner(&self, name: &str) -> Statement {
        let mut sql = self.sql();
        let name = sql.bind(SqlValue::Text(name.to_owned()));
        sql.push(&format!(
            "SELECT pipeline FROM {OWNERS} WHERE name = {name}"
        ));
        sql.finish()
    }

    /// The table `name` as one `pipeline`'s session may change, where `owner`, what
    /// [`SqlPlanner::owner`] returned, is `pipeline`.
    ///
    /// A name the destination keeps is a `Config` error coded `table_name_reserved`, a table no
    /// pipeline owns one coded `table_unowned`, and another pipeline's one coded `table_owned`.
    pub fn owned(&self, pipeline: &PipelineId, name: &str, owner: Option<&str>) -> Result<Owned> {
        self.named(name)?;
        match owner {
            Some(owner) if owner == pipeline.as_str() => Ok(Owned {
                name: name.into(),
                pipeline: pipeline.clone(),
            }),
            Some(owner) => Err(ConnectorError::table_owned(name, owner)),
            None => Err(ConnectorError::config(format!(
                "table {name} belongs to no pipeline, so no pipeline's session changes it"
            ))
            .with_code("table_unowned")),
        }
    }

    /// The table `name` as `pipeline`'s commit drops it, or none where there is nothing to drop:
    /// no pipeline owns it and it does not exist, as after an earlier try of the commit.
    ///
    /// A table that exists is refused as [`SqlPlanner::owned`] refuses it.
    pub fn dropped(
        &self,
        pipeline: &PipelineId,
        name: &str,
        owner: Option<&str>,
        exists: bool,
    ) -> Result<Option<Owned>> {
        self.named(name)?;
        if owner.is_none() && !exists {
            return Ok(None);
        }
        self.owned(pipeline, name, owner).map(Some)
    }

    /// The query returning the identifier of every table `pipeline` owns.
    pub fn owned_by(&self, pipeline: &PipelineId) -> Statement {
        let mut sql = self.sql();
        let pipeline = sql.bind(SqlValue::Text(pipeline.to_string()));
        sql.push(&format!(
            "SELECT name FROM {OWNERS} WHERE pipeline = {pipeline} ORDER BY name"
        ));
        sql.finish()
    }

    /// The query returning the identifier of every table a pipeline owns.
    pub fn owned_tables(&self) -> Statement {
        Statement {
            sql: format!("SELECT name FROM {OWNERS} ORDER BY name"),
            params: Vec::new(),
        }
    }
}
