//! Whose table a session may change: the names a destination table may take, who owns each, how
//! the database takes each name, and the witness every statement changing a table is planned
//! from.

#[cfg(test)]
mod tests;

use std::marker::PhantomData;
use std::sync::Arc;

use super::catalog::CATALOG;
use super::tables::TABLE_PREFIX;
use super::upsert::Row;
use super::{SqlDialect, SqlPlanner, SqlValue, Statement};
use crate::destination::TableRef;
use crate::error::{ConnectorError, Result};
use crate::id::PipelineId;

pub(super) const OWNERS: &str = "_rdlt_owners";

/// A destination table a session may change, for as long as the transaction that checked it.
///
/// Its name is none the planner or the database keeps, its owner record names the session's
/// pipeline or is written with the table, and the database takes its name for no other table.
/// Only a [`Standing`] makes one, from what the planner's own queries answered, and it borrows
/// what those answers were read in, so none outlives its transaction or is copied out of it.
///
/// The planner runs no statement: that the answers are the database's is the connector's to
/// keep, by handing [`Check::answered`] the rows its queries returned and nothing else.
#[derive(Debug, PartialEq, Eq)]
pub struct Owned<'t> {
    name: Arc<str>,
    pipeline: PipelineId,
    claims: bool,
    within: PhantomData<&'t ()>,
}

impl Owned<'_> {
    /// The table's identifier.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The pipeline that owns the table.
    pub fn pipeline(&self) -> &PipelineId {
        &self.pipeline
    }

    /// Whether no pipeline owns the table yet: creating it writes its owner record.
    pub fn claims(&self) -> bool {
        self.claims
    }

    /// Refuses a statement planned for the table `name` from the witness of another table, or
    /// of a table that is yet to be created.
    pub(super) fn is(&self, name: &str) -> Result<()> {
        self.names(name)?;
        if self.claims {
            return Err(ConnectorError::internal(format!(
                "a statement for table {name} was planned before the table was created"
            )));
        }
        Ok(())
    }

    /// Refuses a statement planned for the table `name` from the witness of another table.
    pub(super) fn names(&self, name: &str) -> Result<()> {
        if *self.name == *name {
            return Ok(());
        }
        Err(ConnectorError::internal(format!(
            "a statement for table {name} was planned from the owner check of table {}",
            self.name
        )))
    }
}

/// The queries that say how a table stands: who owns it, and what the database takes its name
/// for.
#[derive(Debug)]
pub struct Check {
    name: String,
    owner: Statement,
    resolved: Statement,
}

impl Check {
    /// The query returning the pipeline the table's owner record names.
    pub fn owner(&self) -> &Statement {
        &self.owner
    }

    /// The query returning what the database takes the table's name for.
    pub fn resolved(&self) -> &Statement {
        &self.resolved
    }

    /// How the table stands, from the rows `owner` and `resolved` that its two queries returned
    /// `within` a transaction, which what is made of the answer borrows.
    pub fn answered<'t, T: ?Sized>(
        self,
        within: &'t T,
        owner: &[Vec<SqlValue>],
        resolved: &[Vec<SqlValue>],
    ) -> Result<Standing<'t>> {
        let _ = within;
        let text = |row: &Vec<SqlValue>| match &row[..] {
            [SqlValue::Text(text)] => Ok(text.clone()),
            _ => Err(ConnectorError::internal(format!(
                "the check of table {} answered with a row that is no name",
                self.name
            ))),
        };
        Ok(Standing {
            owner: owner.first().map(text).transpose()?,
            found: resolved.iter().map(text).collect::<Result<_>>()?,
            name: self.name,
            within: PhantomData,
        })
    }
}

/// How a table stands in the database: who owns it, and the names, as the database holds them,
/// of what it takes the table's name for.
#[derive(Debug)]
pub struct Standing<'t> {
    name: String,
    owner: Option<String>,
    found: Vec<String>,
    within: PhantomData<&'t ()>,
}

impl<'t> Standing<'t> {
    /// Whether no pipeline owns the table.
    pub fn unowned(&self) -> bool {
        self.owner.is_none()
    }

    /// The table as `pipeline`'s session creates it: one it owns, or one no pipeline owns and
    /// the database holds nothing under the name of, which creating it claims.
    ///
    /// A table the database already holds without an owner record is not adopted: it is refused
    /// as `table_unowned`, and so is a name the database takes for a table named otherwise.
    pub fn created(self, pipeline: &PipelineId) -> Result<Owned<'t>> {
        let claims = self.owner.is_none();
        if claims && !self.found.is_empty() {
            return Err(unowned(&self.name, &self.found[0]));
        }
        self.witness(pipeline, claims)
    }

    /// The table as `pipeline`'s session changes it, writes it or publishes into it: one its
    /// owner record names `pipeline` for.
    ///
    /// A table no pipeline owns is a `Config` error coded `table_unowned`, another pipeline's
    /// one coded `table_owned`, and a name the database takes for a table named otherwise one
    /// coded `table_unowned`.
    pub fn owned(self, pipeline: &PipelineId) -> Result<Owned<'t>> {
        match self.owner {
            Some(_) => self.witness(pipeline, false),
            None => Err(unowned(&self.name, &self.name)),
        }
    }

    /// The table as `pipeline`'s commit drops it, or none where there is nothing to drop: no
    /// pipeline owns it and the database holds nothing under its name, as after an earlier try
    /// of the commit.
    ///
    /// Anything else is refused as [`Standing::owned`] refuses it, so a drop reaches only a
    /// table the pipeline's owner record names as the database holds it.
    pub fn dropped(self, pipeline: &PipelineId) -> Result<Option<Owned<'t>>> {
        if self.owner.is_none() && self.found.is_empty() {
            return Ok(None);
        }
        self.owned(pipeline).map(Some)
    }

    /// The witness of the table for `pipeline`, where no other pipeline owns it and the
    /// database takes its name for nothing named otherwise.
    fn witness(self, pipeline: &PipelineId, claims: bool) -> Result<Owned<'t>> {
        if let Some(owner) = self
            .owner
            .as_deref()
            .filter(|owner| *owner != pipeline.as_str())
        {
            return Err(ConnectorError::table_owned(&self.name, owner));
        }
        if let Some(other) = self.found.iter().find(|found| **found != self.name) {
            return Err(unowned(&self.name, other));
        }
        Ok(Owned {
            name: self.name.into(),
            pipeline: pipeline.clone(),
            claims,
            within: PhantomData,
        })
    }
}

/// The error of the table `name`, which the database holds as `found` and no owner record
/// gives the session.
fn unowned(name: &str, found: &str) -> ConnectorError {
    let message = if name == found {
        format!("table {name} belongs to no pipeline, so no pipeline's session changes it")
    } else {
        format!(
            "the database takes the name {name} for {found}, which belongs to no pipeline, so \
             no pipeline's session changes it"
        )
    };
    ConnectorError::config(message).with_code("table_unowned")
}

impl<D: SqlDialect> SqlPlanner<D> {
    /// Refuses `name` as a destination table's where the planner keeps it, as every name under
    /// [`TABLE_PREFIX`] in any case, where the dialect does, or where it holds a NUL, which no
    /// statement's text carries: a `Config` error coded `table_name_reserved`.
    pub fn named(&self, name: &str) -> Result<()> {
        let kept = name
            .get(..TABLE_PREFIX.len())
            .is_some_and(|start| start.eq_ignore_ascii_case(TABLE_PREFIX));
        if name.is_empty() || name.contains('\0') || kept || self.dialect.reserves_table(name) {
            return Err(ConnectorError::config(format!(
                "{name:?} is a name the destination keeps for itself, which no table may take"
            ))
            .with_code("table_name_reserved"));
        }
        Ok(())
    }

    /// The queries that say how the table `name` stands, to run in the transaction that changes
    /// it; a name the destination keeps is a `Config` error coded `table_name_reserved`.
    pub fn check(&self, name: &str) -> Result<Check> {
        self.named(name)?;
        Ok(Check {
            name: name.to_owned(),
            owner: self.owner(name),
            resolved: self.dialect.resolves(name),
        })
    }

    /// The query returning what the database takes `name` for, a table the planner derives or
    /// the catalog keeps; [`SqlPlanner::exact`] reads its answer.
    pub fn resolves(&self, name: &str) -> Statement {
        self.dialect.resolves(name)
    }

    /// Refuses `name`, a table the planner derives or the catalog keeps, where the database
    /// takes the name for a table named otherwise, as `resolved`, the rows of
    /// [`SqlPlanner::resolves`], say: a `Config` error coded `table_name_clash`.
    ///
    /// A statement creating the table would create nothing there, and one writing it would
    /// write the other table.
    pub fn exact(&self, name: &str, resolved: &[Vec<SqlValue>]) -> Result<()> {
        let other = resolved.iter().find(|row| match &row[..] {
            [SqlValue::Text(found)] => found != name,
            _ => true,
        });
        match other {
            None => Ok(()),
            Some(found) => Err(ConnectorError::config(format!(
                "the database takes the name {name}, which the destination keeps for itself, \
                 for {found:?}"
            ))
            .with_code("table_name_clash")),
        }
    }

    /// The tables the planner derives from `table` and creates beside it: its staging, its
    /// tombstones, and for a generation the generation's table.
    pub fn derived(&self, table: &TableRef) -> Vec<String> {
        let mut names = vec![
            self.staging_table(&table.name),
            self.tombstone_table(&table.name),
        ];
        if table.generation.is_some() {
            names.push(self.target(table));
        }
        names
    }

    /// The tables of the catalog, which [`SqlPlanner::bootstrap`] creates.
    pub fn catalog(&self) -> [&'static str; 7] {
        CATALOG
    }

    /// The statements claiming the table `name` for `pipeline` where no pipeline owns it yet.
    pub(super) fn claim(&self, pipeline: &PipelineId, name: &str) -> Vec<Statement> {
        let key = [("name", SqlValue::Text(name.to_owned()))];
        let values = [("pipeline", SqlValue::Text(pipeline.to_string()))];
        let row = Row {
            table: OWNERS,
            key: &key,
            values: &values,
        };
        self.upsert(&row, false)
    }

    /// The query returning the pipeline that owns the table `name`: the first to claim it.
    pub(super) fn owner(&self, name: &str) -> Statement {
        let mut sql = self.sql();
        let name = sql.bind(SqlValue::Text(name.to_owned()));
        sql.push(&format!(
            "SELECT pipeline FROM {OWNERS} WHERE name = {name}"
        ));
        sql.finish()
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
