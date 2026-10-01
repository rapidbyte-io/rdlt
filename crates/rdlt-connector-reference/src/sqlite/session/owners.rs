//! Who owns a destination table, and how the database takes its name: the check every change of
//! a table starts from, in the transaction that makes the change.

use rdlt_connector::prelude::*;
use rdlt_connector::sqlgen::{Owned, SqlPlanner, SqlValue, Sqlite, Standing};
use rdlt_connector::{Epoch, PipelineId};
use rusqlite::types::Value;
use rusqlite::{Connection, Transaction};

use super::super::database::{columns, query, run, run_all, text};

/// The rows `statement` answers, as the planner reads them.
fn answers(
    transaction: &Connection,
    statement: &rdlt_connector::sqlgen::Statement,
) -> Result<Vec<Vec<SqlValue>>> {
    let rows = query(transaction, statement)?;
    Ok(rows
        .into_iter()
        .map(|row| row.into_iter().map(planned).collect())
        .collect())
}

fn planned(value: Value) -> SqlValue {
    match value {
        // The catalog holds no float: one is no name and no pipeline, which is what the planner
        // makes of a value that is no text.
        Value::Null | Value::Real(_) => SqlValue::Null,
        Value::Integer(integer) => SqlValue::Integer(integer),
        Value::Text(text) => SqlValue::Text(text),
        Value::Blob(blob) => SqlValue::Blob(blob),
    }
}

/// How the table `name` stands in `transaction`: who owns it and what the database takes its
/// name for.
pub(super) fn standing<'t>(
    transaction: &'t Connection,
    planner: &SqlPlanner<Sqlite>,
    name: &str,
) -> Result<Standing<'t>> {
    let check = planner.check(name)?;
    let owner = answers(transaction, check.owner())?;
    let resolved = answers(transaction, check.resolved())?;
    check.answered(transaction, &owner, &resolved)
}

/// The name of the table `name` as its rows are read back on `connection`: a table a pipeline
/// owns; none where there is no such table, or no catalog, as in a database no pipeline opened.
pub(in crate::sqlite) fn published(
    connection: &Connection,
    planner: &SqlPlanner<Sqlite>,
    name: &str,
) -> Result<Option<String>> {
    let check = planner.check(name)?;
    let resolved = answers(connection, check.resolved())?;
    // Without a catalog no pipeline owns anything.
    let cataloged = planner.catalog().iter().all(|table| {
        columns(connection, planner.dialect(), table).is_ok_and(|held| !held.is_empty())
    });
    let owner = if cataloged {
        answers(connection, check.owner())?
    } else {
        Vec::new()
    };
    check.answered(connection, &owner, &resolved)?.published()
}

/// The table `name` as `pipeline`'s session may change it: refused under a name the destination
/// keeps, without an owner record, or as `table_owned` where another pipeline owns it.
pub(super) fn owned<'t>(
    transaction: &'t Transaction<'_>,
    planner: &SqlPlanner<Sqlite>,
    pipeline: &PipelineId,
    name: &str,
) -> Result<Owned<'t>> {
    standing(transaction, planner, name)?.owned(pipeline)
}

/// The table `name` as `pipeline`'s session at `epoch` changes or writes it.
///
/// A session a newer one fenced is refused as fenced where no pipeline owns the table: a drop
/// may have released it from that session.
pub(super) fn changed<'t>(
    transaction: &'t Transaction<'_>,
    planner: &SqlPlanner<Sqlite>,
    pipeline: &PipelineId,
    epoch: Epoch,
    name: &str,
) -> Result<Owned<'t>> {
    let standing = standing(transaction, planner, name)?;
    if standing.unowned() && run(transaction, &planner.fence(pipeline, epoch))? == 0 {
        return Err(fenced(pipeline, epoch));
    }
    standing.owned(pipeline)
}

fn fenced(pipeline: &PipelineId, epoch: Epoch) -> ConnectorError {
    ConnectorError::fenced(format!(
        "pipeline {pipeline} has a session newer than epoch {epoch}"
    ))
}

/// The table `table` names as `pipeline`'s session at `epoch` creates it, which claims it where
/// no pipeline owns it and the database holds nothing under its name or the names derived from
/// it; a claim by a session a newer one fenced is refused as fenced, since a drop may have
/// released the table from it.
pub(super) fn created<'t>(
    transaction: &'t Transaction<'_>,
    planner: &SqlPlanner<Sqlite>,
    pipeline: &PipelineId,
    epoch: Epoch,
    table: &TableRef,
) -> Result<Owned<'t>> {
    let owned = standing(transaction, planner, &table.name)?.created(pipeline)?;
    if owned.claims() && run(transaction, &planner.fence(pipeline, epoch))? == 0 {
        return Err(fenced(pipeline, epoch));
    }
    derived(transaction, planner, table)?;
    Ok(owned)
}

/// Refuses `table` where the database takes the name of a table derived from it for a table
/// named otherwise.
pub(super) fn derived(
    transaction: &Transaction<'_>,
    planner: &SqlPlanner<Sqlite>,
    table: &TableRef,
) -> Result<()> {
    for name in planner.derived(table) {
        planner.exact(&name, &answers(transaction, &planner.resolves(&name))?)?;
    }
    Ok(())
}

/// Refuses a database that takes the name of a catalog table for a table named otherwise.
pub(super) fn catalog(transaction: &Transaction<'_>, planner: &SqlPlanner<Sqlite>) -> Result<()> {
    for name in planner.catalog() {
        planner.exact(name, &answers(transaction, &planner.resolves(name))?)?;
    }
    Ok(())
}

/// Removes what sessions of `pipeline` older than `epoch` staged in the tables it owns, and
/// releases an owner record of its that stands for no staging table, which no create left.
pub(super) fn discard(
    transaction: &Transaction<'_>,
    planner: &SqlPlanner<Sqlite>,
    pipeline: &PipelineId,
    epoch: Epoch,
) -> Result<()> {
    let mut staged = Vec::new();
    for row in query(transaction, &planner.owned_by(pipeline))? {
        let name = row.first().map_or(Ok(String::new()), text)?;
        // A record whose name the database takes for another table is left as it is: every
        // change of that table is refused, and nothing of it is touched here.
        let table = match owned(transaction, planner, pipeline, &name) {
            Ok(table) => table,
            Err(refused) if refused.code() == Some("table_unowned") => continue,
            Err(error) => return Err(error),
        };
        let staging = planner.staging_table(&name);
        if columns(transaction, planner.dialect(), &staging)?.is_empty() {
            run_all(transaction, &planner.release(&table))?;
        } else {
            staged.push(table);
        }
    }
    run_all(transaction, &planner.discard(pipeline, epoch, &staged))
}

/// Refuses `table` where another table's derived tables or indexes would take a name of its own.
pub(super) fn distinct(
    transaction: &Transaction<'_>,
    planner: &SqlPlanner<Sqlite>,
    table: &TableRef,
) -> Result<()> {
    let texts = |row: &Vec<Value>, index: usize| row.get(index).map_or(Ok(String::new()), text);
    let owned = query(transaction, &planner.owned_tables())?
        .iter()
        .map(|row| texts(row, 0))
        .collect::<Result<Vec<_>>>()?;
    let generations = query(transaction, &planner.generation_tables())?
        .iter()
        .map(|row| Ok((texts(row, 0)?, texts(row, 1)?)))
        .collect::<Result<Vec<_>>>()?;
    planner.distinct(table, &owned, &generations)
}
