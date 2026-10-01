//! Who owns a destination table: claims, and the owner check every change of a table starts from.

use rdlt_connector::prelude::*;
use rdlt_connector::sqlgen::{Owned, SqlPlanner, Sqlite};
use rdlt_connector::{Epoch, PipelineId};
use rusqlite::Transaction;

use super::super::database::{query, run, run_all, text};

/// The pipeline the owner record of the table `name` names, if it has one.
pub(super) fn owner(
    transaction: &Transaction<'_>,
    planner: &SqlPlanner<Sqlite>,
    name: &str,
) -> Result<Option<String>> {
    query(transaction, &planner.owner(name))?
        .first()
        .and_then(|row| row.first())
        .map(text)
        .transpose()
}

/// The table `name` as `pipeline`'s session may change it: refused under a name the destination
/// keeps, without an owner record, or as `table_owned` where another pipeline owns it.
pub(super) fn owned(
    transaction: &Transaction<'_>,
    planner: &SqlPlanner<Sqlite>,
    pipeline: &PipelineId,
    name: &str,
) -> Result<Owned> {
    planner.named(name)?;
    let owner = owner(transaction, planner, name)?;
    planner.owned(pipeline, name, owner.as_deref())
}

/// Claims the table `name` for `pipeline`'s session at `epoch` where no pipeline owns it; another
/// pipeline's table is refused as `table_owned`, and a claim by a session a newer one fenced as
/// fenced, since a drop may have released the table from it.
pub(super) fn claim(
    transaction: &Transaction<'_>,
    planner: &SqlPlanner<Sqlite>,
    pipeline: &PipelineId,
    epoch: Epoch,
    name: &str,
) -> Result<Owned> {
    let claiming = planner.claim(pipeline, name)?;
    if owner(transaction, planner, name)?.is_none()
        && run(transaction, &planner.fence(pipeline, epoch))? == 0
    {
        return Err(ConnectorError::fenced(format!(
            "pipeline {pipeline} has a session newer than epoch {epoch}"
        )));
    }
    run_all(transaction, &claiming)?;
    owned(transaction, planner, pipeline, name)
}

/// Every table `pipeline` owns.
pub(super) fn owned_by(
    transaction: &Transaction<'_>,
    planner: &SqlPlanner<Sqlite>,
    pipeline: &PipelineId,
) -> Result<Vec<Owned>> {
    query(transaction, &planner.owned_by(pipeline))?
        .iter()
        .map(|row| {
            let name = row.first().map_or(Ok(String::new()), text)?;
            planner.owned(pipeline, &name, Some(pipeline.as_str()))
        })
        .collect()
}

/// Refuses `table` where another table's derived tables or indexes would take a name of its own.
pub(super) fn distinct(
    transaction: &Transaction<'_>,
    planner: &SqlPlanner<Sqlite>,
    table: &TableRef,
) -> Result<()> {
    let texts = |row: &Vec<rusqlite::types::Value>, index: usize| {
        row.get(index).map_or(Ok(String::new()), text)
    };
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
