//! Who owns a destination table: claims, and the owner checks of the commits that swap or drop.

use rdlt_connector::prelude::*;
use rdlt_connector::sqlgen::{SqlPlanner, Sqlite};
use rdlt_connector::{Epoch, PipelineId};
use rusqlite::Transaction;

use super::super::database::{query, run, run_all, text};

/// Refuses the table `name` as `table_owned` where a pipeline other than `pipeline` owns it.
pub(super) fn owned(
    transaction: &Transaction<'_>,
    planner: &SqlPlanner<Sqlite>,
    pipeline: &PipelineId,
    name: &str,
) -> Result<()> {
    let owner = query(transaction, &planner.owner(name))?
        .first()
        .and_then(|row| row.first())
        .map(text)
        .transpose()?;
    match owner {
        Some(owner) if owner != pipeline.as_str() => Err(ConnectorError::table_owned(name, &owner)),
        _ => Ok(()),
    }
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
) -> Result<()> {
    let owner = |transaction: &Transaction<'_>| -> Result<Option<String>> {
        query(transaction, &planner.owner(name))?
            .first()
            .and_then(|row| row.first())
            .map(text)
            .transpose()
    };
    if owner(transaction)?.is_none() && run(transaction, &planner.fence(pipeline, epoch))? == 0 {
        return Err(ConnectorError::fenced(format!(
            "pipeline {pipeline} has a session newer than epoch {epoch}"
        )));
    }
    run_all(transaction, &planner.claim(pipeline, name))?;
    let owner = owner(transaction)?.unwrap_or_default();
    if owner == pipeline.as_str() {
        Ok(())
    } else {
        Err(ConnectorError::table_owned(name, &owner))
    }
}
