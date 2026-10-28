//! What a commit's transaction does: reading the fence row, then publishing, swapping
//! generations in, and dropping tables, each table checked against its owner record first.

use rdlt_connector::prelude::*;
use rdlt_connector::sqlgen::{self, Owned, SqlPlanner, Sqlite, Staged};
use rdlt_connector::{Epoch, Fence, GenerationId, PipelineId, SegmentSet};
use rusqlite::Transaction;
use rusqlite::types::Value;

use super::super::database::{columns, integer, query, run, run_all, text};
use super::Columns;
use super::owners::{owned, standing};

/// `pipeline`'s fence row: its epoch and the receipt of its last commit that landed, read
/// under the transaction's write lock.
pub(super) fn fence(
    transaction: &Transaction<'_>,
    planner: &SqlPlanner<Sqlite>,
    pipeline: &PipelineId,
) -> Result<Fence> {
    let rows = query(transaction, &planner.fence(pipeline))?;
    let Some(row) = rows.first() else {
        return Err(ConnectorError::internal(format!(
            "pipeline {pipeline} has no fence row"
        )));
    };
    let [epoch, landed @ ..] = &row[..] else {
        return Err(ConnectorError::internal("a fence row has no epoch"));
    };
    let epoch = Epoch(sqlgen::unsigned(integer(epoch)?));
    let landed = match landed {
        [
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ] => None,
        [key, seq, at, rows, bytes] => Some(sqlgen::receipt(
            integer(key)?,
            integer(seq)?,
            integer(at)?,
            integer(rows)?,
            integer(bytes)?,
        )),
        _ => {
            return Err(ConnectorError::internal("a fence row has missing fields"));
        }
    };
    Ok(Fence { epoch, landed })
}

/// Publishes what this session staged in `meta`'s segments into each table its pipeline owns,
/// the child tables that follow a staged root included, its rows of each table with the columns
/// `noted` holds for it; returns the rows and bytes published.
pub(super) fn publish(
    transaction: &Transaction<'_>,
    planner: &SqlPlanner<Sqlite>,
    (pipeline, epoch): (&PipelineId, Epoch),
    meta: &CommitMeta,
    noted: &Columns,
) -> Result<(i64, i64)> {
    let (mut rows, mut bytes) = (0, 0);
    let mut staged = Vec::new();
    for row in query(
        transaction,
        &planner.staged(pipeline, epoch, &meta.segments)?,
    )? {
        let (table, count, size) = staged_segment(&row)?;
        staged.push(table);
        rows += count;
        bytes += size;
    }
    let written: Vec<String> = staged.iter().map(|staged| staged.name.clone()).collect();
    for staged in planner.publishing(staged, &meta.child_tables) {
        let target = match staged.generation {
            Some(generation) => planner.generation_table(&staged.name, generation),
            None => staged.name.clone(),
        };
        let standing = standing(transaction, planner, &staged.name)?;
        let columns = columns(transaction, planner.dialect(), &target)?;
        // A child table the commit names that no row has reached is not created yet: it holds
        // no children to replace.
        let unreached = columns.is_empty() && standing.unowned();
        if unreached && !written.contains(&staged.name) {
            continue;
        }
        let table = standing.owned(pipeline)?;
        let staging = (staged.name.clone(), staged.generation);
        let names = noted.get(&staging).into_iter().flatten();
        let names: Vec<&str> = names.map(String::as_str).collect();
        let plan = planner.publish(&table, &staged, &columns, &names, epoch, &meta.segments)?;
        run_all(transaction, &plan)?;
    }
    run(
        transaction,
        &planner.forget(pipeline, epoch, &meta.segments)?,
    )?;
    abandon(transaction, planner, (pipeline, epoch), &meta.abandoned)?;
    Ok((rows, bytes))
}

/// Removes what this session staged in `abandoned`, segments the load abandoned, which nothing
/// publishes.
fn abandon(
    transaction: &Transaction<'_>,
    planner: &SqlPlanner<Sqlite>,
    (pipeline, epoch): (&PipelineId, Epoch),
    abandoned: &SegmentSet,
) -> Result<()> {
    if abandoned.is_empty() {
        return Ok(());
    }
    for row in query(transaction, &planner.staged(pipeline, epoch, abandoned)?)? {
        let (staged, ..) = staged_segment(&row)?;
        let table = owned(transaction, planner, pipeline, &staged.name)?;
        run(
            transaction,
            &planner.abandon(&table, &staged, (pipeline, epoch), abandoned)?,
        )?;
    }
    run(transaction, &planner.forget(pipeline, epoch, abandoned)?)?;
    Ok(())
}

/// A row of [`SqlPlanner::staged`]: what a table staged, with its rows and bytes.
fn staged_segment(row: &[Value]) -> Result<(Staged, i64, i64)> {
    let [name, generation, key, seq, count, size] = row else {
        return Err(ConnectorError::internal(
            "a staged segment has missing fields",
        ));
    };
    let generation = match generation {
        Value::Null => None,
        other => Some(GenerationId(sqlgen::unsigned(integer(other)?))),
    };
    let merge = match (key, seq) {
        (Value::Text(key), Value::Text(seq)) => Some(sqlgen::merge_key(key, seq)?),
        _ => None,
    };
    let staged = Staged {
        name: text(name)?,
        generation,
        merge,
    };
    Ok((staged, integer(count)?, integer(size)?))
}

/// Swaps in each generation `meta` finishes as the table its pipeline registered for the path.
///
/// A path the pipeline registered no table for names a table it never created, which has no
/// generation to swap in.
pub(super) fn finish(
    transaction: &Transaction<'_>,
    planner: &SqlPlanner<Sqlite>,
    pipeline: &PipelineId,
    meta: &CommitMeta,
) -> Result<()> {
    for (path, generation) in &meta.finish_generations {
        let found = query(transaction, &planner.table_name(pipeline, path))?;
        let Some(name) = found.first().and_then(|row| row.first()) else {
            continue;
        };
        let table = owned(transaction, planner, pipeline, &text(name)?)?;
        swap(transaction, planner, &table, *generation)?;
    }
    Ok(())
}

/// Drops each table `meta` drops; one that no pipeline owns and that does not exist was dropped
/// by an earlier try of the commit.
pub(super) fn drop_tables(
    transaction: &Transaction<'_>,
    planner: &SqlPlanner<Sqlite>,
    pipeline: &PipelineId,
    meta: &CommitMeta,
) -> Result<()> {
    for dropped in &meta.drop_tables {
        let Some(table) = standing(transaction, planner, &dropped.name)?.dropped(pipeline)? else {
            continue;
        };
        let generations = generation_tables(transaction, planner, table.name())?
            .into_iter()
            .map(|(generation, _)| generation)
            .collect::<Vec<_>>();
        run_all(transaction, &planner.drop_table(&table, &generations)?)?;
    }
    Ok(())
}

/// The generation tables of the table `name`, with their generations.
fn generation_tables(
    transaction: &Transaction<'_>,
    planner: &SqlPlanner<Sqlite>,
    name: &str,
) -> Result<Vec<(String, GenerationId)>> {
    query(transaction, &planner.generations(name))?
        .iter()
        .map(|row| match &row[..] {
            [table, found] => Ok((
                text(table)?,
                GenerationId(sqlgen::unsigned(integer(found)?)),
            )),
            _ => Err(ConnectorError::internal("a generation has missing fields")),
        })
        .collect()
}

/// Swaps `generation` in as `table`.
fn swap(
    transaction: &Transaction<'_>,
    planner: &SqlPlanner<Sqlite>,
    table: &Owned<'_>,
    generation: GenerationId,
) -> Result<()> {
    let generations = generation_tables(transaction, planner, table.name())?;
    let exists = !columns(transaction, planner.dialect(), table.name())?.is_empty();
    run_all(
        transaction,
        &planner.swap(table, exists, generation, &generations)?,
    )?;
    // The rows a change stream removed from the table swapped out never come back to its successor.
    let tombstones = planner.tombstone_table(table.name());
    if !columns(transaction, planner.dialect(), &tombstones)?.is_empty() {
        run(transaction, &planner.forget_tombstones(table)?)?;
    }
    Ok(())
}
