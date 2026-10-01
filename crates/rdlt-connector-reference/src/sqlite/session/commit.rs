//! What a commit's transaction does after its fence: publishing, swapping generations in, and
//! dropping tables, each table checked against its owner record first.

use rdlt_connector::prelude::*;
use rdlt_connector::sqlgen::{self, Owned, SqlPlanner, Sqlite, Staged};
use rdlt_connector::{CommitSeq, Epoch, GenerationId, LoadId, PipelineId};
use rusqlite::Transaction;
use rusqlite::types::Value;

use super::super::database::{columns, integer, query, run, run_all, text};
use super::owners::{owned, standing};

/// The receipt stored for `(load_id, commit_seq)`, if that commit already happened.
pub(super) fn stored(
    transaction: &Transaction<'_>,
    planner: &SqlPlanner<Sqlite>,
    pipeline: &PipelineId,
    load_id: LoadId,
    commit_seq: CommitSeq,
) -> Result<Option<Receipt>> {
    let rows = query(transaction, &planner.receipt(pipeline, load_id, commit_seq))?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    match &row[..] {
        [at, count, bytes] => Ok(Some(sqlgen::receipt(
            load_id,
            commit_seq,
            integer(at)?,
            integer(count)?,
            integer(bytes)?,
        ))),
        _ => Err(ConnectorError::internal(
            "a stored receipt has missing fields",
        )),
    }
}

/// Publishes what this session staged in `meta`'s segments into each table its pipeline owns,
/// the child tables that follow a staged root included; returns the rows and bytes published.
pub(super) fn publish(
    transaction: &Transaction<'_>,
    planner: &SqlPlanner<Sqlite>,
    pipeline: &PipelineId,
    epoch: Epoch,
    meta: &CommitMeta,
) -> Result<(i64, i64)> {
    let (mut rows, mut bytes) = (0, 0);
    let mut staged = Vec::new();
    for row in query(
        transaction,
        &planner.staged(pipeline, epoch, &meta.segments),
    )? {
        let (table, count, size) = staged_segment(&row)?;
        staged.push(table);
        rows += count;
        bytes += size;
    }
    for staged in planner.publishing(staged, &meta.child_tables) {
        let table = owned(transaction, planner, pipeline, &staged.name)?;
        let target = match staged.generation {
            Some(generation) => planner.generation_table(&staged.name, generation),
            None => staged.name.clone(),
        };
        let columns = columns(transaction, planner.dialect(), &target)?;
        run_all(
            transaction,
            &planner.publish(&table, &staged, &columns, epoch, &meta.segments)?,
        )?;
    }
    run(
        transaction,
        &planner.forget(pipeline, epoch, &meta.segments),
    )?;
    Ok((rows, bytes))
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
        run(transaction, &planner.forget_tombstones(table))?;
    }
    Ok(())
}
