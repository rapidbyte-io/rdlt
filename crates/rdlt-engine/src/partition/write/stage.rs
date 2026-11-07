//! The transform stage on a flush's units: each batch run on the compute pool inside one
//! request of lowering's bytes, whose rest its lowering is lent.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{Fields, SchemaRef};
use rdlt_connector::StreamName;
use rdlt_connector::cost::{Allocations, ViewBuffers, arrays, listed_bytes};

use super::held::Held;
use super::units::Units;
use super::{Room, Schemas, reserving};
use crate::budget::{Reservation, TooLarge};
use crate::error::Error;
use crate::partition::{OpenSegment, PartitionContext, PartitionJob};
use crate::report::FilteredRows;
use crate::transform::stage::{Chain, Refusal, Stage, refused};

/// `units` through `stage`.
///
/// A stage that only includes columns runs on the partition's task on a unit whose batches
/// carry no unchanged flags: it shares every array it keeps and copies no row, allocating only
/// for each column, never for each row, and reserves nothing. Where the stage transforms, or a
/// unit's flags are to be moved, the flush holds one request of lowering's bytes, reserved while
/// the partition holds only its pushes, or what a JSON flush holds for its lowering already. Each
/// such unit's batches run on the pool within what is left of it, its transforms and the stage's
/// own work on its rows asking for room before they run. What they keep alive beyond the unit's
/// pushes is held with them, and the rest is lent to the write, so the partition never waits on
/// the budget while it holds what its transforms made.
///
/// A unit whose every row a filter drops is let go, with what its pushes were charged; `open`
/// counts what the filters did.
pub(super) async fn staged(
    job: &PartitionJob,
    context: &PartitionContext,
    open: &mut OpenSegment,
    stage: &Arc<Stage>,
    schemas: &mut Schemas,
    units: Units,
) -> Result<Units, Error> {
    let Units { units, room } = units;
    let reserving = |held: &Held| {
        let mut batches = held.batches.iter();
        batches.any(|batch| stage.reserves(batch.schema_ref()))
    };
    if !units.iter().any(reserving) {
        let mut kept = Vec::with_capacity(units.len());
        for held in units {
            kept.extend(projected(job, stage, schemas, held)?);
        }
        return Ok(Units { units: kept, room });
    }
    let mut reserved = whole(job, context, room).await?;
    let mut staged = Vec::with_capacity(units.len());
    for held in units {
        if !reserving(&held) {
            staged.extend(projected(job, stage, schemas, held)?);
            continue;
        }
        let running = (stage, &mut *schemas);
        let (held, filtered) = transformed(job, context, running, held, &mut reserved).await?;
        open.filtered.add(filtered);
        staged.extend(held);
    }
    let room = Room::Lent(reserved.lend());
    Ok(Units {
        units: staged,
        room,
    })
}

/// One request of lowering's bytes for the flush: reserved now, or taken whole from what is
/// lent to it, which then lends no more.
async fn whole(
    job: &PartitionJob,
    context: &PartitionContext,
    room: Room,
) -> Result<Reservation, Error> {
    match room {
        Room::Budget => {
            let request = context.budget.shares().request;
            let too_large = |large: TooLarge| {
                Error::internal(format!(
                    "stream {}: one request of {} bytes is refused as more than the {} a request \
                     may take",
                    job.stream, large.asked, large.limit
                ))
                .with_stream(&job.stream)
            };
            reserving(job, context, request, too_large).await
        }
        Room::Lent(lent) => {
            let taken = lent.try_take(lent.bytes()).ok_or_else(|| {
                Error::internal(format!(
                    "stream {}: a flush's room was taken before its transforms ran",
                    job.stream
                ))
                .with_stream(&job.stream)
            })?;
            lent.close();
            Ok(taken)
        }
    }
}

/// `held`'s batches through `stage`, each on the pool within what `reserved` has left, and what
/// the stage's filters did.
///
/// What the batches keep alive beyond the pushes, each allocation counted once however many of
/// them share it, is split from `reserved` and held with them. Where no row is left of a unit
/// that had rows, the pushes are let go and there is no unit.
async fn transformed(
    job: &PartitionJob,
    context: &PartitionContext,
    (stage, schemas): (&Arc<Stage>, &mut Schemas),
    held: Held,
    reserved: &mut Reservation,
) -> Result<(Option<Held>, FilteredRows), Error> {
    let Held {
        batches,
        mut permits,
    } = held;
    // The pushes stay alive until what the outputs keep beyond them is measured.
    let pushed: Allocations = batches.iter().collect();
    let columns: BTreeSet<usize> = batches
        .iter()
        .flat_map(RecordBatch::columns)
        .map(address)
        .collect();
    let mut made = Allocations::default();
    let (mut outputs, mut arranged) = (Vec::with_capacity(batches.len()), 0_u64);
    let mut filtered = FilteredRows::default();
    for batch in &batches {
        let chain = chained(job, context, stage, schemas, batch.schema_ref()).await?;
        let grown = made.bytes().saturating_add(arranged);
        let free = reserved.bytes().saturating_sub(grown);
        let (running, batch) = (Arc::clone(stage), batch.clone());
        let run = move || running.run(&chain, batch, free);
        let ran = context.pool.run_all([run]).await.pop();
        let ran = ran.ok_or_else(|| Error::internal("the pool returned no batch"))?;
        let ran = ran.map_err(|refusal| refused(&job.stream, refusal))?;
        for output in &ran.batches {
            made.add_beyond(output, &pushed);
            let listed = listed_bytes(ViewBuffers::of(output).beyond_first);
            arranged = arranged
                .saturating_add(new_arrays(output, &columns))
                .saturating_add(listed);
        }
        filtered.add(FilteredRows {
            dropped: ran.dropped,
            deleted: ran.deleted,
        });
        outputs.extend(ran.batches);
    }
    let grown = made.bytes().saturating_add(arranged);
    fitted(&job.stream, stage.last_transform(), grown, reserved.bytes())?;
    if outputs.is_empty() {
        return Ok((None, filtered));
    }
    permits.push(Box::new(reserved.split(grown)));
    Ok((Some(Held::of(outputs, permits)), filtered))
}

/// Where an array starts, which tells it from another.
fn address(array: &ArrayRef) -> usize {
    Arc::as_ptr(array).cast::<()>().addr()
}

/// Bytes: the arrays of `output`'s columns that are none of the pushes' own, whose `columns`
/// holds where each starts.
fn new_arrays(output: &RecordBatch, columns: &BTreeSet<usize>) -> u64 {
    let fields = output.schema_ref().fields().iter();
    let made = fields
        .zip(output.columns())
        .filter(|(_, column)| !columns.contains(&address(column)));
    let made: Fields = made.map(|(field, _)| Arc::clone(field)).collect();
    arrays(&made)
}

/// Refuses a unit of `stream` whose batches, as `transform` left them or as moving their flags
/// did where that is none, keep `grown` bytes alive beyond their pushes, where that passes the
/// `free` bytes left of the flush's request: the stage measures each part as it runs, which a
/// part's schema and lists of view buffers may still pass.
pub(super) fn fitted(
    stream: &StreamName,
    transform: Option<&str>,
    grown: u64,
    free: u64,
) -> Result<(), Error> {
    if grown > free {
        let transform = transform.map(str::to_owned);
        let refusal = Refusal::Flush {
            transform,
            asked: grown,
            free,
        };
        return Err(refused(stream, refusal));
    }
    Ok(())
}

/// The chain `stage` makes of `input`: one kept, or else made on the pool and kept.
async fn chained(
    job: &PartitionJob,
    context: &PartitionContext,
    stage: &Arc<Stage>,
    schemas: &mut Schemas,
    input: &SchemaRef,
) -> Result<Arc<Chain>, Error> {
    if let Some(chain) = schemas.chain(input) {
        return Ok(chain);
    }
    let (stage, schema) = (Arc::clone(stage), Arc::clone(input));
    let made = context
        .pool
        .run_all([move || stage.chain(&schema)])
        .await
        .pop();
    let made = made.ok_or_else(|| Error::internal("the pool returned no chain"))?;
    let chain = Arc::new(made.map_err(|refusal| refused(&job.stream, refusal))?);
    schemas.keep_chain(Arc::clone(&chain));
    Ok(chain)
}

/// `held`'s batches through a stage that only includes columns of batches that carry no
/// unchanged flags, on the partition's task; none where it held no batch.
fn projected(
    job: &PartitionJob,
    stage: &Stage,
    schemas: &mut Schemas,
    held: Held,
) -> Result<Option<Held>, Error> {
    let Held { batches, permits } = held;
    let mut outputs: Vec<RecordBatch> = Vec::with_capacity(batches.len());
    for batch in batches {
        let chain = if let Some(chain) = schemas.chain(batch.schema_ref()) {
            chain
        } else {
            let made = stage.chain(batch.schema_ref());
            let chain = Arc::new(made.map_err(|refusal| refused(&job.stream, refusal))?);
            schemas.keep_chain(Arc::clone(&chain));
            chain
        };
        let ran = stage.run(&chain, batch, u64::MAX);
        outputs.extend(
            ran.map_err(|refusal| refused(&job.stream, refusal))?
                .batches,
        );
    }
    Ok((!outputs.is_empty()).then(|| Held::of(outputs, permits)))
}
