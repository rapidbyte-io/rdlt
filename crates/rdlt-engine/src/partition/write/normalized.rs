//! Writing the units of a normalized stream: each normalized into its table's and child tables'
//! parts, planned parents first so dropped rows change no schema, and lowered on the pool.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::ArrowError;
use parking_lot::Mutex;
use rdlt_connector::{Permit, StreamName};

use super::{Held, OpenSegment, PartitionContext, PartitionJob, queue, schema_of, stamp, windows};
use crate::budget::MemoryBudget;
use crate::compute::run_all;
use crate::error::Error;
use crate::normalize::{self, Dropped, Part, Pruned, Shape};
use crate::table::{Admission, Incoming, LoweringPlan, Prepared, Stamp};

/// Lowers `units` of a stream that normalizes as `shape` into its table and child tables, and
/// queues them on their lanes, a window of units at a time.
///
/// Each unit is concatenated and normalized on the compute pool. Its parts' tables and plans are
/// then found in order, since finding them may add child tables or change tables, and the parts
/// are lowered on the pool. A unit's parts share the permits holding its memory, charged with its
/// growth before any part waits on its lane.
pub(super) async fn write_normalized(
    job: &PartitionJob,
    context: &PartitionContext,
    open: &mut OpenSegment,
    units: Vec<(Vec<RecordBatch>, Held)>,
    shape: &Arc<Shape>,
) -> Result<(), Error> {
    for window in windows(units) {
        let (batches, reservations): (Vec<_>, Vec<_>) = window.into_iter().unzip();
        let jobs = batches.into_iter().map(|parts| {
            let (shape, stream) = (Arc::clone(shape), job.stream.clone());
            move || split(&stream, &parts, &shape)
        });
        let split = run_all(context.env.compute(), jobs).await;
        let mut planned = Vec::with_capacity(split.len());
        for (parts, held) in split.into_iter().zip(reservations) {
            let parts = parts?;
            // The parts wait on their tables' changes, so their memory is held from now.
            let held = charge_parts(&context.budget, &parts, held);
            let received = parts.first().map_or(0, |part| part.batch.num_rows() as u64);
            if received == 0 {
                continue;
            }
            let stamp = stamp(context, open, received);
            let (unit, discarded) = plan_parts(job, context, parts).await?;
            open.discarded_values += discarded.values;
            open.discarded_rows += discarded.rows;
            let lower_unit = move || lower_unit(unit, &stamp);
            planned.push((lower_unit, held));
        }
        let (jobs, reservations): (Vec<_>, Vec<_>) = planned.into_iter().unzip();
        let lowered: Vec<_> = run_all(context.env.compute(), jobs)
            .await
            .into_iter()
            .zip(reservations)
            .map(|(prepared, held)| {
                prepared.map(|prepared| {
                    let shared = share_growth(&context.budget, &prepared, held);
                    (prepared, shared)
                })
            })
            .collect();
        for lowered in lowered {
            let (prepared, shared) = lowered?;
            for (table, prepared) in prepared {
                let reservation: Permit = Box::new(Arc::clone(&shared));
                queue(job, context, open, table, prepared, reservation).await?;
            }
        }
    }
    Ok(())
}

/// The parts of a unit, each with its table and plan.
type PlannedParts = Vec<(usize, Part, Arc<LoweringPlan>)>;

/// What planning a unit's parts discarded: the values of new arrays, and the rows that went with
/// dropped parents.
#[derive(Default)]
struct Discarded {
    values: u64,
    rows: u64,
}

/// The table and plan of each of `parts`, a unit's, found in order, parents first, and what they
/// discarded.
///
/// Each part first loses the rows whose parent was dropped, and a part left without rows is
/// neither planned nor given a table, so dropped rows change no schema. The rows its own policy
/// drops then go for the parts below.
async fn plan_parts(
    job: &PartitionJob,
    context: &PartitionContext,
    parts: Vec<Part>,
) -> Result<(PlannedParts, Discarded), Error> {
    let (admitted, mut dropped, values) = admit(job, context, parts);
    let mut discarded = Discarded { values, rows: 0 };
    let mut planned = Vec::with_capacity(admitted.len());
    for (part, refused) in admitted {
        let pruned = if dropped.is_empty() {
            Pruned::whole(part)
        } else {
            let pruning = move || {
                let pruned = dropped.prune(part);
                (dropped, pruned)
            };
            let (back, pruned) = on_pool(context, pruning).await;
            dropped = back;
            pruned.map_err(|error| pruning_failed(job, &error))?
        };
        discarded.rows += pruned.count;
        if pruned.part.batch.num_rows() == 0 {
            continue;
        }
        let path = &pruned.part.path;
        if refused {
            return Err(frozen(job, path));
        }
        let table = if path.is_empty() {
            job.table
        } else {
            context.tables.child(job.table, path).await?
        };
        let incoming = Incoming {
            schema: schema_of(job, &pruned.part.batch)?,
            paths: pruned.part.columns.clone(),
        };
        let plan = context.tables.plan(table, incoming).await?;
        if plan.drops_rows() {
            let (batch, rows) = (pruned.part.batch.clone(), Arc::clone(&plan));
            if let Some(kept) = on_pool(context, move || rows.kept(&batch)).await {
                dropped.unkept(&pruned, &kept);
            }
        }
        planned.push((table, pruned.part, plan));
    }
    Ok((planned, discarded))
}

/// The parts of `parts` their stream does not discard, parents first, each with whether its
/// frozen schema refuses it should any of its rows remain; the rows holding new arrays whose rows
/// its policy drops; and the values of new arrays it discards.
///
/// A part below the stream's table goes to its child table, added the first time unless the
/// stream's policy refuses or discards a new one, whose descendants then go with it, uncounted:
/// they are part of the array value already discarded.
fn admit(
    job: &PartitionJob,
    context: &PartitionContext,
    parts: Vec<Part>,
) -> (Vec<(Part, bool)>, Dropped, u64) {
    let existed = context.tables.view(job.table).model.created();
    let (mut admitted, mut dropped, mut values) =
        (Vec::with_capacity(parts.len()), Dropped::default(), 0);
    let mut skipped: BTreeSet<Vec<Arc<str>>> = BTreeSet::new();
    for part in parts {
        let parent = part.lineage.parent.as_ref();
        if parent.is_some_and(|parent| skipped.contains(&parent.path)) {
            skipped.insert(part.path);
            continue;
        }
        let mut refused = false;
        if !part.path.is_empty() {
            match context.tables.admit_child(job.table, &part.path, existed) {
                Admission::Add => {}
                Admission::Refuse => refused = true,
                Admission::Discard => {
                    values += part.batch.num_rows() as u64;
                    skipped.insert(part.path);
                    continue;
                }
                Admission::DiscardParents => {
                    dropped.parents_of(&part);
                    skipped.insert(part.path);
                    continue;
                }
            }
        }
        admitted.push((part, refused));
    }
    (admitted, dropped, values)
}

/// Runs `work` on the compute pool.
async fn on_pool<T: Send + 'static>(
    context: &PartitionContext,
    work: impl FnOnce() -> T + Send + 'static,
) -> T {
    run_all(context.env.compute(), [work])
        .await
        .into_iter()
        .next()
        .expect("the pool returns a result for each job")
}

fn pruning_failed(job: &PartitionJob, error: &ArrowError) -> Error {
    Error::internal(format!("stream {}: dropping children: {error}", job.stream))
}

/// `unit`'s parts as their plans lower them.
fn lower_unit(unit: PlannedParts, stamp: &Stamp) -> Result<Vec<(usize, Prepared)>, Error> {
    unit.into_iter()
        .map(|(table, part, plan)| {
            let prepared = plan.prepare(&part.batch, Some(&part.lineage), stamp)?;
            Ok((table, prepared))
        })
        .collect()
}

/// The error for rows of a new array in a stream whose schema is frozen.
fn frozen(job: &PartitionJob, path: &[Arc<str>]) -> Error {
    let array = path.join(".");
    Error::schema(format!(
        "stream {}: array {array}: a new array would add a child table to a frozen schema",
        job.stream
    ))
    .with_code("schema_frozen")
    .with_stream(&job.stream)
}

/// `parts`, one batch once concatenated, normalized as `shape`.
fn split(stream: &StreamName, parts: &[RecordBatch], shape: &Shape) -> Result<Vec<Part>, Error> {
    let failed = |error: ArrowError| {
        Error::internal(format!("stream {stream}: normalizing a batch: {error}"))
    };
    let batch = arrow_select::concat::concat_batches(&parts[0].schema(), parts).map_err(failed)?;
    normalize::normalize(&batch, shape).map_err(failed)
}

/// The permits of `held` with the growth of `prepared`, a unit's lowered parts, beyond them
/// charged, shared by the parts: the unit's memory is held until the last part is staged.
pub(super) fn share_growth(
    budget: &MemoryBudget,
    prepared: &[(usize, Prepared)],
    mut held: Held,
) -> Arc<Mutex<Vec<Permit>>> {
    let bytes: u64 = prepared
        .iter()
        .map(|(_, prepared)| {
            u64::try_from(prepared.batch.get_array_memory_size()).unwrap_or(u64::MAX)
        })
        .sum();
    held.permits
        .push(Box::new(budget.charge(bytes.saturating_sub(held.bytes))));
    Arc::new(Mutex::new(held.permits))
}

/// The memory `part`'s arrays take.
fn part_bytes(part: &Part) -> u64 {
    let lineage = &part.lineage;
    let mut arrays = vec![&lineage.id, &lineage.root_row];
    if let Some(parent) = &lineage.parent {
        arrays.extend([&parent.id, &parent.root, &parent.idx, &parent.row]);
    }
    let bytes = part.batch.get_array_memory_size()
        + arrays
            .into_iter()
            .map(|array| array.get_array_memory_size())
            .sum::<usize>();
    u64::try_from(bytes).unwrap_or(u64::MAX)
}

/// `held` with the growth of `parts`, a unit's normalized parts, beyond it charged.
pub(super) fn charge_parts(budget: &MemoryBudget, parts: &[Part], mut held: Held) -> Held {
    let bytes: u64 = parts.iter().map(part_bytes).sum();
    held.permits
        .push(Box::new(budget.charge(bytes.saturating_sub(held.bytes))));
    held.bytes = held.bytes.max(bytes);
    held
}
