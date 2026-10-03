//! Writing the units of a normalized stream: each normalized into its table's and child tables'
//! parts, planned parents first so dropped rows change no schema, and lowered on the pool.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::ArrowError;
use parking_lot::Mutex;
use rdlt_connector::cost::Allocations;
use rdlt_connector::{ColumnPath, Permit, StreamName};

use super::allowance::{Cutter, PartPiece};
use super::pieces::{self, Lowered};
use super::{
    Held, OpenSegment, PartitionContext, PartitionJob, full, queue, reserve, row_too_large,
    schema_of, stamp,
};
use crate::budget::Shares;
use crate::compute::run_all;
use crate::cost::{LINEAGE_ITEM, LINEAGE_ROW, SPLIT_COPIES};
use crate::error::Error;
use crate::limits::MIN_PIECE;
use crate::normalize::{self, Dropped, Part, Pruned, Shape};
use crate::table::{Admission, Incoming, LoweringPlan, Prepared, Stamp};

/// Lowers `units` of a stream that normalizes as `shape` into its table and child tables, and
/// queues them on their lanes.
///
/// - Each unit is cut, before its tables are known, into pieces whose split makes no more than a
///   piece may hold.
/// - Each piece then asks the budget once, for all a request may take: what its split makes and
///   an allowance for lowering its parts. It is split on the compute pool, its parts' tables and
///   plans are found in order, parents first, and each part is cut by what lowering it into its
///   own table holds. What the piece asked for beyond what it needs is given back.
/// - The parts' pieces are lowered inside the allowance: the partition waits for its own pieces
///   to be written, never for the budget, while it holds any of them.
///
/// The units are one flush, so each table's integers are judged over all of them, as the rows
/// arrive: where the flush was cut decides no column's type.
pub(super) async fn write_normalized(
    job: &PartitionJob,
    context: &PartitionContext,
    open: &mut OpenSegment,
    units: Vec<(Vec<RecordBatch>, Held)>,
    shape: &Arc<Shape>,
) -> Result<(), Error> {
    let pieces = sliced(job, context, units).await?;
    let cut = pieces
        .iter()
        .map(|(parts, _, bytes)| (parts.clone(), *bytes));
    let rounding = judged(job, context, cut.collect(), shape).await?;
    let request = context.budget.shares().request;
    for (parts, held, _) in pieces {
        let mut reserved = reserve(job, context, request).await?;
        let (shape, stream) = (Arc::clone(shape), job.stream.clone());
        let parts = on_pool(context, move || split(&stream, &parts, &shape)).await?;
        // What the split made beside the unit is held until the last of it is written.
        let made = {
            let mut allocations = held.allocations.lock();
            let made = parts.iter().map(|part| part_growth(part, &mut allocations));
            made.fold(0, u64::saturating_add)
        };
        let received = parts.first().map_or(0, |part| part.batch.num_rows() as u64);
        if received == 0 {
            continue;
        }
        let stamp = stamp(context, open, received);
        let (unit, discarded) = plan_parts(job, context, parts, &rounding).await?;
        open.discarded_values += discarded.values;
        open.discarded_rows += discarded.rows;
        let cutter = Cutter::within(context, request.saturating_sub(made));
        let rendering = context.rendering.as_ref().clone();
        let cut = on_pool(context, move || cutter.cut(&rendering, unit)).await;
        let cut = cut.map_err(|row| row_too_large(job, &row))?;
        // The allowance is as much as the pieces lowered at once take: the rest is given back.
        let allowance = cutter.for_pieces(&cut);
        reserved.shrink(made.saturating_add(allowance.bytes));
        let hold = Arc::new(Mutex::new((reserved, held.permits)));
        let mut cut = cut.into_iter().peekable();
        while let Some(piece) = cut.next() {
            // The partition waits for its allowance only while it holds no piece it has not
            // handed to its lane; the pieces the allowance has room for are lowered together.
            let taken = allowance.take(&context.budget, &context.cancel, &piece);
            let taken = taken.await?;
            let mut window = vec![(piece, taken)];
            while !full(window.len()) {
                let Some(taken) = cut.peek().and_then(|next| allowance.try_take(next)) else {
                    break;
                };
                window.extend(cut.next().map(|piece| (piece, taken)));
            }
            let (pieces, taken): (Vec<_>, Vec<_>) = window.into_iter().unzip();
            let tables: Vec<usize> = pieces.iter().map(|piece| piece.table).collect();
            let prepared = lower(context, pieces, stamp).await;
            for ((prepared, taken), table) in prepared.into_iter().zip(taken).zip(tables) {
                // The piece on its lane and its frame in the log each hold their part of the
                // allowance, and with it what the piece reserved, until they are written.
                let part = |taken| Box::new((taken, Arc::clone(&hold))) as Permit;
                let permits = (part(taken.piece), taken.frame.map(part));
                queue(job, context, open, table, prepared?, permits).await?;
            }
        }
    }
    Ok(())
}

/// `pieces` lowered on the compute pool, in order: each in a job of its own, or all in one where
/// they are small, so a unit of a few rows takes no trip to the pool for each of its parts.
async fn lower(
    context: &PartitionContext,
    pieces: Vec<PartPiece>,
    stamp: Stamp,
) -> Vec<Result<Prepared, Error>> {
    let lowering = move |piece: PartPiece| {
        let lineage = Some(&piece.lineage);
        piece.plan.prepare(&piece.batch, lineage, &stamp, None)
    };
    let bytes = pieces.iter().map(|piece| piece.bytes);
    if bytes.fold(0, u64::saturating_add) <= MIN_PIECE {
        return on_pool(context, move || pieces.into_iter().map(lowering).collect()).await;
    }
    let lowerings = pieces.into_iter().map(|piece| move || lowering(piece));
    run_all(context.env.compute(), lowerings).await
}

/// Bytes: the most the rows of a piece cut before its split may take as they arrive, and a row's
/// alone: what the split makes is twice that, within a piece's share of `shares`, and a row's
/// within half a request's.
pub(super) fn split_bounds(shares: Shares) -> (u64, u64) {
    (
        shares.piece / SPLIT_COPIES,
        shares.request / SPLIT_COPIES / 2,
    )
}

/// `units` cut into pieces by what splitting their rows makes, on the compute pool: a normalized
/// unit's tables are known only once it is split, so its pieces are cut before, as they arrive,
/// and their parts again by their own tables once they are.
async fn sliced(
    job: &PartitionJob,
    context: &PartitionContext,
    units: Vec<(Vec<RecordBatch>, Held)>,
) -> Result<Vec<(Vec<RecordBatch>, Held, u64)>, Error> {
    let (max, limit) = split_bounds(context.budget.shares());
    let lowered = Lowered {
        rendering: context.rendering.as_ref().clone(),
        stored: Vec::new(),
        row: LINEAGE_ROW,
        item: LINEAGE_ITEM,
        // What the split makes is measured as rows expand, and may be twice that.
        max,
        limit,
    };
    let mut cut = Vec::with_capacity(units.len());
    for (parts, held) in units {
        let lowered = lowered.clone();
        let pieces = on_pool(context, move || pieces::sliced(parts, held, lowered)).await;
        cut.extend(pieces.map_err(|row| row_too_large(job, &row))?);
    }
    Ok(cut)
}

/// The parts of a unit, each with its table and plan.
pub(super) type PlannedParts = Vec<(usize, Part, Arc<LoweringPlan>)>;

/// What planning a unit's parts discarded: the values of new arrays, and the rows that went with
/// dropped parents.
#[derive(Default)]
struct Discarded {
    values: u64,
    rows: u64,
}

/// What becomes of one of a unit's parts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fate {
    /// Its table takes it, or refuses it, as `refused` says, should any of its rows remain.
    Taken { refused: Option<Refused> },
    /// It is a new array its stream discards: its rows whose parents load are discarded values.
    Discarded,
}

/// Why a new child table refuses a part's rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Refused {
    /// The stream's schema is frozen.
    Frozen,
    /// No table can be named after the part's path.
    Unnamable,
}

/// The table and plan of each of `parts`, a unit's, found in order, parents first, and what they
/// discarded.
///
/// Each part first loses the rows whose parent was dropped, and a part left without rows is
/// neither planned nor given a table, so dropped rows change no schema. The rows its own policy
/// drops then go for the parts below. A discarded array's items count as discarded values only
/// where their rows load.
async fn plan_parts(
    job: &PartitionJob,
    context: &PartitionContext,
    parts: Vec<Part>,
    judged: &Rounding,
) -> Result<(PlannedParts, Discarded), Error> {
    let (admitted, mut dropped) = admit(job, context, parts);
    let mut discarded = Discarded::default();
    let mut planned = Vec::with_capacity(admitted.len());
    for (part, fate) in admitted {
        let mut rounding = rounding_of(job, &part)?;
        rounding.extend(judged.get(&part.path).into_iter().flatten().cloned());
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
        if fate == Fate::Discarded {
            discarded.values += pruned.part.batch.num_rows() as u64;
            continue;
        }
        discarded.rows += pruned.count;
        if pruned.part.batch.num_rows() == 0 {
            continue;
        }
        let path = &pruned.part.path;
        match fate {
            Fate::Taken {
                refused: Some(Refused::Frozen),
            } => return Err(frozen(job, path)),
            Fate::Taken {
                refused: Some(Refused::Unnamable),
            } => return Err(unnamable(job, path)),
            _ => {}
        }
        let table = if path.is_empty() {
            job.table
        } else {
            context.tables.child(job.table, path).await?
        };
        let mut incoming = Incoming::of(
            schema_of(job, &pruned.part.batch)?,
            pruned.part.columns.clone(),
            &[],
        );
        incoming.rounding = rounding;
        let plan = context.tables.plan(table, incoming).await?;
        if plan.drops_rows() {
            unkept(job, context, &plan, &pruned, &mut dropped).await?;
        }
        planned.push((table, pruned.part, plan));
    }
    Ok((planned, discarded))
}

/// Records as dropped the rows of `pruned` that `plan`'s schema policy drops.
async fn unkept(
    job: &PartitionJob,
    context: &PartitionContext,
    plan: &Arc<LoweringPlan>,
    pruned: &Pruned,
    dropped: &mut Dropped,
) -> Result<(), Error> {
    let (batch, rows) = (pruned.part.batch.clone(), Arc::clone(plan));
    let kept = on_pool(context, move || rows.kept(&batch)).await;
    let kept = kept.map_err(|error| {
        Error::internal(format!("stream {}: reading kept rows: {error}", job.stream))
    })?;
    if let Some(kept) = kept {
        dropped.unkept(pruned, &kept);
    }
    Ok(())
}

/// Each of `parts`, parents first, with its fate, and the rows holding new arrays whose rows the
/// stream's policy drops.
///
/// A part below the stream's table goes to its child table, added the first time unless the
/// stream's policy refuses or discards a new one, whose descendants then go with it, uncounted:
/// they are part of the array value already discarded.
fn admit(
    job: &PartitionJob,
    context: &PartitionContext,
    parts: Vec<Part>,
) -> (Vec<(Part, Fate)>, Dropped) {
    let existed = context.tables.view(job.table).model.created();
    let (mut admitted, mut dropped) = (Vec::with_capacity(parts.len()), Dropped::default());
    let mut skipped: BTreeSet<Vec<Arc<str>>> = BTreeSet::new();
    for part in parts {
        let parent = part.lineage.parent.as_ref();
        if parent.is_some_and(|parent| skipped.contains(&parent.path)) {
            skipped.insert(part.path);
            continue;
        }
        let mut fate = Fate::Taken { refused: None };
        if !part.path.is_empty() {
            match context.tables.admit_child(job.table, &part.path, existed) {
                Admission::Add => {}
                Admission::Refuse => {
                    fate = Fate::Taken {
                        refused: Some(Refused::Frozen),
                    };
                }
                Admission::Unnamable => {
                    fate = Fate::Taken {
                        refused: Some(Refused::Unnamable),
                    };
                }
                Admission::Discard => {
                    skipped.insert(part.path.clone());
                    fate = Fate::Discarded;
                }
                Admission::DiscardParents => {
                    dropped.parents_of(&part);
                    skipped.insert(part.path);
                    continue;
                }
            }
        }
        admitted.push((part, fate));
    }
    (admitted, dropped)
}

/// The columns of 64-bit integers holding a value a float would round, by the path of their table.
pub(super) type Rounding = BTreeMap<Vec<Arc<str>>, BTreeSet<ColumnPath>>;

/// The columns of `part`'s table holding, in its rows, a value a 64-bit float would round.
fn rounding_of(job: &PartitionJob, part: &Part) -> Result<BTreeSet<ColumnPath>, Error> {
    let schema = schema_of(job, &part.batch)?;
    let paths = part.columns.clone();
    Ok(Incoming::of(schema, paths, std::slice::from_ref(&part.batch)).rounding)
}

/// Where a flush was cut into several pieces, the columns of each table holding, in any piece's
/// rows, a value a 64-bit float would round.
///
/// Each piece is split to judge it and its parts dropped, one at a time: a piece reserves what
/// its split makes before it is split, and holds nothing of it while the next waits.
async fn judged(
    job: &PartitionJob,
    context: &PartitionContext,
    pieces: Vec<(Vec<RecordBatch>, u64)>,
    shape: &Arc<Shape>,
) -> Result<Rounding, Error> {
    let mut rounding = Rounding::new();
    if pieces.len() < 2 {
        return Ok(rounding);
    }
    for (parts, bytes) in pieces {
        let _held = reserve(job, context, bytes.saturating_mul(SPLIT_COPIES)).await?;
        let (shape, stream) = (Arc::clone(shape), job.stream.clone());
        let parts = on_pool(context, move || split(&stream, &parts, &shape)).await?;
        for (path, columns) in judge(job, parts)? {
            rounding.entry(path).or_default().extend(columns);
        }
    }
    Ok(rounding)
}

/// The columns of each table holding, in any of `parts`, a value a 64-bit float would round.
pub(super) fn judge(job: &PartitionJob, parts: Vec<Part>) -> Result<Rounding, Error> {
    let mut rounding = Rounding::new();
    for part in parts {
        let columns = rounding_of(job, &part)?;
        rounding.entry(part.path).or_default().extend(columns);
    }
    Ok(rounding)
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

/// The error for an array at `path` no table can be named after.
fn unnamable(job: &PartitionJob, path: &[Arc<str>]) -> Error {
    let array = rdlt_connector::text::shown(path.join("."), PATH_SHOWN);
    Error::schema(format!(
        "stream {}: array {array}: no table can be named after its path: a key of it is empty, \
         longer than a table path's segment may be, or holds a control character",
        job.stream
    ))
    .with_code("table_path_invalid")
    .with_stream(&job.stream)
}

/// Bytes: the most of an array's path an error quotes.
const PATH_SHOWN: usize = 256;

/// `parts`, one batch once concatenated, normalized as `shape`.
fn split(stream: &StreamName, parts: &[RecordBatch], shape: &Shape) -> Result<Vec<Part>, Error> {
    let failed = |error: ArrowError| {
        Error::internal(format!("stream {stream}: normalizing a batch: {error}"))
    };
    let batch = arrow_select::concat::concat_batches(&parts[0].schema(), parts).map_err(failed)?;
    normalize::normalize(&batch, shape).map_err(failed)
}

/// The bytes of the allocations `part`'s batch and lineage keep alive beyond those `held`
/// holds, which then holds them too.
pub(super) fn part_growth(part: &Part, held: &mut Allocations) -> u64 {
    let lineage = &part.lineage;
    let mut arrays = vec![&lineage.id, &lineage.root_row];
    if let Some(parent) = &lineage.parent {
        arrays.extend([&parent.id, &parent.root, &parent.idx, &parent.row]);
    }
    let batch = held.add(&part.batch);
    arrays
        .into_iter()
        .map(|array| held.add_array(array.as_ref()))
        .fold(batch, u64::saturating_add)
}
