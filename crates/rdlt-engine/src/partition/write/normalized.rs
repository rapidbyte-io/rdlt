//! Writing the units of a normalized stream: each normalized into its table's and child tables'
//! parts, planned parents first so dropped rows change no schema, and lowered on the pool.

mod discarded;
mod keys;
pub(super) mod split;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_schema::ArrowError;
use parking_lot::Mutex;
use rdlt_connector::Permit;

use self::split::{kept, made, split_kept};
use super::allowance::{Cutter, PartPiece};
use super::pieces::{self, Lowered};
use super::units::Units;
use super::{
    Held, OpenSegment, PartitionContext, PartitionJob, Room, full, queue, reserve, schema_invalid,
    stamp, taking,
};
use crate::budget::{Shares, TooLarge};
use crate::cost::{LINEAGE_ITEM, SPLIT_COPIES, lineage_row};
use crate::error::{Error, codes};
use crate::limits::MIN_PIECE;
use crate::normalize::rounding::Rounding;
use crate::normalize::{self, Dropped, Part, PartSchemas, Pruned, PushKind, Shape};
use crate::table::{Admission, Creation, Incoming, LoweringPlan, Prepared, Stamp};
use discarded::Discarded;

/// Lowers `units`, written from pushes of `kind`, of a stream that normalizes as `shape` into its
/// table and child tables, and queues them on their lanes.
///
/// - Each unit is cut, before its tables are known, into pieces whose split makes no more than a
///   piece may hold.
/// - Each piece then asks its room once, for all a request may take: what its planned parts hold
///   and an allowance for lowering them. It is split on the compute pool, its parts' tables and
///   plans are found in order, parents first, and each part is cut by what lowering it into its
///   own table holds. What the piece asked for beyond what it needs is given back.
/// - The allowance is lent to the parts' pieces, each taking what lowering it holds and giving it
///   back as it is written: the partition waits for its own pieces to be written, never for the
///   budget, while it holds any of them.
///
/// The units are one flush, so each table's integers are judged over all of them where they lie,
/// before any is split, and a table the flush finds not yet created takes every column and array
/// of every piece: where the flush was cut decides no column's existence or type.
pub(super) async fn write_normalized(
    job: &PartitionJob,
    context: &PartitionContext,
    open: &mut OpenSegment,
    schemas: &mut PartSchemas,
    units: Units,
    (shape, kind): (&Arc<Shape>, PushKind),
) -> Result<(), Error> {
    let Units { units, room } = units;
    let pieces = sliced(job, context, (&room, shape), units).await?;
    let mut flush = Flush::new(context, job.table, judged(job, &pieces, shape)?, kind);
    let request = room.shares(&context.budget).request;
    let last = pieces.len().saturating_sub(1);
    for (piece, held) in pieces.into_iter().enumerate() {
        let mut reserved = reserve(job, context, &room, request).await?;
        let parts = split_kept(job, context, schemas, shape, &held.batches).await?;
        let received = parts.first().map_or(0, |part| part.batch.num_rows() as u64);
        if received == 0 {
            continue;
        }
        let stamp = stamp(context, open, received);
        let (unit, discarded) = plan_parts(job, context, (parts, schemas), &mut flush).await?;
        open.discarded_values += discarded.values;
        open.discarded_rows += discarded.rows;
        open.discards.absorb(discarded.columns);
        // Measured once planned: a part that lost rows is a copy of those it kept.
        let planned = unit.iter().map(|(_, part, _)| part);
        let (made, kept) = (made(&held.batches, planned.clone()), kept(planned));
        let cutter = Cutter::within(context, request.saturating_sub(made));
        let rendering = context.rendering.as_ref().clone();
        let cut = on_pool(context, move || cutter.cut(&rendering, unit)).await;
        let cut = cut.map_err(|row| room.refusal(job, &context.budget, &row, made))?;
        // The allowance is as much as the pieces lowered at once take: the rest is given back,
        // and the allowance is lent to the pieces.
        let allowance = cutter.allowance(&cut);
        reserved.shrink(made.saturating_add(allowance));
        let lent = reserved.split(allowance).lend();
        if piece == last {
            // No piece asks for more: what the room has left goes back to the budget.
            room.close();
        }
        // The piece's parts on their lanes share what holds them, the piece's batches and its
        // planned parts, until the last of them is written: the parts go first, then the
        // piece's rows and with them its share of the unit, then their charge.
        let hold = Arc::new(Mutex::new((kept, held, reserved)));
        let mut cut = cut.into_iter().peekable();
        while let Some(piece) = cut.next() {
            // The partition waits for its allowance only while it holds no piece it has not
            // handed to its lane; the pieces the allowance has room for are lowered together.
            let taken = taking(&context.cancel, &lent, piece.bytes, unfit).await?;
            let mut window = vec![(piece, taken)];
            while !full(window.len()) {
                let Some(taken) = cut.peek().and_then(|next| lent.try_take(next.bytes)) else {
                    break;
                };
                window.extend(cut.next().map(|piece| (piece, taken)));
            }
            let (pieces, taken): (Vec<_>, Vec<_>) = window.into_iter().unzip();
            let tables: Vec<usize> = pieces.iter().map(|piece| piece.table).collect();
            let prepared = lower(context, pieces, stamp).await;
            for ((prepared, taken), table) in prepared.into_iter().zip(taken).zip(tables) {
                // The piece on its lane holds its part of the allowance, which goes back to it
                // as the piece is written, and with it what holds the piece's parts.
                let permit = Box::new((taken, Arc::clone(&hold))) as Permit;
                queue(job, context, open, table, prepared?, permit).await?;
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
        piece.plan.prepare(piece.batch, lineage, &stamp, None)
    };
    let bytes = pieces.iter().map(|piece| piece.bytes);
    if bytes.fold(0, u64::saturating_add) <= MIN_PIECE {
        return on_pool(context, move || pieces.into_iter().map(lowering).collect()).await;
    }
    let lowerings = pieces.into_iter().map(|piece| move || lowering(piece));
    context.pool.run_all(lowerings).await
}

/// The error for a part's piece that takes more than the allowance it is lowered in, which holds
/// every piece whole.
fn unfit(large: TooLarge) -> Error {
    Error::internal(format!(
        "a part's piece takes {} bytes, more than its allowance of {}",
        large.asked, large.limit
    ))
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
    (room, shape): (&Room, &Shape),
    units: Vec<Held>,
) -> Result<Vec<Held>, Error> {
    let shares = room.shares(&context.budget);
    let (max, limit) = split_bounds(shares);
    // What the split may make of a piece is kept from what its rows may take.
    let split = shares.request.saturating_sub(limit);
    let lowered = Lowered {
        rendering: context.rendering.as_ref().clone(),
        stored: Arc::default(),
        row: lineage_row(shape),
        item: LINEAGE_ITEM,
        // What a split holds whatever its rows is within the whole request each reserves.
        piece: 0,
        part: 0,
        // What the split makes is measured as rows expand, and may be twice that.
        max,
        limit,
    };
    let mut cut = Vec::with_capacity(units.len());
    for unit in units {
        let lowered = lowered.clone();
        let pieces = on_pool(context, move || pieces::sliced(unit, lowered)).await;
        cut.extend(pieces.map_err(|row| room.refusal(job, &context.budget, &row, split))?);
    }
    Ok(cut)
}

/// What a flush judges as it begins and its pieces are planned, so where it was cut decides no
/// column's existence or type.
struct Flush {
    rounding: Rounding,
    kind: PushKind,
    /// Whether the stream's table had been created as the flush began.
    existed: bool,
    /// The tables the flush has met, each found created, or not yet, the first time.
    creation: BTreeMap<usize, Creation>,
}

impl Flush {
    fn new(context: &PartitionContext, table: usize, rounding: Rounding, kind: PushKind) -> Self {
        let mut flush = Self {
            rounding,
            kind,
            existed: false,
            creation: BTreeMap::new(),
        };
        flush.existed = flush.creation(context, table) == Creation::AsModeled;
        flush
    }

    /// How `table` takes columns for the rest of the flush: every one where the flush found it
    /// not yet created.
    fn creation(&mut self, context: &PartitionContext, table: usize) -> Creation {
        *self.creation.entry(table).or_insert_with(|| {
            if context.tables.view(table).model.created() {
                Creation::AsModeled
            } else {
                Creation::ByFlush
            }
        })
    }
}

/// The parts of a unit, each with its table and plan.
pub(super) type PlannedParts = Vec<(usize, Part, Arc<LoweringPlan>)>;

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
    (parts, schemas): (Vec<Part>, &mut PartSchemas),
    flush: &mut Flush,
) -> Result<(PlannedParts, Discarded), Error> {
    let kind = flush.kind;
    let (admitted, mut dropped) = admit(job, context, parts, flush);
    let mut discarded = Discarded::default();
    let mut planned = Vec::with_capacity(admitted.len());
    for (part, fate) in admitted {
        let rounding = flush.rounding.get(&part.path).cloned().unwrap_or_default();
        let pruned = if dropped.is_empty() {
            Pruned::whole(part, kind)
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
            discarded.array(&planned, &pruned.part);
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
        let part = &pruned.part;
        let schema = schemas.table(&part.path, part.batch.schema_ref());
        let incoming = Incoming {
            schema: schema.map_err(|error| schema_invalid(job, &error))?,
            paths: Arc::clone(&part.columns),
            rounding,
            creation: flush.creation(context, table),
        };
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
/// they are part of the array value already discarded. An array is new only where the stream's
/// table had been created as `flush` began.
fn admit(
    job: &PartitionJob,
    context: &PartitionContext,
    parts: Vec<Part>,
    flush: &Flush,
) -> (Vec<(Part, Fate)>, Dropped) {
    let existed = flush.existed;
    let (mut admitted, mut dropped) = (Vec::with_capacity(parts.len()), Dropped::new(flush.kind));
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

/// The columns of each table holding, in any of `pieces`' rows, a value a 64-bit float would
/// round, judged where the rows lie, inline, before any piece is split.
fn judged(job: &PartitionJob, pieces: &[Held], shape: &Shape) -> Result<Rounding, Error> {
    let mut rounding = Rounding::new();
    for held in pieces {
        normalize::rounding::judge(&held.batches, shape, &mut rounding)
            .map_err(|error| schema_invalid(job, &error))?;
    }
    Ok(rounding)
}

/// Runs `work` on the compute pool.
async fn on_pool<T: Send + 'static>(
    context: &PartitionContext,
    work: impl FnOnce() -> T + Send + 'static,
) -> T {
    context
        .pool
        .run_all([work])
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
    .with_code(codes::SCHEMA_FROZEN)
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
    .with_code(codes::TABLE_PATH_INVALID)
    .with_stream(&job.stream)
}

/// Bytes: the most of an array's path an error quotes.
const PATH_SHOWN: usize = 256;
