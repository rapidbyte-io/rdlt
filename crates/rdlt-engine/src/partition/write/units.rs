//! What a flush is written as: its Arrow batches, every value of their columns of JSON checked,
//! or its JSON pushes shredded into a batch a chunk, paid for before they are built.

use arrow_array::RecordBatch;
use bytes::Bytes;
use rdlt_connector::Permit;

use super::held::{self, Held};
use super::reserving;
use crate::budget::TooLarge;
use crate::error::{Error, ErrorKind};
use crate::json::{self, NotJson};
use crate::limits::JSON_EXCEEDS_BUDGET;
use crate::partition::coalesce::{Flushed, Unit};
use crate::partition::{PartitionContext, PartitionJob};
use crate::report::ShredCounts;
use crate::shred::{self, ShredError, ShredLimits};

/// Bytes: the most of a column's name an error quotes.
const NAME_SHOWN: usize = 256;

/// Some batches of one schema, and the memory they hold.
pub(super) type Units = Vec<(Vec<RecordBatch>, Held)>;

/// The units `flushed` is written as, and what shredding them took.
///
/// # Errors
///
/// Where a value of a column of JSON is not JSON, the shredder refuses the pushes, or building
/// their batches takes more of the budget than a request may.
pub(super) async fn of(
    job: &PartitionJob,
    context: &PartitionContext,
    flushed: Flushed,
) -> Result<(Units, ShredCounts), Error> {
    let permits = flushed.permits;
    match flushed.unit {
        Unit::Arrow(batches) => {
            checked(job, context, &batches).await?;
            let held = Held::of(permits, &batches);
            Ok((vec![(batches, held)], ShredCounts::default()))
        }
        Unit::Json(pushes) => shredded(job, context, pushes, permits).await,
    }
}

/// Checks, on the compute pool, that every value the columns of JSON of `batches` hold is JSON
/// nested within the limit: one that is not fails the write before anything reads it.
async fn checked(
    job: &PartitionJob,
    context: &PartitionContext,
    batches: &[RecordBatch],
) -> Result<(), Error> {
    let batches = batches.to_vec();
    let measured = batches.clone();
    let measure = move || measured.iter().map(json::held_by_check).max();
    let held = context
        .pool
        .run_all([measure])
        .await
        .pop()
        .flatten()
        .unwrap_or(0);
    // What the check holds beside the batches is reserved before it is held, and held with it.
    let _reserved = match held {
        0 => None,
        bytes => {
            let too_large = |large: TooLarge| checking_beyond_a_request(job, &large);
            Some(reserving(job, context, bytes, too_large).await?)
        }
    };
    let check = move || batches.iter().try_for_each(json::check_batch);
    let checked = context.pool.run_all([check]).await.pop();
    match checked {
        Some(Err(refused)) => Err(not_json(job, refused)),
        _ => Ok(()),
    }
}

/// The error for an Arrow push holding a value of a column of JSON that is not JSON, why kept
/// as its cause.
pub(super) fn not_json(job: &PartitionJob, refused: NotJson) -> Error {
    let NotJson { column, error } = refused;
    let message = format!(
        "stream {}: column {} of a push holds a value that is not JSON",
        job.stream,
        rdlt_connector::text::shown(&column, NAME_SHOWN),
    );
    Error::new(ErrorKind::Source, message)
        .with_code(error.code())
        .with_stream(&job.stream)
        .with_source(error)
}

/// The JSON `pushes`, admitted with `permits`, shredded into a unit a batch, and what every
/// observation of them and building them took.
///
/// The pushes were admitted for their text and twice it for the batches it becomes; what building
/// the batches takes beyond that is reserved before they are built, as lowering reserves its
/// pieces, and held with them.
async fn shredded(
    job: &PartitionJob,
    context: &PartitionContext,
    pushes: Vec<Bytes>,
    permits: Vec<Permit>,
) -> Result<(Units, ShredCounts), Error> {
    let failed = |error: ShredError| shred_failed(job, error);
    let pool = &context.pool;
    let chunk_bytes = context.batch.chunk_bytes().get();
    let limits = ShredLimits::new(context.budget.limits().schema_columns);
    let observe = || async {
        shred::observe(pool, &pushes, chunk_bytes, limits)
            .await
            .map_err(failed)
    };
    // Observing reserves its room first; building's bytes are taken without a wait while that
    // is held, or else waited for holding only the pushes, which are then observed again.
    let (mut room, mut counts) = (limits.beyond_bytes(), ShredCounts::default());
    let (beyond, observed) = loop {
        let too_large = |large: TooLarge| beyond_a_request(job, &large);
        let mut observing = reserving(job, context, room, too_large).await?;
        let observed = observe().await?;
        counts.add(&observed.counts());
        let excess = observed.excess();
        if excess <= observing.bytes() {
            observing.shrink(excess);
            break (observing, observed);
        }
        if let Some(reserved) = context.budget.try_acquire_working(excess) {
            break (reserved, observed);
        }
        room = excess;
    };
    let batches = observed.build(pool).await.map_err(failed)?;
    drop(pushes);
    let held = held::shredded(permits, beyond, &batches);
    let units = batches
        .into_iter()
        .zip(held)
        .map(|(batch, held)| (vec![batch], held))
        .collect();
    Ok((units, counts))
}

/// The error for an Arrow push whose columns of JSON take more to check than one request may
/// take of the budget.
fn checking_beyond_a_request(job: &PartitionJob, large: &TooLarge) -> Error {
    Error::new(
        ErrorKind::Source,
        format!(
            "stream {}: checking the JSON values of a push takes {} bytes, more than the {} one \
             request may take of the memory budget",
            job.stream, large.asked, large.limit
        ),
    )
    .with_code(JSON_EXCEEDS_BUDGET)
    .with_stream(&job.stream)
}

/// The error for JSON pushes whose batches take more beyond what the pushes were admitted for
/// than one request may take of the budget.
fn beyond_a_request(job: &PartitionJob, large: &TooLarge) -> Error {
    Error::new(
        ErrorKind::Source,
        format!(
            "stream {}: building the batches of JSON pushes takes {} bytes beyond what the pushes \
             were admitted for, more than the {} one request may take of the memory budget",
            job.stream, large.asked, large.limit
        ),
    )
    .with_code(JSON_EXCEEDS_BUDGET)
    .with_stream(&job.stream)
}

/// The error for a JSON push the shredder refused, its error kept as the cause.
pub(super) fn shred_failed(job: &PartitionJob, error: ShredError) -> Error {
    let message = format!("stream {}: a JSON push cannot be loaded", job.stream);
    let failed = match error {
        ShredError::Internal(_) => Error::internal(message),
        _ => Error::new(ErrorKind::Source, message),
    };
    failed
        .with_code(error.code())
        .with_stream(&job.stream)
        .with_source(error)
}
