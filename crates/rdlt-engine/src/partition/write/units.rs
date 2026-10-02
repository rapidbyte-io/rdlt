//! What a flush is written as: its Arrow batches, every value of their columns of JSON checked,
//! or its JSON pushes shredded into a batch a chunk, paid for before they are built.

use arrow_array::RecordBatch;
use bytes::Bytes;
use rdlt_connector::Permit;

use super::held::{self, Held};
use super::reserving;
use crate::budget::TooLarge;
use crate::compute::run_all;
use crate::error::{Error, ErrorKind};
use crate::json::{self, NotJson};
use crate::limits::JSON_EXCEEDS_BUDGET;
use crate::partition::coalesce::{Flushed, Unit};
use crate::partition::{PartitionContext, PartitionJob};
use crate::shred::{self, ShredError, ShredLimits};

/// Bytes: the most of a column's name an error quotes.
const NAME_SHOWN: usize = 256;

/// The units `flushed` is written as, each some batches of one schema and the memory they hold.
///
/// # Errors
///
/// Where a value of a column of JSON is not JSON, the shredder refuses the pushes, or building
/// their batches takes more of the budget than a request may.
pub(super) async fn of(
    job: &PartitionJob,
    context: &PartitionContext,
    flushed: Flushed,
) -> Result<Vec<(Vec<RecordBatch>, Held)>, Error> {
    let permits = flushed.permits;
    match flushed.unit {
        Unit::Arrow(batches) => {
            checked(job, context, &batches).await?;
            let held = Held::of(permits, &batches);
            Ok(vec![(batches, held)])
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
    if !batches
        .first()
        .is_some_and(|batch| json::holds_json(&batch.schema()))
    {
        return Ok(());
    }
    let batches = batches.to_vec();
    let check = move || batches.iter().try_for_each(json::check_batch);
    let checked = run_all(context.env.compute(), [check]).await.pop();
    let Some(Err(NotJson { column, error })) = checked else {
        return Ok(());
    };
    let message = format!(
        "stream {}: column {} of a push holds a value that is not JSON: {error}",
        job.stream,
        rdlt_connector::text::shown(&column, NAME_SHOWN),
    );
    Err(Error::new(ErrorKind::Source, message)
        .with_code(error.code())
        .with_stream(&job.stream))
}

/// The JSON `pushes`, admitted with `permits`, shredded into a unit a batch.
///
/// The pushes were admitted for their text and twice it for the batches it becomes; what building
/// the batches takes beyond that is reserved before they are built, as lowering reserves its
/// pieces, and held with them.
async fn shredded(
    job: &PartitionJob,
    context: &PartitionContext,
    pushes: Vec<Bytes>,
    permits: Vec<Permit>,
) -> Result<Vec<(Vec<RecordBatch>, Held)>, Error> {
    let failed = |error: ShredError| shred_failed(job, &error);
    let compute = context.env.compute();
    let chunk_bytes = context.batch.chunk_bytes().get();
    let limits = ShredLimits::new(context.budget.limits().schema_columns);
    let observed = shred::observe(compute, &pushes, chunk_bytes, limits)
        .await
        .map_err(failed)?;
    let beyond = match observed.excess() {
        0 => None,
        excess => {
            let too_large = |large: TooLarge| beyond_a_request(job, &large);
            Some(reserving(job, context, excess, too_large).await?)
        }
    };
    let batches = observed.build(compute).await.map_err(failed)?;
    drop(pushes);
    let held = held::shredded(permits, beyond, &batches);
    Ok(batches
        .into_iter()
        .zip(held)
        .map(|(batch, held)| (vec![batch], held))
        .collect())
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

/// The error for a JSON push the shredder refused.
pub(super) fn shred_failed(job: &PartitionJob, error: &ShredError) -> Error {
    let message = format!(
        "stream {}: a JSON push cannot be loaded: {error}",
        job.stream
    );
    let failed = match error {
        ShredError::Internal(_) => Error::internal(message),
        _ => Error::new(ErrorKind::Source, message),
    };
    failed.with_code(error.code()).with_stream(&job.stream)
}
