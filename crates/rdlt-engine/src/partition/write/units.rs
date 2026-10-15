//! What a flush is written as: its Arrow batches, every value of their columns of JSON checked,
//! or its JSON pushes shredded into a batch a chunk, paid for before they are built.

use arrow_array::RecordBatch;
use bytes::Bytes;
use rdlt_connector::Permit;

use super::held::Held;
use super::{Room, reserving};
use crate::budget::{Reservation, TooLarge};
use crate::error::{Error, ErrorKind};
use crate::json::{self, NotJson};
use crate::limits::JSON_EXCEEDS_BUDGET;
use crate::partition::{PartitionContext, PartitionJob};
use crate::report::ShredCounts;
use crate::shred::{self, ShredError, ShredLimits};

/// Bytes: the most of a column's name an error quotes.
const NAME_SHOWN: usize = 256;

/// A flush's units, each some batches of one schema and the memory they hold, and where their
/// lowering reserves its bytes.
pub(super) struct Units {
    pub(super) units: Vec<(Vec<RecordBatch>, Held)>,
    pub(super) room: Room,
}

/// The unit Arrow `batches`, admitted with `permits`, are written as.
///
/// # Errors
///
/// Where a value of a column of JSON is not JSON.
pub(super) async fn arrow(
    job: &PartitionJob,
    context: &PartitionContext,
    batches: Vec<RecordBatch>,
    permits: Vec<Permit>,
) -> Result<Units, Error> {
    checked(job, context, &batches).await?;
    let held = Held::of(permits, &batches);
    let units = vec![(batches, held)];
    let room = Room::Budget;
    Ok(Units { units, room })
}

/// Checks, on the compute pool, that every value the columns of JSON of `batches` hold is JSON
/// nested within the limit: one that is not fails the write before anything reads it.
///
/// Batches whose schema holds no column of JSON, which every batch of a unit shares, are not sent
/// to the pool.
async fn checked(
    job: &PartitionJob,
    context: &PartitionContext,
    batches: &[RecordBatch],
) -> Result<(), Error> {
    if !batches
        .first()
        .is_some_and(|batch| json::schema_holds_json(batch.schema_ref()))
    {
        return Ok(());
    }
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

/// What lowering a JSON flush asks of the budget with what building its batches takes beyond
/// its pushes, in one request.
#[derive(Clone, Copy, Debug)]
pub(super) enum Beside {
    /// These bytes.
    Bytes(u64),
    /// The rest of a request.
    Rest,
}

/// A JSON flush's batches, a batch a chunk, and what building them took.
pub(super) struct Built {
    pub(super) batches: Vec<RecordBatch>,
    /// Bytes: what the batches take beyond twice their pushes' text.
    pub(super) excess: u64,
    /// The excess, reserved; nothing where it is none.
    pub(super) beyond: Option<Reservation>,
    /// What was asked beside the excess, reserved with it; nothing where the excess is none.
    pub(super) beside: Option<Reservation>,
    /// The passes over the pushes' records observing and building them took.
    pub(super) counts: ShredCounts,
}

/// The JSON `pushes` shredded into a batch a chunk, the pushes kept so they can be built again.
///
/// The pushes were admitted for their text and twice it for the batches it becomes. What
/// building the batches takes beyond that, and `beside` with it, is reserved in one request
/// before they are built; the partition waits for it holding only its pushes.
///
/// # Errors
///
/// The shredder refuses the pushes, or building their batches and `beside` take more than a
/// request may.
pub(super) async fn built(
    job: &PartitionJob,
    context: &PartitionContext,
    pushes: &[Bytes],
    beside: Beside,
) -> Result<Built, Error> {
    let failed = |error: ShredError| shred_failed(job, error);
    let pool = &context.pool;
    let chunk_bytes = context.batch.chunk_bytes().get();
    let limits = ShredLimits::new(context.budget.limits().schema_columns);
    let observe = || async {
        shred::observe(pool, pushes.to_vec(), chunk_bytes, limits)
            .await
            .map_err(failed)
    };
    // Observing reserves its room first; what building takes is reserved without a wait while
    // that is held, or else waited for holding only the pushes, which are then observed again.
    let request = context.budget.shares().request;
    let (mut asked, mut counts) = (limits.beyond_bytes(), ShredCounts::default());
    let (reserved, observed) = loop {
        let too_large = |large: TooLarge| beyond_a_request(job, &large);
        let mut observing = reserving(job, context, asked, too_large).await?;
        let observed = observe().await?;
        let excess = observed.excess();
        if excess == 0 {
            break (None, observed);
        }
        let wanted = match beside {
            Beside::Bytes(bytes) => excess.saturating_add(bytes),
            Beside::Rest => request.max(excess),
        };
        if wanted > request {
            let (what, limit) = ("lowering", request);
            let large = TooLarge {
                what,
                asked: wanted,
                limit,
            };
            return Err(beyond_a_request(job, &large));
        }
        if wanted <= observing.bytes() {
            observing.shrink(wanted);
            break (Some(observing), observed);
        }
        if let Some(reserved) = context.budget.try_acquire_working(wanted) {
            break (Some(reserved), observed);
        }
        // This observation is dropped, its chunks never built.
        counts.add(&observed.passes());
        asked = wanted;
    };
    let excess = observed.excess();
    let (beyond, beside) = match reserved {
        Some(mut reserved) => (Some(reserved.split(excess)), Some(reserved)),
        None => (None, None),
    };
    counts.add(&observed.counts());
    let batches = observed.build(pool).await.map_err(failed)?;
    Ok(Built {
        batches,
        excess,
        beyond,
        beside,
        counts,
    })
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
