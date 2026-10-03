//! JSON shredding: pushes of JSON records become Arrow batches, on the compute pool, built only
//! once what they take is paid for.
//!
//! The pushes of a coalesced unit are scanned on the pool for where their records lie, grouped
//! into chunks. Each chunk is parsed on the pool, its values observed and, speculatively, built
//! into columns typed by what the chunk held, within what its text was admitted for; a chunk whose
//! builders would take more is read again only to observe its values and count them. The
//! observations are joined in push order into one shape, whose columns and cells are bounded, and
//! what building every chunk's batch against that shape takes is known before any is built. A
//! chunk whose own shape is that shape keeps its columns; any other is parsed again and built
//! against it, on the pool. The join is the same whatever the order chunks finish in, so the
//! batches are too.

mod build;
mod conform;
mod cost;
#[cfg(test)]
mod differential;
mod exact;
mod meter;
mod observe;
mod observing;
mod records;
#[cfg(test)]
mod reference;
mod render;
#[cfg(test)]
mod tests;
pub(crate) mod values;
mod visit;

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use rdlt_connector::TableSchema;
use serde::de::DeserializeSeed;

use crate::compute::{ComputePool, run_all};
use crate::limits::MAX_CELLS;

use build::Record;
use meter::{Columns, Meter};
use observe::Shape;
use records::{Chunk, chunks};
use visit::{Context, Row};

/// Why a JSON push cannot be shredded.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ShredError {
    /// The push is not JSON.
    #[error("the push is not valid JSON: {0}")]
    Invalid(String),
    /// A record is not an object.
    #[error("a record is not a JSON object")]
    NotObject,
    /// A value nests deeper than the limit.
    #[error(
        "a value nests deeper than {} levels",
        rdlt_connector::limits::MAX_NESTING_DEPTH
    )]
    TooDeep,
    /// An object repeats a key, which is shown cut to a limit.
    #[error("an object repeats the key {0:?}")]
    DuplicateKey(String),
    /// The records hold more columns than the limit, the second.
    #[error("the records hold {0} columns or more, over the limit of {1}")]
    TooManyColumns(u64, u64),
    /// The records would shred into more cells than the limit.
    #[error("the records would shred into {0} cells, over the limit of {MAX_CELLS}")]
    TooManyCells(u64),
    /// A number's exponent, shown cut to a limit, has more digits than its value's canonical
    /// text holds.
    #[error(
        "the number {0} has an exponent of more than {digits} digits",
        digits = crate::json::EXPONENT_DIGITS
    )]
    Exponent(String),
    /// A list holds more items than a column can.
    #[error("a list column holds more items than one batch can")]
    TooLarge,
    /// A bug in the shredder.
    #[error("shredding: {0}")]
    Internal(String),
}

impl ShredError {
    /// The machine code of the error.
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::Invalid(_) => "json_invalid",
            Self::NotObject => "json_not_object",
            Self::TooDeep
            | Self::Exponent(_)
            | Self::TooManyColumns(..)
            | Self::TooManyCells(_)
            | Self::TooLarge => "limit_exceeded",
            Self::DuplicateKey(_) => "json_duplicate_key",
            Self::Internal(_) => "shred_internal",
        }
    }
}

/// What shredding pushes may take.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ShredLimits {
    /// The most columns the records may hold together, at any depth, a list's items counting
    /// as one: the limit a schema's columns meet.
    pub(crate) columns: u64,
    /// Bytes the pushes were admitted for, for the batches they become, for each byte of their
    /// records: what a chunk's speculative build may take.
    pub(crate) admitted: u64,
}

impl ShredLimits {
    /// The limits of pushes admitted as the engine admits JSON, whose records may hold at most
    /// `columns` columns.
    pub(crate) fn new(columns: u64) -> Self {
        Self {
            columns,
            admitted: crate::cost::JSON_CHARGE - 1,
        }
    }
}

/// One chunk, parsed: the columns built from it where its builders had room, and what its values
/// were observed to be and how many.
struct Parsed {
    chunk: Chunk,
    /// `None` where the builders would have taken more than the chunk was admitted for.
    record: Option<Record>,
    shape: Shape,
    /// Whether a column stopped building, so the chunk must be built again.
    spoiled: bool,
    /// Whether the chunk is parsed with exact numbers, as one holding an integer beyond 64 bits is.
    exact: bool,
    /// Whether a value of a column that stopped building held a float.
    json_floats: bool,
    /// Bytes the speculative build took.
    spent: u64,
}

/// How a chunk's batch is built.
#[derive(Clone, Copy, Debug)]
struct Plan {
    /// Parsed again against the push's shape, rather than its columns fitted to it.
    again: bool,
    /// With exact numbers.
    exact: bool,
}

/// Pushes observed, and what building their batches takes beyond what they were admitted for.
pub(crate) struct Shredding {
    parsed: Vec<Parsed>,
    plans: Vec<Plan>,
    shape: Arc<Shape>,
    schema: SchemaRef,
    excess: u64,
}

impl Shredding {
    /// Bytes building the batches takes beyond what the pushes were admitted for, which must be
    /// reserved before [`Shredding::build`].
    pub(crate) fn excess(&self) -> u64 {
        self.excess
    }

    /// The batches, one per chunk, built on `pool`.
    ///
    /// # Errors
    ///
    /// A refusal a chunk's second parse makes, or a bug.
    pub(crate) async fn build(
        self,
        pool: &dyn ComputePool,
    ) -> Result<Vec<RecordBatch>, ShredError> {
        let Self {
            parsed,
            plans,
            shape,
            schema,
            ..
        } = self;
        run_all(
            pool,
            parsed.into_iter().zip(plans).map(|(chunk, plan)| {
                let (shape, schema) = (Arc::clone(&shape), Arc::clone(&schema));
                move || job(|| batch(chunk, plan, &shape, schema))
            }),
        )
        .await
        .into_iter()
        .collect()
    }
}

/// Parses `chunk`, observing its values and building them into columns as they arrive, within
/// what it was admitted for; a chunk whose builders would take more is observed instead.
///
/// A chunk the fast parse finds a float that may be a rounded integer in is parsed again with its
/// numbers exact.
fn parse(chunk: Chunk, limits: ShredLimits) -> Result<Parsed, ShredError> {
    let mut exact = false;
    loop {
        let allowance = count(chunk.bytes).saturating_mul(limits.admitted);
        let context = Context::new(Meter::new(allowance), Columns::new(limits.columns));
        let mut record = Record::empty(chunk.rows);
        let appended = each(&chunk, exact, &context, |bytes| {
            let row = Row {
                record: &mut record,
                context: &context,
            };
            visit(bytes, row, exact, &context)
        })?;
        if appended.tripped {
            drop(record);
            return observed(chunk, exact, limits);
        }
        if appended.imprecise && !exact {
            exact = true;
            continue;
        }
        return Ok(Parsed {
            shape: record.shape(),
            record: Some(record),
            spoiled: context.spoiled(),
            exact,
            json_floats: context.json_floats(),
            spent: context.meter.spent(),
            chunk,
        });
    }
}

/// Observes `chunk`'s values, exactly where `exact` says or where the fast parse finds a float
/// that may be a rounded integer, building nothing.
fn observed(chunk: Chunk, exact: bool, limits: ShredLimits) -> Result<Parsed, ShredError> {
    let mut exact = exact;
    loop {
        let context = Context::new(Meter::new(0), Columns::new(limits.columns));
        let mut shape = Shape::default();
        let appended = each(&chunk, exact, &context, |bytes| {
            let row = observing::Record {
                shape: &mut shape,
                context: &context,
            };
            visit(bytes, row, exact, &context)
        })?;
        if appended.imprecise && !exact {
            exact = true;
            continue;
        }
        return Ok(Parsed {
            chunk,
            record: None,
            shape,
            spoiled: true,
            exact,
            json_floats: context.json_floats(),
            spent: 0,
        });
    }
}

/// Parses the record `bytes` into `seed`, with exact numbers where `exact` says.
fn visit<S>(bytes: &[u8], seed: S, exact: bool, context: &Context) -> Result<(), sonic_rs::Error>
where
    S: for<'de> DeserializeSeed<'de, Value = ()>,
{
    if exact {
        return exact::visit(bytes, seed, context);
    }
    let mut deserializer = sonic_rs::Deserializer::from_slice(bytes);
    seed.deserialize(&mut deserializer)
        .and_then(|()| deserializer.end())
}

/// How parsing a chunk went.
struct Appended {
    /// Whether the builders would have taken more than the chunk was admitted for.
    tripped: bool,
    /// Whether the parse stopped at a float that may be a rounded integer, or, parsing exactly,
    /// whether it parsed so.
    imprecise: bool,
}

/// Parses each record of `chunk` with `record`, its numbers exact where `exact` says.
///
/// The fast parse reads an integer beyond 64 bits as the float nearest it, and refuses one beyond a
/// float's range, so it stops at the first float that may be one, or at the first refusal, leaving
/// the chunk to be parsed again exactly. A parse whose builders trip their meter stops there.
fn each(
    chunk: &Chunk,
    exact: bool,
    context: &Context,
    mut record: impl FnMut(&[u8]) -> Result<(), sonic_rs::Error>,
) -> Result<Appended, ShredError> {
    for (index, bytes) in chunk.records().enumerate() {
        let parsed = record(bytes);
        // The fast parse reads a float of a vast negative exponent as zero, as it reads `0.0`:
        // a record where it read a zero and that may hold such a number is parsed exactly.
        if context.zeroed() && !exact && crate::json::may_hold_long_exponent(bytes) {
            context.reparse();
        }
        if let Err(error) = parsed {
            if let Some(fault) = context.fault() {
                return Err(fault);
            }
            if context.meter.tripped() {
                return Ok(Appended {
                    tripped: true,
                    imprecise: exact || context.imprecise(),
                });
            }
            // The fast parse refuses an integer beyond a float's range as it refuses invalid JSON;
            // the exact parse tells them apart.
            if exact {
                return Err(invalid(chunk.before + index, &error));
            }
            context.reparse();
        }
        if context.imprecise() && !exact {
            break;
        }
    }
    Ok(Appended {
        tripped: false,
        imprecise: exact || context.imprecise(),
    })
}

/// The error for record `index` of the pushes, which sonic-rs refused with `error`: what broke and
/// where, without the excerpt of the record sonic-rs quotes after it.
fn invalid(index: usize, error: &sonic_rs::Error) -> ShredError {
    let message = error.to_string();
    let what = message.lines().next().unwrap_or_default();
    ShredError::Invalid(format!("record {}: {what}", index + 1))
}

/// The shape every chunk's records fit, the join of each chunk's in push order, and how each
/// chunk's batch is built against it.
///
/// # Errors
///
/// [`ShredError::TooManyColumns`] where the shape holds more columns than `limits` lets it, and
/// [`ShredError::TooManyCells`] where the batches would hold more cells than the limit.
fn join(parsed: &[Parsed], limits: ShredLimits) -> Result<(Shape, Vec<Plan>, u64), ShredError> {
    let mut joined = Shape::default();
    for chunk in parsed {
        joined.join(&chunk.shape);
    }
    // Each chunk refuses columns past the limit as it is read; chunks that each fit may still
    // join into a shape that does not.
    let columns = joined.columns();
    if columns > limits.columns {
        return Err(ShredError::TooManyColumns(columns, limits.columns));
    }
    let text = cost::holds_text(&joined);
    let (mut cells, mut excess) = (0_u64, 0_u64);
    let mut plans = Vec::with_capacity(parsed.len());
    for chunk in parsed {
        let rows = count(chunk.chunk.rows);
        let size = cost::built(&joined, &chunk.shape, rows);
        cells = cells.saturating_add(size.cells);
        let again =
            chunk.record.is_none() || chunk.spoiled || !conform::shape_fits(&chunk.shape, &joined);
        let exact = chunk.exact || chunk.json_floats || cost::floats_in_json(&joined, &chunk.shape);
        let bytes = count(chunk.chunk.bytes);
        let takes = if again {
            // Text grows as it is written, to twice the chunk's at most.
            let text = if text { bytes.saturating_mul(2) } else { 0 };
            size.bytes.saturating_add(text)
        } else {
            let fitted = cost::fitted(&joined, &chunk.shape, rows);
            chunk.spent.saturating_add(fitted)
        };
        let admitted = bytes.saturating_mul(limits.admitted);
        excess = excess.saturating_add(takes.saturating_sub(admitted));
        plans.push(Plan { again, exact });
    }
    if cells > MAX_CELLS {
        return Err(ShredError::TooManyCells(cells));
    }
    Ok((joined, plans, excess))
}

/// The batch of `parsed`'s records against `shape`: the columns built as it was parsed, fitted to
/// `shape`, or the records built again against it, as `plan` says.
fn batch(
    parsed: Parsed,
    plan: Plan,
    shape: &Shape,
    schema: SchemaRef,
) -> Result<RecordBatch, ShredError> {
    let Parsed {
        chunk,
        record,
        shape: local,
        ..
    } = parsed;
    let rows = chunk.rows;
    let columns = match record {
        Some(record) if !plan.again => {
            conform::columns(&record.finish_columns()?, &local, shape, rows)?
        }
        speculative => {
            drop(speculative);
            again(&chunk, &cost::sized(shape, &local), plan.exact)?
        }
    };
    let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(rows));
    RecordBatch::try_new_with_options(schema, columns, &options)
        .map_err(|error| ShredError::Internal(format!("building a batch: {error}")))
}

/// The columns of `chunk`'s records built against `shape`, presized for what they hold: every
/// value fits it, so no column stops building and no builder grows past what was reserved.
fn again(
    chunk: &Chunk,
    shape: &Shape,
    exact: bool,
) -> Result<Vec<arrow_array::ArrayRef>, ShredError> {
    let unbuilt = |what: &str| ShredError::Internal(format!("building a chunk again: {what}"));
    let context = Context::new(Meter::reserved(), Columns::new(u64::MAX));
    let mut record =
        Record::new(shape, chunk.rows, &context.meter).map_err(|_| unbuilt("its builders"))?;
    let appended = each(chunk, exact, &context, |bytes| {
        let row = Row {
            record: &mut record,
            context: &context,
        };
        visit(bytes, row, exact, &context)
    })?;
    if appended.tripped || (appended.imprecise && !exact) {
        return Err(unbuilt("its records"));
    }
    record.finish_columns()
}

/// A count as the meter's bytes are measured in.
fn count(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// Stack a shredding job is sure of before it starts, 4 MiB: building and checking the columns of
/// values at the nesting limit walks their types once per level.
const JOB_STACK: usize = 4_194_304;

/// Stack added when a job must grow it, 8 MiB.
const JOB_SEGMENT: usize = 8_388_608;

/// Runs `work`, one shredding job, with at least [`JOB_STACK`] of stack, whatever thread runs it.
fn job<T>(work: impl FnOnce() -> T) -> T {
    stacker::maybe_grow(JOB_STACK, JOB_SEGMENT, work)
}

/// Observes the JSON `pushes`, in order, in chunks of about `chunk_bytes` each, on `pool`, within
/// `limits`: what the batches will be, and what building them takes beyond what the pushes were
/// admitted for.
///
/// A push is a JSON array of objects, or objects on their own lines; blank lines are skipped.
///
/// # Errors
///
/// Why the pushes cannot be shredded.
pub(crate) async fn observe(
    pool: &dyn ComputePool,
    pushes: &[Bytes],
    chunk_bytes: usize,
    limits: ShredLimits,
) -> Result<Shredding, ShredError> {
    let scanned = pushes.to_vec();
    let chunks = run_all(pool, [move || chunks(&scanned, chunk_bytes)])
        .await
        .pop()
        .ok_or_else(|| {
            ShredError::Internal("the scan of the pushes returned nothing".to_owned())
        })??;
    let parsed: Vec<Parsed> = run_all(
        pool,
        chunks
            .into_iter()
            .map(|chunk| move || job(|| parse(chunk, limits))),
    )
    .await
    .into_iter()
    .collect::<Result<_, _>>()?;
    let (shape, plans, excess) = join(&parsed, limits)?;
    let schema = TableSchema::new(shape.logical_fields())
        .map_err(|error| ShredError::Internal(format!("naming the columns: {error}")))?;
    Ok(Shredding {
        parsed,
        plans,
        shape: Arc::new(shape),
        schema: Arc::new(schema.to_arrow()),
        excess,
    })
}

/// Shreds the JSON `pushes` as [`observe`] and [`Shredding::build`] do, building whatever it
/// takes: for a caller with no budget to reserve it from.
#[cfg(any(test, feature = "bench"))]
pub(crate) async fn shred(
    pool: &dyn ComputePool,
    pushes: &[Bytes],
    chunk_bytes: usize,
    limits: ShredLimits,
) -> Result<Vec<RecordBatch>, ShredError> {
    observe(pool, pushes, chunk_bytes, limits)
        .await?
        .build(pool)
        .await
}
