//! `K-DESTINATION`: generated rows, loaded into the destination as an engine loads them while the
//! destination is killed at random points of the load, are each published exactly once.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_cast::CastOptions;
use arrow_schema::DataType;
use rdlt_connector::testing::Probe;
use rdlt_connector::{
    ConnectContext, ConnectorId, Destination, PipelineId, Source, StreamName, source_factory,
};
use rdlt_connector_reference::GeneratorSource;
use rdlt_engine::{PipelinePlan, StreamPlan, WriteMode};
use rdlt_host::Kills;

use super::killing::{Killing, Schedule};
use super::{Loaded, converged};
use crate::protocol::Violation;
use crate::target::Target;

/// The rows a load generates: enough that a load commits many times, so every scheduled kill
/// finds one in flight.
pub(super) const ROWS: u64 = 2000;

/// `K-DESTINATION` against the destination `target` reaches, which answers to `id`, with
/// `config`, reading what it published through `probe`.
pub(crate) async fn exactly_once(
    target: &Target,
    id: &ConnectorId,
    config: &serde_json::Value,
    probe: &dyn Probe,
) -> Loaded {
    if !probe.reads() {
        let reason = "nothing reads back what the destination published";
        return Loaded::Inapplicable(reason.to_owned());
    }
    let run = super::run();
    let seed = target.chosen_seed().unwrap_or(run);
    match loaded(target, id, config, probe, run, seed).await {
        Ok(loaded) => loaded,
        Err(Violation(reason)) => Loaded::Broken(format!("{reason} (kill seed {seed})")),
    }
}

async fn loaded(
    target: &Target,
    id: &ConnectorId,
    config: &serde_json::Value,
    probe: &dyn Probe,
    run: u64,
    seed: u64,
) -> Result<Loaded, Violation> {
    let kills = Kills::new();
    let (provider, reference) = target.provider(id, &kills);
    let placed = provider
        .destination(&reference, config)
        .await
        .map_err(|error| {
            let described = crate::connect::described(&error);
            format!("the destination could not be placed: {described}")
        })?;
    let destination: Arc<dyn Destination> = Arc::from(placed.connector);
    if !destination.capabilities().write_modes.append {
        let reason = "the destination does not append, as the load appends";
        return Ok(Loaded::Inapplicable(reason.to_owned()));
    }
    let name = format!("certify_{run:x}_k");
    let source = generator(&name, seed).await?;
    let stream = StreamName::new(&name).map_err(Violation::of)?;
    let pipeline = PipelineId::parse(&name).map_err(Violation::of)?;
    let plan = PipelinePlan::new(pipeline, [StreamPlan::new(stream).write(WriteMode::Append)])
        .map_err(Violation::of)?;
    let killing = Arc::new(Killing::new(
        destination,
        &kills,
        Schedule::seeded(seed, true),
    ));
    let interrupted = converged(
        &plan,
        &source,
        &(Arc::clone(&killing) as Arc<dyn Destination>),
    )
    .await?;
    if let Some(unproven) = super::unproven(&kills, interrupted, seed) {
        return Ok(unproven);
    }
    let tables = killing.tables();
    let [table] = tables.as_slice() else {
        return Err(Violation(format!(
            "the load wrote {} tables, not one",
            tables.len()
        )));
    };
    let batches = probe
        .published(table)
        .await
        .map_err(|error| format!("reading back table `{}` failed: {error}", table.name))?;
    every_row_once(&batches).map_err(|fault| Violation(fault.to_string()))?;
    Ok(Loaded::Kept)
}

/// A generator of [`ROWS`] rows, drawn from `seed`, in the stream `name`.
async fn generator(name: &str, seed: u64) -> Result<Arc<dyn Source>, Violation> {
    let config = serde_json::json!({
        "seed": seed,
        "streams": [{ "name": name, "rows": ROWS, "partitions": 2, "batch_rows": 8 }],
    });
    source_factory::<GeneratorSource>()
        .connect(config, ConnectContext::new())
        .await
        .map(Arc::from)
        .map_err(|error| Violation(format!("the generator did not connect: {error}")))
}

/// Why a table read back does not hold each generated row exactly once.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Fault {
    /// It has no `id` column.
    NoIds,
    /// Its `id` column is no integer, for the stated reason.
    NotIntegers(String),
    /// Its `id` column holds nulls.
    Nulls,
    /// The row was published more than once.
    Repeated(i64),
    /// The row was never published.
    Missing(i64),
    /// The row was published but never loaded.
    Stray(i64),
}

impl std::fmt::Display for Fault {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoIds => formatter.write_str("the table read back has no `id` column"),
            Self::NotIntegers(reason) => {
                write!(
                    formatter,
                    "the `id` column read back is no integer: {reason}"
                )
            }
            Self::Nulls => formatter.write_str("the `id` column read back holds nulls"),
            Self::Repeated(row) => write!(formatter, "row {row} was published more than once"),
            Self::Missing(row) => write!(formatter, "row {row} was never published"),
            Self::Stray(row) => write!(formatter, "row {row} was published but never loaded"),
        }
    }
}

/// Whether `batches` hold each generated id exactly once.
pub(super) fn every_row_once(batches: &[RecordBatch]) -> Result<(), Fault> {
    let mut ids = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        let column = schema
            .fields()
            .iter()
            .position(|field| field.name().eq_ignore_ascii_case("id"))
            .map(|index| batch.column(index))
            .ok_or(Fault::NoIds)?;
        // Strict, so an id that is no integer is refused rather than read as a null.
        let strict = CastOptions {
            safe: false,
            ..CastOptions::default()
        };
        let cast = arrow_cast::cast_with_options(column, &DataType::Int64, &strict)
            .map_err(|error| Fault::NotIntegers(error.to_string()))?;
        let cast = cast
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Fault::NotIntegers("it casts to no `Int64` array".to_owned()))?;
        if cast.null_count() > 0 {
            return Err(Fault::Nulls);
        }
        ids.extend(cast.values().iter().copied());
    }
    ids.sort_unstable();
    let expected = 0..i64::try_from(ROWS).unwrap_or(i64::MAX);
    let repeats = |pair: &[i64]| match pair {
        [left, right] if left == right => Some(*left),
        _ => None,
    };
    if let Some(repeated) = ids.windows(2).find_map(repeats) {
        return Err(Fault::Repeated(repeated));
    }
    if let Some(missing) = expected.clone().find(|id| ids.binary_search(id).is_err()) {
        return Err(Fault::Missing(missing));
    }
    match ids.iter().find(|id| !expected.contains(id)) {
        Some(stray) => Err(Fault::Stray(*stray)),
        None => Ok(()),
    }
}
