//! `K-SOURCE`: the source, loaded into memory as an engine loads it and killed at random points of
//! the load, converges on the tables a load it was never killed in publishes.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use rdlt_connector::{
    BoxFuture, Catalog, ConnectContext, ConnectorId, Cursor, Destination, PartitionId,
    PartitionPlan, PartitionSink, PipelineId, ReadMode, ReadRequest, Source, StreamName,
    StreamSpec, StreamState, destination_factory,
};
use rdlt_connector_reference::{MemoryDestination, published, tables};
use rdlt_engine::{PipelinePlan, StreamPlan, WriteMode};
use rdlt_host::Kills;

use super::bounded::{Beyond, Bounded};
use super::killing::{Killing, Schedule};
use super::rows::{Parted, parted, rendered};
use super::{Loaded, Proof, converged};
use crate::protocol::Violation;
use crate::target::Target;
use rdlt_connector::testing::RENDERED_BYTES;
use rdlt_connector::testing::render::{RenderError, Rendering};

/// `K-SOURCE` against the source `target` reaches, which answers to `id`, with `config`.
pub(crate) async fn resumed(
    target: &Target,
    id: &ConnectorId,
    config: &serde_json::Value,
) -> Loaded {
    super::proven(target.chosen_seed(), super::run(), |run, seed| async move {
        match compared(target, id, config, run, seed).await {
            Ok(loaded) => loaded,
            Err(Violation(reason)) => Loaded::Broken(format!("{reason} (kill seed {seed})")),
        }
    })
    .await
}

async fn compared(
    target: &Target,
    id: &ConnectorId,
    config: &serde_json::Value,
    run: u64,
    seed: u64,
) -> Result<Loaded, Violation> {
    let unkilled = Kills::new();
    let source = placed(target, id, config, &unkilled).await?;
    let catalog = source
        .discover()
        .await
        .map_err(|error| format!("the discovery failed: {error}"))?;
    let streams: Vec<StreamPlan> = catalog.iter().filter_map(planned).collect();
    if streams.is_empty() {
        let reason = "the source has no stream that is read in any mode";
        return Ok(Loaded::Inapplicable(reason.to_owned()));
    }
    let name = format!("certify_{run:x}_k");
    let pipeline = PipelineId::parse(&name).map_err(Violation::of)?;
    let plan = PipelinePlan::new(pipeline, streams).map_err(Violation::of)?;
    let (clean, killed) = (format!("{name}_clean"), format!("{name}_killed"));
    let (store, beyond) = memory(&clean).await?;
    let loaded = converged(&plan, &source, &store).await;
    if let Some(reason) = beyond.unobserved() {
        return Ok(Loaded::Unobserved(reason));
    }
    loaded.map_err(|Violation(reason)| format!("a load never killed failed: {reason}"))?;
    let kills = Kills::new();
    let placed = placed(target, id, config, &kills).await?;
    let resumed = Arc::new(Resumed::new(placed, &kills));
    let source: Arc<dyn Source> = Arc::clone(&resumed) as Arc<dyn Source>;
    let (store, beyond) = memory(&killed).await?;
    let killing = Killing::new(store, &kills, Schedule::seeded(seed, false));
    let destination: Arc<dyn Destination> = Arc::new(killing);
    let interrupted = converged(&plan, &source, &destination).await;
    if let Some(reason) = beyond.unobserved() {
        return Ok(Loaded::Unobserved(reason));
    }
    if let Some(unproven) = super::unproven(&kills, interrupted?, seed) {
        return Ok(unproven);
    }
    if let Some(unresumed) = resumed.unproven(seed) {
        return Ok(unresumed);
    }
    same(&clean, &killed, Proof::of(&kills)).await
}

/// The source `target` reaches, placed as an engine's placement places it, killed by `kills`.
async fn placed(
    target: &Target,
    id: &ConnectorId,
    config: &serde_json::Value,
    kills: &Kills,
) -> Result<Arc<dyn Source>, Violation> {
    let (provider, reference) = target.provider(id, kills, &super::limits()?);
    let placed = provider.source(&reference, config).await.map_err(|error| {
        format!(
            "the source could not be placed: {}",
            crate::connect::described(&error)
        )
    })?;
    Ok(Arc::from(placed.connector))
}

/// A source a kill clause kills, noting whether a read of it started after a kill: a read that
/// resumes it.
///
/// A kill that lands once the source was read to its end, as a load that commits slowly may
/// read it, fails an attempt with nothing left to resume, and proves nothing of how the source
/// resumes.
pub(super) struct Resumed {
    source: Arc<dyn Source>,
    kills: Kills,
    read_after_kill: AtomicBool,
}

impl Resumed {
    /// `source`, killed by `kills`.
    pub(super) fn new(source: Arc<dyn Source>, kills: &Kills) -> Self {
        Self {
            source,
            kills: kills.clone(),
            read_after_kill: AtomicBool::new(false),
        }
    }

    /// Why a load proves nothing of how the source resumes, drawn from `seed`: no read of it
    /// started after a kill; `None` once one did.
    pub(super) fn unproven(&self, seed: u64) -> Option<Loaded> {
        (!self.read_after_kill.load(Ordering::SeqCst)).then(|| {
            Loaded::Unseen(format!(
                "no read resumed after a kill: the source was read to its end first (kill seed \
                 {seed})"
            ))
        })
    }
}

impl Source for Resumed {
    fn check(&self) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        self.source.check()
    }

    fn discover(&self) -> BoxFuture<'_, rdlt_connector::Result<Catalog>> {
        self.source.discover()
    }

    fn plan<'a>(
        &'a self,
        stream: &'a StreamName,
        state: &'a StreamState,
    ) -> BoxFuture<'a, rdlt_connector::Result<PartitionPlan>> {
        self.source.plan(stream, state)
    }

    fn read(
        &self,
        request: ReadRequest,
        sink: PartitionSink,
    ) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        if self.kills.count() > 0 {
            self.read_after_kill.store(true, Ordering::SeqCst);
        }
        self.source.read(request, sink)
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, rdlt_connector::Result<()>> {
        self.source.committed(stream, cursors)
    }

    fn max_calls(&self) -> Option<NonZeroUsize> {
        self.source.max_calls()
    }
}

/// How a load reads and writes `stream`: incrementally, appending, when it can; else in full,
/// replacing; else as changes, merged by its key when it has one, and appended when not.
///
/// Every stream an engine reads is read in one of those, so none is left out of the load.
pub(super) fn planned(stream: &StreamSpec) -> Option<StreamPlan> {
    let plan = StreamPlan::new(stream.name().clone());
    if stream.supports(ReadMode::Incremental) {
        Some(plan.read(ReadMode::Incremental).write(WriteMode::Append))
    } else if stream.supports(ReadMode::Full) {
        Some(plan.read(ReadMode::Full).write(WriteMode::Replace))
    } else if stream.supports(ReadMode::Cdc) {
        let keyed = stream.primary_key().is_some_and(|key| !key.is_empty());
        let write = if keyed {
            WriteMode::Merge
        } else {
            WriteMode::Append
        };
        Some(plan.read(ReadMode::Cdc).write(write))
    } else {
        None
    }
}

/// A memory destination writing to `store`, taking as much as a kill clause loads, and what
/// tells whether a load wrote beyond that.
async fn memory(store: &str) -> Result<(Arc<dyn Destination>, Beyond), Violation> {
    let memory = destination_factory::<MemoryDestination>()
        .connect(serde_json::json!({ "store": store }), ConnectContext::new())
        .await
        .map_err(|error| Violation(format!("the memory destination did not connect: {error}")))?;
    let bounded = Bounded::new(Arc::from(memory));
    let beyond = bounded.witness();
    Ok((Arc::new(bounded), beyond))
}

/// Whether the stores `clean` and `killed` hold the same tables, with the same rows: kept on
/// `proof` when they do, and not observed when their rows cannot be rendered to compare.
async fn same(clean: &str, killed: &str, proof: Proof) -> Result<Loaded, Violation> {
    let mut rendering = Rendering::new(RENDERED_BYTES);
    let names = tables(clean);
    let killed_names = tables(killed);
    if names != killed_names {
        return Err(Violation(format!(
            "the load once killed published the tables {killed_names:?}, never killed \
             {names:?}"
        )));
    }
    for table in &names {
        let rows = rendered(&published(clean, table), &mut rendering).await;
        let (rows, killed_rows) = match rows {
            Ok(rows) => match rendered(&published(killed, table), &mut rendering).await {
                Ok(killed_rows) => (rows, killed_rows),
                Err(error) => return Ok(Loaded::Unobserved(uncompared(table, &error))),
            },
            Err(error) => return Ok(Loaded::Unobserved(uncompared(table, &error))),
        };
        if let Some(parted) = parted(&rows, &killed_rows) {
            let example = match parted {
                Parted::Missing(row) => format!("the row {row} is missing"),
                Parted::Extra(row) => format!("the row {row} is extra"),
            };
            return Err(Violation(format!(
                "table `{table}` holds {} rows once killed, {} never killed: {example}",
                killed_rows.len(),
                rows.len()
            )));
        }
    }
    Ok(Loaded::Kept(proof))
}

/// Why the clause is not observed when `table`'s rows cannot be rendered to compare.
pub(super) fn uncompared(table: &str, error: &RenderError) -> String {
    format!("table `{table}` cannot be compared: {error}")
}
