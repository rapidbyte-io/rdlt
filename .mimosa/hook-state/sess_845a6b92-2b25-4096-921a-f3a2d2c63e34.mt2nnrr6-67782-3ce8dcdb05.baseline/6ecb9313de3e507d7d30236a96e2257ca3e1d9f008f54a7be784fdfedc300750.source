//! One attempt of a run: discovery and validation, the workdir lock,
//! recovery, then the graph — per-stream source + shred tasks feeding one
//! loader over a byte-bounded channel. Sources are I/O tasks on the tokio
//! runtime; shredding is CPU-bound and runs via `spawn_blocking` state
//! ping-pong, so parse work never starves the async I/O stages; the loader
//! is a single task owning the `LoadSession`, and per-table ordering falls
//! out of per-sender FIFO plus one-stream-per-table ownership.

use std::{sync::Arc, time::Instant};

use rdlt_connector::channel::bytes;
use rdlt_connector::destination::Destination;
use rdlt_connector::source::Source;
use rdlt_core::commit::WriteMode;
use rdlt_core::error::Error;
use rdlt_core::event::PipelineEvent;
use rdlt_core::id::StreamName;
use rdlt_core::report;
use tokio::{sync::broadcast, task::JoinSet};
use tokio_util::sync::CancellationToken;

use super::extract::{StreamPlan, stream_task};
use super::recover::{WalResidue, recover_wal};
use super::retry::new_load_id;
use super::{drain, lock, validate};
use crate::config::Config;
use crate::lineage;
use crate::load::{LoadItem, Loader, Policies, Sink};
use crate::wal::{dir, writer};

/// Secondary message-count bound on a stage channel. The byte budget is the primary
/// backpressure; this hard cap keeps zero-byte items (markers) from queueing without
/// limit when the budget alone would never park them.
///
/// Runtime rule: stage channels are bounded by BYTES, not batch count — peak memory
/// stays capped regardless of row width; a batch-count bound would silently scale RSS
/// with schema size. A slow consumer exhausts the byte budget and the producer parks
/// on it: that *is* the backpressure. The byte-bounded channel itself is the SPI's
/// (`rdlt_connector::channel`) — one implementation serves both the engine's stages
/// and the source-push path.
pub(crate) const STAGE_MSG_CAPACITY: usize = 256;

pub(super) async fn run_once(
    config: &Config,
    source: Arc<dyn Source>,
    destination: Arc<dyn Destination>,
    cancel: CancellationToken,
    events: broadcast::Sender<PipelineEvent>,
    prior_retries: u64,
) -> Result<report::Run, Error> {
    let started = Instant::now();
    let load_id = new_load_id();
    // The load id is minted HERE, so the root span cannot carry it from
    // the caller; it is recorded onto the current span (installed by
    // `run`, bound to this attempt's future) instead.
    tracing::Span::current().record("rdlt.load_id", tracing::field::display(&load_id));
    let capabilities = destination.capabilities();

    // ---- Discovery & build-time validation ----
    let streams = validate::discover_and_validate(
        config,
        source.as_ref(),
        capabilities,
        destination.as_ref(),
    )
    .await?;

    // ---- Workdir lock (one process per pipeline). Held for the whole run. ----
    let _lock = config
        .workdir
        .as_deref()
        .map(lock::WorkdirLock::acquire)
        .transpose()?;
    let wal_dir = config.workdir.as_deref().map(dir::dir_in);

    // ---- Session open + state recovery + WAL replay ----
    // Output bytes per table, accumulated in the part-event forwarder
    // ITSELF (synchronous with the connectors), so the report's copy
    // is exact regardless of broadcast lag.
    let output_totals: Arc<std::sync::Mutex<std::collections::BTreeMap<String, u64>>> =
        Arc::default();
    let (mut session, base_state, resumed_from, residue) = recover_wal(
        destination.as_ref(),
        config,
        &load_id,
        wal_dir.as_deref(),
        capabilities,
        &events,
        &output_totals,
    )
    .await?;

    let wal = match wal_dir
        .as_ref()
        .map(|dir| {
            writer::Wal::open(
                dir.clone(),
                &config.pipeline,
                &load_id,
                capabilities.ident_rules,
                // Recovery vouches for Discard-class residue it could
                // not clear — the manifest holds nothing replayable and
                // the new run's records append after it.
                residue == WalResidue::Resolved,
            )
        })
        .transpose()
    {
        Ok(wal) => wal,
        Err(e) => {
            // `session` was just opened by `recover_wal` and has not yet
            // been handed to a `Loader` (that happens further down, once
            // this WAL is in hand) — a `Wal::open` failure here abandons
            // it before a single commit, the same reasoning `replay_span`
            // applies to ITS abandonment paths: a dead session protects
            // nothing, and leaving its lease held only costs the next
            // session (this same pipeline, possibly this same process on
            // retry) a `TTL_SECS`-long wait for a holder that is already
            // gone. Best-effort — a close failure must not shadow the
            // real `Wal::open` error being propagated.
            session.close().await.ok();
            return Err(e);
        }
    };

    let mut report = report::Run::new(config.pipeline.clone(), load_id.clone());
    report.resumed_from = resumed_from.clone();
    let _ = events.send(rdlt_core::event::PipelineEvent::RunStarted {
        load_id: load_id.clone(),
        resumed_from,
    });
    report.retries = prior_retries;

    // Read-side totals per stream, exact for the report (see the
    // extract task's note).
    let read_totals: Arc<std::sync::Mutex<std::collections::BTreeMap<StreamName, (u64, u64)>>> =
        Arc::default();

    // ---- Wire the graph ----
    let (load_tx, load_rx) = bytes::<LoadItem>(config.byte_budget, STAGE_MSG_CAPACITY);
    // ONE read-side budget for the whole run: every stream's
    // records channel spends from this single pool, so peak in-flight read
    // memory is the configured budget regardless of how many streams the
    // source declared — per-stream budgets multiplied the cap by the one
    // axis a rogue source controls directly.
    let records_budget = rdlt_connector::channel::SharedBudget::new(config.byte_budget);
    // ONE pool of read slots for the whole run: discovery may declare up
    // to the stream cap, but only `max_concurrent_streams` of them read
    // at a time. The permit is taken INSIDE each stream's task (before
    // its reader spawns), so every stream still starts — and its
    // StreamStarted still precedes any of its data events — while the
    // reads themselves queue on the pool.
    let read_slots = Arc::new(tokio::sync::Semaphore::new(config.max_concurrent_streams));
    let mut stream_tasks: JoinSet<Result<(), Error>> = JoinSet::new();

    for spec in streams {
        let mode = config.write_mode_for(&spec.name);
        // Replace streams are full-refresh by definition: they never resume from a
        // cursor (and full-refresh sources typically have none to honor).
        let since = if matches!(mode, WriteMode::Replace) {
            None
        } else {
            base_state.cursors.get(&spec.name).cloned()
        };
        let root_table = lineage::root_table(&spec.name, capabilities.ident_rules);

        let _ = events.send(rdlt_core::event::PipelineEvent::StreamStarted {
            stream: spec.name.clone(),
            table: root_table.clone(),
        });

        // Instrumented HERE rather than with a guard inside the task. A guard
        // held across `.await` stays on the worker thread's span stack while
        // other tasks run on it, so concurrent streams attribute each other's
        // events; `Instrument` binds the span to the FUTURE, which is what
        // "this stream's work" actually means.
        let span = tracing::info_span!("rdlt.extract", stream = %spec.name);
        let plan = StreamPlan {
            spec,
            capabilities,
            since,
            mode,
            root_table,
            records_budget: records_budget.clone(),
            read_slots: Arc::clone(&read_slots),
            load_id: load_id.clone(),
            policy: config.schema_policy.clone(),
            max_batch_cells: config.max_batch_cells,
        };
        stream_tasks.spawn(tracing::Instrument::instrument(
            stream_task(
                plan,
                Arc::clone(&source),
                load_tx.clone(),
                cancel.clone(),
                events.clone(),
                Arc::clone(&read_totals),
            ),
            span,
        ));
    }
    drop(load_tx);

    // ---- Heartbeat: a liveness tick while the run is active ----
    // Advisory like every event; aborted when the run ends. One
    // second is coarse enough to cost nothing and fine enough that a
    // consumer can call a silent MINUTE a stall with confidence.
    //
    // The first beat is emitted SYNCHRONOUSLY, right here, rather than
    // left to the spawned ticker's first tick: a spawned task only runs
    // once the scheduler polls it, and a fast run can finish and abort
    // the ticker before that ever happens (observed as a flake — the
    // run beat the task's first poll). Emitting inline makes "every run
    // carries >=1 heartbeat" structural instead of a race.
    let _ = events.send(rdlt_core::event::PipelineEvent::Heartbeat {
        elapsed_ms: started.elapsed().as_millis() as u64,
    });
    let heartbeat = {
        let events = events.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // `interval`'s first tick fires immediately (its next-deadline
            // starts at creation time); consume it here so the loop's own
            // sends begin at ~1s, not immediately again on top of the
            // synchronous beat above.
            tick.tick().await;
            loop {
                tick.tick().await;
                let _ = events.send(rdlt_core::event::PipelineEvent::Heartbeat {
                    elapsed_ms: started.elapsed().as_millis() as u64,
                });
            }
        })
    };

    // ---- Loader: drain the channel, join the streams, commit the tail ----
    let loader = Loader::new(
        Sink {
            session,
            capabilities,
        },
        report,
        base_state,
        load_id.clone(),
        Policies {
            commit: config.commit_policy,
            batch: config.batch_policy,
            max_batch_cells: config.max_batch_cells,
        },
        wal,
        events.clone(),
    );
    // The loader is one task; its span binds to that future rather than to
    // whichever worker thread happens to poll it.
    let drained = tracing::Instrument::instrument(
        drain::drive(loader, load_rx, stream_tasks, &cancel),
        tracing::info_span!("rdlt.load"),
    )
    .await;
    heartbeat.abort();
    let mut report = drained?;

    // Clean finish: nothing left to replay. Best-effort deliberately: every
    // commit is already acknowledged, so failing the run over cleanup would
    // trade a real success for an error — a surviving committed manifest
    // resolves as an ordinary Discard on the next run's scan.
    if let Some(dir) = &wal_dir {
        let _ = dir::clear(dir);
    }

    report.elapsed_ms = started.elapsed().as_millis() as u64;
    if let Ok(totals) = read_totals.lock() {
        for (stream, (rows_read, bytes_read)) in totals.iter() {
            let entry = report.streams.entry(stream.clone()).or_default();
            entry.rows_read = *rows_read;
            entry.bytes_read = *bytes_read;
        }
    }
    if let Ok(outputs) = output_totals.lock() {
        for (table, bytes) in outputs.iter() {
            report
                .table_mut(&rdlt_core::id::TableName::new(table.as_str()))
                .output_bytes = *bytes;
        }
    }
    let total_rows: u64 = report.tables.values().map(|t| t.rows).sum();
    let elapsed_secs = report.elapsed_ms as f64 / 1_000.0;
    report.rows_per_sec_avg =
        (elapsed_secs > f64::EPSILON && total_rows > 0).then(|| total_rows as f64 / elapsed_secs);
    Ok(report)
}
