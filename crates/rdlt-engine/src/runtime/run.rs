//! Run orchestration: per-stream source + shred tasks feeding one loader over a
//! byte-bounded channel.
//!
//! - Sources are I/O tasks on the tokio runtime.
//! - Shredding is CPU-bound and runs via `spawn_blocking` state ping-pong — parse
//!   work never starves the async I/O stages.
//! - The loader is a single task owning the `LoadSession`; per-table ordering falls
//!   out of per-sender FIFO plus one-stream-per-table ownership.
//!
//! Retries are RUN-level: a transient source failure restarts the whole attempt
//! through the crash-recovery path (session re-open tears down staging,
//! cursors resume from committed state, WAL replays). Retrying a single stream
//! in place would leave rows staged after the last checkpoint and publish them
//! twice on re-extraction — the exactly-once bug the crash path exists to prevent.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use rdlt_connector::{Destination, Source, channel::byte_channel};
use rdlt_core::{LoadId, PipelineEvent, RdltError, RunReport, StreamName, WriteMode};
use tokio::{sync::broadcast, task::JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{
    EngineConfig,
    load::{LoadItem, Loader},
};

use super::{
    classify::classify_source_error,
    drain::drain_loader,
    extract::{StreamPlan, stream_task},
    recover::recover_wal,
    validate::{root_table, validate_streams},
};

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

static LOAD_COUNTER: AtomicU64 = AtomicU64::new(0);

/// This process's random load-id component, drawn once from OS entropy
/// (`RandomState` seeds each instance from the OS; hashing nothing
/// through it yields its keys' 64 random bits — std-only). Cached: one
/// process is one id space, and the millis/pid/seq prefix separates ids
/// within it.
fn process_entropy() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    static ENTROPY: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *ENTROPY.get_or_init(|| {
        std::collections::hash_map::RandomState::new()
            .build_hasher()
            .finish()
    })
}

fn new_load_id() -> LoadId {
    // A wall clock before the Unix epoch yields no usable millis; fall back to 0.
    // The load id must be UNIQUE across every pipeline sharing a destination
    // store, not merely within one pipeline: destination receipt lookups key on
    // `(load_id, commit_seq)` alone (iceberg's snapshot-history scan since 042;
    // file/postgres receipts likewise), so a collision would make one
    // pipeline's commit replay-mask another's. Not monotonic — the millis are a
    // human-readable prefix; process-id + atomic sequence keep one host's
    // processes apart, and the per-process entropy suffix is the CROSS-HOST
    // claim: two hosts sharing a store no longer rely on pid+clock
    // disjointness (a recycled pid in the same millisecond would otherwise
    // replay-mask a genuine publish). The id is opaque to every consumer —
    // nothing parses this shape.
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let seq = LOAD_COUNTER.fetch_add(1, Ordering::Relaxed);
    LoadId::new(format!(
        "{millis:x}-{:x}-{seq:x}-{:x}",
        std::process::id(),
        process_entropy()
    ))
}

/// Engine-owned retry ceiling for transient failures (source OR
/// destination): each retry is a full run from committed state.
const MAX_RUN_ATTEMPTS: u32 = 5;

fn backoff(attempt: u32) -> std::time::Duration {
    std::time::Duration::from_millis(100u64.saturating_mul(1 << attempt.min(6)))
}

/// Retry driver: each attempt is a full run from committed state. A per-attempt
/// child token keeps internal failure-cancellation from poisoning the next attempt;
/// only the caller's token (`cancel`) survives across attempts.
pub(crate) async fn run(
    config: EngineConfig,
    source: Arc<dyn Source>,
    destination: Arc<dyn Destination>,
    cancel: CancellationToken,
    events: broadcast::Sender<PipelineEvent>,
) -> Result<RunReport, RdltError> {
    let mut attempt: u32 = 0;
    loop {
        let attempt_cancel = cancel.child_token();
        // The ROOT of the documented span contract (docs/telemetry.md):
        // everything under `rdlt.run` inherits the identity fields.
        // `Instrument` binds it to the attempt's FUTURE — a guard held
        // across an await would leak onto whichever worker thread polls
        // other tasks (the same reasoning as the per-stream spans).
        // `rdlt.load_id` is declared EMPTY and recorded inside, where
        // the id is minted.
        let span = tracing::info_span!(
            "rdlt.run",
            rdlt.pipeline = %config.pipeline,
            rdlt.load_id = tracing::field::Empty,
            rdlt.attempt = attempt,
        );
        let result = tracing::Instrument::instrument(
            run_once(
                &config,
                Arc::clone(&source),
                Arc::clone(&destination),
                attempt_cancel,
                events.clone(),
                u64::from(attempt),
            ),
            span,
        )
        .await;
        // Retryable failures from EITHER side restart the run from
        // committed state: the crash-recovery path tears down staging and
        // resumes cursors, so a retry can never double-publish.
        let (stream, message, retry_after_ms) = match result {
            Err(RdltError::Source {
                stream,
                message,
                retryable: true,
                retry_after_ms,
            }) if attempt + 1 < MAX_RUN_ATTEMPTS && !cancel.is_cancelled() => {
                (Some(stream), message, retry_after_ms)
            }
            Err(RdltError::Destination {
                message,
                retryable: true,
                retry_after_ms,
            }) if attempt + 1 < MAX_RUN_ATTEMPTS && !cancel.is_cancelled() => {
                (None, message, retry_after_ms)
            }
            other => return other,
        };
        attempt += 1;
        let delay = retry_after_ms
            .map(std::time::Duration::from_millis)
            .unwrap_or_else(|| backoff(attempt));
        tracing::warn!(
            stream = ?stream, attempt, %message,
            "transient failure; restarting run from committed state"
        );
        let _ = events.send(rdlt_core::PipelineEvent::Retried { stream, attempt });
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = cancel.cancelled() => return Err(RdltError::Cancelled),
        }
    }
}

async fn run_once(
    config: &EngineConfig,
    source: Arc<dyn Source>,
    destination: Arc<dyn Destination>,
    cancel: CancellationToken,
    events: broadcast::Sender<PipelineEvent>,
    prior_retries: u64,
) -> Result<RunReport, RdltError> {
    let started = Instant::now();
    let load_id = new_load_id();
    // The load id is minted HERE, so the root span cannot carry it from
    // the caller; it is recorded onto the current span (installed by
    // `run`, bound to this attempt's future) instead.
    tracing::Span::current().record("rdlt.load_id", tracing::field::display(&load_id));
    let capabilities = destination.capabilities();

    // ---- Discovery & build-time validation ----
    let streams = source
        .streams()
        .await
        .map_err(|e| classify_source_error(StreamName::new("<discovery>"), &e))?;
    validate_streams(config, &streams, capabilities, destination.as_ref())?;

    // ---- Workdir lock (one process per pipeline). Held for the whole run. ----
    let _lock = config
        .workdir
        .as_deref()
        .map(super::lock::WorkdirLock::acquire)
        .transpose()?;
    let wal_dir = config.workdir.as_deref().map(crate::wal::dir_in);

    // ---- Session open + state recovery + WAL replay ----
    // Output bytes per table, accumulated in the part-event forwarder
    // ITSELF (synchronous with the connectors), so the report's copy
    // is exact regardless of broadcast lag.
    let output_totals: Arc<std::sync::Mutex<std::collections::BTreeMap<String, u64>>> =
        Arc::default();
    let (mut session, base_state, resumed_from) = recover_wal(
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
            crate::wal::Wal::open(
                dir.clone(),
                &config.pipeline,
                &load_id,
                capabilities.ident_rules,
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

    let mut report = RunReport::new(config.pipeline.clone(), load_id.clone());
    report.resumed_from = resumed_from.clone();
    let _ = events.send(rdlt_core::PipelineEvent::RunStarted {
        load_id: load_id.clone(),
        resumed_from,
    });
    report.retries = prior_retries;

    // Read-side totals per stream, exact for the report (see the
    // extract task's note).
    let read_totals: Arc<std::sync::Mutex<std::collections::BTreeMap<StreamName, (u64, u64)>>> =
        Arc::default();

    // ---- Wire the graph ----
    let (load_tx, load_rx) = byte_channel::<LoadItem>(config.byte_budget, STAGE_MSG_CAPACITY);
    let mut stream_tasks: JoinSet<Result<(), RdltError>> = JoinSet::new();

    for spec in streams {
        let mode = config.write_mode_for(&spec.name);
        // Replace streams are full-refresh by definition: they never resume from a
        // cursor (and full-refresh sources typically have none to honor).
        let since = if matches!(mode, WriteMode::Replace) {
            None
        } else {
            base_state.cursors.get(&spec.name).cloned()
        };
        let root_table = root_table(&spec.name, capabilities.ident_rules);

        let _ = events.send(rdlt_core::PipelineEvent::StreamStarted {
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
            byte_budget: config.byte_budget,
            load_id: load_id.clone(),
            policy: config.schema_policy.clone(),
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
    let _ = events.send(rdlt_core::PipelineEvent::Heartbeat {
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
                let _ = events.send(rdlt_core::PipelineEvent::Heartbeat {
                    elapsed_ms: started.elapsed().as_millis() as u64,
                });
            }
        })
    };

    // ---- Loader: drain the channel, join the streams, commit the tail ----
    let loader = Loader::new(
        crate::load::Sink {
            session,
            capabilities,
        },
        report,
        base_state,
        load_id.clone(),
        crate::load::Policies {
            commit: config.commit_policy,
            batch: config.batch_policy,
        },
        wal,
        events.clone(),
    );
    // The loader is one task; its span binds to that future rather than to
    // whichever worker thread happens to poll it.
    let drained = tracing::Instrument::instrument(
        drain_loader(loader, load_rx, stream_tasks, &cancel),
        tracing::info_span!("rdlt.load"),
    )
    .await;
    heartbeat.abort();
    let mut report = drained?;

    // Clean finish: nothing left to replay.
    if let Some(dir) = &wal_dir {
        crate::wal::clear(dir);
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
                .table_mut(&rdlt_core::TableName::new(table.as_str()))
                .output_bytes = *bytes;
        }
    }
    let total_rows: u64 = report.tables.values().map(|t| t.rows).sum();
    let elapsed_secs = report.elapsed_ms as f64 / 1_000.0;
    report.rows_per_sec_avg =
        (elapsed_secs > f64::EPSILON && total_rows > 0).then(|| total_rows as f64 / elapsed_secs);
    Ok(report)
}

#[cfg(test)]
mod load_id_tests {
    use super::*;

    /// Consecutive ids differ (the sequence advances) and stay in the
    /// hex-and-dash shape. The shape itself is OPAQUE — nothing in the
    /// workspace parses a load id, and this pin documents the only
    /// properties a consumer may lean on: distinctness, and characters
    /// safe in paths and identifiers.
    #[test]
    fn consecutive_load_ids_differ_and_stay_hex_and_dash() {
        let a = new_load_id();
        let b = new_load_id();
        assert_ne!(a, b, "the sequence component separates consecutive ids");
        for id in [&a, &b] {
            assert!(
                id.as_str()
                    .bytes()
                    .all(|c| c.is_ascii_hexdigit() || c == b'-'),
                "load id `{id}` strays outside hex-and-dash"
            );
        }
    }

    /// The entropy component is drawn once and cached: every id this
    /// process mints carries the same suffix. (That two PROCESSES draw
    /// different values is `RandomState`'s OS-entropy seeding — not
    /// observable from one test process, so the claim tested is the
    /// caching, and the cross-process claim rides the seed's contract.)
    #[test]
    fn the_entropy_component_is_cached_per_process() {
        assert_eq!(process_entropy(), process_entropy());
    }
}

#[cfg(test)]
mod backoff_tests {
    // Mutation-report closure: the retry backoff curve, by value.
    #[test]
    fn backoff_doubles_and_saturates() {
        use std::time::Duration;
        assert_eq!(super::backoff(0), Duration::from_millis(100));
        assert_eq!(super::backoff(1), Duration::from_millis(200));
        assert_eq!(super::backoff(3), Duration::from_millis(800));
        assert_eq!(super::backoff(6), Duration::from_millis(6400));
        assert_eq!(super::backoff(60), Duration::from_millis(6400), "capped");
    }
}
