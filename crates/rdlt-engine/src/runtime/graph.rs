//! The task graph: per-stream source + shred tasks feeding one loader over a
//! byte-bounded channel (design doc §3).
//!
//! - Sources are I/O tasks on the tokio runtime.
//! - Shredding is CPU-bound and runs via `spawn_blocking` state ping-pong — parse
//!   work never starves the async I/O stages.
//! - The loader is a single task owning the `LoadSession`; per-table ordering falls
//!   out of per-sender FIFO plus one-stream-per-table ownership (clause E2).
//!
//! Retries are RUN-level: a transient source failure restarts the whole attempt
//! through the crash-recovery path (session re-open tears down staging per clause
//! D4, cursors resume from committed state, WAL replays). Retrying a single stream
//! in place would leave rows staged after the last checkpoint and publish them
//! twice on re-extraction — the exactly-once bug the crash path exists to prevent.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use rdlt_connector::{
    Destination, OpenCtx, PushPayload, ReadRequest, Source, SourceError, records_channel,
};
use rdlt_core::naming::normalize_ident;
use rdlt_core::{
    LoadId, RdltError, ResumedFrom, RunReport, StateDoc, StreamName, TableName, WriteMode,
};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::EngineConfig;
use crate::load::{LoadItem, Loader};
use crate::runtime::channel::byte_channel;
use crate::schema::registry::SchemaRegistry;
use crate::shred::TapeShredder;
use crate::shred::tape::PushError;

static LOAD_COUNTER: AtomicU64 = AtomicU64::new(0);

fn new_load_id() -> LoadId {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let seq = LOAD_COUNTER.fetch_add(1, Ordering::Relaxed);
    LoadId::new(format!("{millis:x}-{:x}-{seq:x}", std::process::id()))
}

/// Engine-owned retry ceiling for transient source failures (clause E5).
const MAX_SOURCE_ATTEMPTS: u32 = 5;

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
    events: tokio::sync::broadcast::Sender<rdlt_core::PipelineEvent>,
) -> Result<RunReport, RdltError> {
    let mut attempt: u32 = 0;
    let mut retries: u64 = 0;
    loop {
        let attempt_cancel = cancel.child_token();
        let result = run_once(
            &config,
            Arc::clone(&source),
            Arc::clone(&destination),
            attempt_cancel,
            events.clone(),
            retries,
        )
        .await;
        match result {
            Err(RdltError::Source {
                stream,
                message,
                retryable: true,
                retry_after_ms,
            }) if attempt + 1 < MAX_SOURCE_ATTEMPTS && !cancel.is_cancelled() => {
                attempt += 1;
                retries += 1;
                let delay = retry_after_ms
                    .map(std::time::Duration::from_millis)
                    .unwrap_or_else(|| backoff(attempt));
                tracing::warn!(
                    stream = %stream, attempt, %message,
                    "transient source failure; restarting run from committed state"
                );
                let _ = events.send(rdlt_core::PipelineEvent::Retried {
                    stream: Some(stream),
                    attempt,
                });
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = cancel.cancelled() => return Err(RdltError::Cancelled),
                }
            }
            other => return other,
        }
    }
}

async fn run_once(
    config: &EngineConfig,
    source: Arc<dyn Source>,
    destination: Arc<dyn Destination>,
    cancel: CancellationToken,
    events: tokio::sync::broadcast::Sender<rdlt_core::PipelineEvent>,
    prior_retries: u64,
) -> Result<RunReport, RdltError> {
    let started = Instant::now();
    let load_id = new_load_id();
    let caps = destination.capabilities();

    // ---- Discovery & build-time validation ----
    let streams = source
        .streams()
        .await
        .map_err(|e| classify_source_error(StreamName::new("<discovery>"), &e))?;

    let mut root_tables: BTreeMap<TableName, StreamName> = BTreeMap::new();
    for spec in &streams {
        let table = TableName::new(normalize_ident(spec.name.as_str(), caps.ident_rules));
        if let Some(owner) = root_tables.insert(table.clone(), spec.name.clone()) {
            // Clause E2: exactly one stream owns a table.
            return Err(RdltError::config(format!(
                "streams `{owner}` and `{}` both map to table `{table}`",
                spec.name
            )));
        }
        if matches!(config.mode_for(&spec.name), WriteMode::Merge { .. }) && !caps.merge {
            return Err(RdltError::config(format!(
                "stream `{}` requests Merge but destination `{}` does not support it",
                spec.name,
                destination.spec().name
            )));
        }
        // Clause B4: structured streams carry no per-row identity — Merge cannot
        // deduplicate. Rejected here, at plan time, before the destination opens.
        if spec.structured && matches!(config.mode_for(&spec.name), WriteMode::Merge { .. }) {
            return Err(RdltError::config(format!(
                "stream `{}` is structured (Arrow passthrough, no per-row identity) \
                 and cannot use Merge (contract clause B4); use Append or Replace",
                spec.name
            )));
        }
    }

    // ---- Workdir lock (one process per pipeline, clause R5) ----
    let _lock = match &config.workdir {
        Some(dir) => Some(crate::runtime::lock::WorkdirLock::acquire(dir)?),
        None => None,
    };
    let wal_dir = config.workdir.as_ref().map(|d| d.join("wal"));

    // ---- Session + recovered state ----
    let mut session = destination
        .open(OpenCtx::new(config.pipeline.clone(), load_id.clone()))
        .await
        .map_err(RdltError::destination)?;
    let recovered = session
        .read_state(&config.pipeline)
        .await
        .map_err(RdltError::destination)?;
    if let Some(state) = &recovered {
        state
            .check_readable()
            .map_err(|e| RdltError::config(e.to_string()))?;
    }
    let mut resumed_from = match &recovered {
        Some(state) if !state.cursors.is_empty() => ResumedFrom::Cursor,
        _ => ResumedFrom::Fresh,
    };
    let mut base_state = recovered.unwrap_or_else(|| StateDoc::new(config.pipeline.clone()));

    // ---- WAL recovery: replay the uncommitted span of a crashed run (row 2), or
    // degrade to cursor re-extraction on damage (row 4 — slower, never wrong) ----
    if let Some(wal_dir) = &wal_dir {
        match crate::wal::resume::scan(wal_dir) {
            crate::wal::resume::Scan::Nothing => {}
            crate::wal::resume::Scan::Recover(span) => {
                match crate::wal::resume::replay(wal_dir, span, &mut session, &mut base_state, caps)
                    .await?
                {
                    Some(replayed_batches) => {
                        resumed_from = ResumedFrom::Wal { replayed_batches };
                        tracing::info!(replayed_batches, "recovered WAL span into destination");
                    }
                    None => {
                        tracing::warn!(
                            "WAL segments unreadable; falling back to cursor re-extraction"
                        );
                    }
                }
                crate::wal::clear(wal_dir);
            }
            crate::wal::resume::Scan::Damaged(reason) => {
                tracing::warn!(%reason, "WAL manifest damaged; re-extracting from cursors");
                crate::wal::clear(wal_dir);
            }
        }
    }

    let wal = match &wal_dir {
        Some(dir) => Some(crate::wal::Wal::open(
            dir.clone(),
            &config.pipeline,
            &load_id,
        )?),
        None => None,
    };

    let mut report = RunReport::new(config.pipeline.clone(), load_id.clone());
    report.resumed_from = resumed_from;
    report.retries = prior_retries;

    // ---- Wire the graph ----
    let (load_tx, mut load_rx) = byte_channel::<LoadItem>(config.byte_budget);
    let mut stream_tasks: JoinSet<Result<(), RdltError>> = JoinSet::new();

    for spec in streams {
        let stream_cancel = cancel.clone();
        let tx = load_tx.clone();
        let source = Arc::clone(&source);
        let mode = config.mode_for(&spec.name);
        // Replace streams are full-refresh by definition: they never resume from a
        // cursor (and full-refresh sources typically have none to honor).
        let since = if matches!(mode, WriteMode::Replace) {
            None
        } else {
            base_state.cursors.get(&spec.name).cloned()
        };
        let root_table = TableName::new(normalize_ident(spec.name.as_str(), caps.ident_rules));
        let byte_budget = config.byte_budget;
        let load_id = load_id.clone();
        let policy = config.schema_policy.clone();
        let stream_events = events.clone();

        let _ = events.send(rdlt_core::PipelineEvent::StreamStarted {
            stream: spec.name.clone(),
        });

        stream_tasks.spawn(async move {
            let stream_name = spec.name.clone();
            let span = tracing::info_span!("rdlt.extract", stream = %stream_name);
            let _guard = span.enter();

            let arrow_table = root_table.clone();
            let mut shredder = Some(TapeShredder::new(spec.clone(), caps, root_table));
            let mut registry = Some(SchemaRegistry::default());

            let (out, mut input) = records_channel(byte_budget);
            let request = ReadRequest::new(spec.clone(), since, out);
            let read_source = Arc::clone(&source);
            let mut reader = tokio::spawn(async move { read_source.read(request).await });

            let push_result: Result<(), RdltError> = loop {
                let push = tokio::select! {
                    push = input.recv() => push,
                    _ = stream_cancel.cancelled() => {
                        input.close();
                        break Err(RdltError::Cancelled);
                    }
                };
                let Some(push) = push else { break Ok(()) };
                match push.payload {
                    PushPayload::RawJson(bytes) => {
                        // CPU-bound shred on the blocking pool; state ping-pong
                        // keeps the shredder single-owner without locks. The tape
                        // path (feature 003 R24) parses the slab into an arena and
                        // drains it in one call — no per-row trees.
                        let mut sh = shredder.take().expect("shredder present");
                        let mut reg = registry.take().expect("registry present");
                        let batch_load_id = load_id.clone();
                        let batch_mode = mode.clone();
                        let batch_policy = policy.clone();
                        let stream_for_err = stream_name.clone();
                        let joined = tokio::task::spawn_blocking(move || {
                            let span = tracing::info_span!("rdlt.shred");
                            let _guard = span.enter();
                            let items = sh
                                .push_and_drain(
                                    &bytes,
                                    &mut reg,
                                    &batch_load_id,
                                    &batch_mode,
                                    &batch_policy,
                                )
                                .map_err(|e| match e {
                                    PushError::Json(e) => RdltError::source(
                                        stream_for_err,
                                        format!("invalid JSON from source: {e}"),
                                    ),
                                    PushError::Engine(e) => e,
                                });
                            (sh, reg, items)
                        })
                        .await
                        .map_err(|e| RdltError::config(format!("shred task panicked: {e}")))?;
                        let (sh, reg, items) = joined;
                        shredder = Some(sh);
                        registry = Some(reg);
                        for item in items? {
                            if tx.send(item).await.is_err() {
                                break;
                            }
                        }
                    }
                    PushPayload::Arrow(batch) => {
                        // Structured fast path (clause E7); undeclared streams are a
                        // contract violation (clause S7).
                        if !spec.structured {
                            break Err(RdltError::source(
                                stream_name.clone(),
                                "source pushed Arrow batches on a stream not declared \
                                 `structured` (contract clause S7)",
                            ));
                        }
                        // Same blocking-pool ping-pong as the shred arm: the common
                        // path is cheap (schema map + one constant column), but a
                        // widened column casts real data — that work must not sit on
                        // the async executor.
                        let mut reg = registry.take().expect("registry present");
                        let batch_load_id = load_id.clone();
                        let batch_mode = mode.clone();
                        let batch_policy = policy.clone();
                        let batch_table = arrow_table.clone();
                        let joined = tokio::task::spawn_blocking(move || {
                            let span = tracing::info_span!("rdlt.passthrough");
                            let _guard = span.enter();
                            let items = crate::shred::passthrough::passthrough_items(
                                &batch,
                                &batch_table,
                                &mut reg,
                                &batch_policy,
                                &batch_load_id,
                                &batch_mode,
                                caps,
                            );
                            (reg, items)
                        })
                        .await
                        .map_err(|e| {
                            RdltError::config(format!("passthrough task panicked: {e}"))
                        })?;
                        let (reg, items) = joined;
                        registry = Some(reg);
                        for item in items? {
                            if tx.send(item).await.is_err() {
                                break;
                            }
                        }
                    }
                    PushPayload::Checkpoint(cursor) => {
                        if tx
                            .send(LoadItem::Checkpoint {
                                stream: stream_name.clone(),
                                cursor,
                            })
                            .await
                            .is_err()
                        {
                            break Ok(());
                        }
                    }
                }
            };

            // Surface source-side failures even when the push loop ended first.
            // Classification only — the retry decision lives in `run` (run-level).
            let read_result = (&mut reader).await;
            match (push_result, read_result) {
                (Err(e), _) => Err(e),
                (Ok(()), Ok(Ok(()))) => {
                    let _ = stream_events.send(rdlt_core::PipelineEvent::StreamFinished {
                        stream: stream_name,
                    });
                    Ok(())
                }
                (Ok(()), Ok(Err(e))) => Err(classify_source_error(stream_name, &e)),
                (Ok(()), Err(join_err)) => Err(RdltError::source(
                    stream_name,
                    format!("source task: {join_err}"),
                )),
            }
        });
    }
    drop(load_tx);

    // ---- Loader ----
    let mut loader = Loader::new(
        session,
        report,
        base_state,
        load_id.clone(),
        config.commit_policy,
        wal,
        events.clone(),
        caps,
    );
    let loader_result: Result<(), RdltError> = loop {
        // `biased` toward the channel: items already in flight (e.g. a checkpoint
        // preceding a stream failure) are drained and committed before a
        // cancellation is observed — keeps failure semantics deterministic.
        let item = tokio::select! {
            biased;
            item = load_rx.recv() => item,
            _ = cancel.cancelled() => break Err(RdltError::Cancelled),
        };
        match item {
            Some(item) => {
                if let Err(e) = loader.process(item).await {
                    cancel.cancel();
                    break Err(e);
                }
            }
            None => break Ok(()),
        }
    };
    // Release the channel BEFORE joining: a stream task parked in `tx.send` (byte
    // budget held by queued-but-undelivered items) only unblocks when the receiver
    // drops — without this, a loader failure deadlocks the join below.
    drop(load_rx);

    // ---- Join stream tasks; prefer real errors over induced cancellations ----
    let mut first_error: Option<RdltError> = None;
    let mut saw_cancelled = false;
    while let Some(joined) = stream_tasks.join_next().await {
        let outcome = match joined {
            Ok(res) => res,
            Err(join_err) => Err(RdltError::config(format!(
                "stream task panicked: {join_err}"
            ))),
        };
        match outcome {
            Err(RdltError::Cancelled) => saw_cancelled = true,
            Err(e) => {
                if first_error.is_none() {
                    cancel.cancel();
                    first_error = Some(e);
                }
            }
            Ok(()) => {}
        }
    }

    // Precedence: a concrete stream error > a concrete loader error > Cancelled.
    // (A loader failure cancels the streams; their induced `Cancelled` results must
    // not mask the original destination error.)
    if let Some(error) = first_error {
        return Err(error);
    }
    match loader_result {
        Err(e) => return Err(e),
        Ok(()) if saw_cancelled => return Err(RdltError::Cancelled),
        Ok(()) => {}
    }

    // ---- Final commit: trailing work; state travels with the data (design doc §6) ----
    loader.finish().await?;

    // Clean finish: nothing left to replay.
    if let Some(dir) = &wal_dir {
        crate::wal::clear(dir);
    }

    let mut report = loader.report;
    report.elapsed_ms = started.elapsed().as_millis() as u64;
    Ok(report)
}

/// Map a connector-classified source error onto the embedder taxonomy, preserving
/// retryability for the run-level driver.
fn classify_source_error(stream: StreamName, e: &SourceError) -> RdltError {
    match e {
        SourceError::Transient(inner) => {
            RdltError::source_retryable(stream, format!("transient: {inner}"), None)
        }
        SourceError::RateLimited {
            retry_after,
            source,
        } => RdltError::source_retryable(stream, format!("rate limited: {source}"), *retry_after),
        SourceError::Fatal(inner) => RdltError::source(stream, format!("fatal: {inner}")),
        other => RdltError::source(stream, other.to_string()),
    }
}
