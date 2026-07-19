//! The task graph: per-stream source + shred tasks feeding one loader over a
//! byte-bounded channel (design doc §3).
//!
//! - Sources are I/O tasks on the tokio runtime.
//! - Shredding is CPU-bound and runs via `spawn_blocking` state ping-pong — parse
//!   work never starves the async I/O stages.
//! - The loader is a single task owning the `LoadSession`; per-table ordering falls
//!   out of per-sender FIFO plus one-stream-per-table ownership (clause E2).

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
use crate::shred::StreamShredder;

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

pub(crate) async fn run(
    config: EngineConfig,
    source: Arc<dyn Source>,
    destination: Arc<dyn Destination>,
    cancel: CancellationToken,
    events: tokio::sync::broadcast::Sender<rdlt_core::PipelineEvent>,
) -> Result<RunReport, RdltError> {
    let started = Instant::now();
    let load_id = new_load_id();
    let caps = destination.capabilities();

    // ---- Discovery & build-time validation ----
    let streams = source
        .streams()
        .await
        .map_err(|e| RdltError::source(StreamName::new("<discovery>"), source_msg(&e)))?;

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

            let mut shredder = Some(StreamShredder::new(spec.clone(), caps, root_table));
            let mut registry = Some(SchemaRegistry::default());
            let mut resume_cursor = since;
            let mut attempt: u32 = 0;

            // Retry driver (clause E5): transient/rate-limited source failures retry
            // with backoff, resuming from the last checkpoint seen — connectors never
            // write retry loops.
            'attempts: loop {
                let (out, mut input) = records_channel(byte_budget);
                let request = ReadRequest::new(spec.clone(), resume_cursor.clone(), out);
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
                            // keeps the shredder single-owner without locks.
                            let mut sh = shredder.take().expect("shredder present");
                            let mut reg = registry.take().expect("registry present");
                            let batch_load_id = load_id.clone();
                            let batch_mode = mode.clone();
                            let batch_policy = policy.clone();
                            let stream_for_err = stream_name.clone();
                            let joined = tokio::task::spawn_blocking(move || {
                                let span = tracing::info_span!("rdlt.shred");
                                let _guard = span.enter();
                                let items = match sh.push_bytes(&bytes) {
                                    Ok(()) => sh.drain_batch(
                                        &mut reg,
                                        &batch_load_id,
                                        &batch_mode,
                                        &batch_policy,
                                    ),
                                    Err(e) => Err(RdltError::source(
                                        stream_for_err,
                                        format!("invalid JSON from source: {e}"),
                                    )),
                                };
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
                        PushPayload::Arrow(_batch) => {
                            break Err(RdltError::config(
                                "Arrow passthrough lands with the benchmark phase; \
                                 push raw_json/rows for now",
                            ));
                        }
                        PushPayload::Checkpoint(cursor) => {
                            // Remember for retry resumption BEFORE forwarding.
                            resume_cursor = Some(cursor.clone());
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
                let read_result = (&mut reader).await;
                match (push_result, read_result) {
                    (Err(e), _) => break 'attempts Err(e),
                    (Ok(()), Ok(Ok(()))) => break 'attempts Ok(()),
                    (Ok(()), Ok(Err(e))) => {
                        let retry_delay = match &e {
                            SourceError::Transient(_) => Some(backoff(attempt)),
                            SourceError::RateLimited { retry_after, .. } => {
                                Some(retry_after.unwrap_or_else(|| backoff(attempt)))
                            }
                            _ => None,
                        };
                        match retry_delay {
                            Some(delay) if attempt + 1 < MAX_SOURCE_ATTEMPTS => {
                                attempt += 1;
                                tracing::warn!(
                                    stream = %stream_name, attempt, error = %source_msg(&e),
                                    "retrying source after transient failure"
                                );
                                let _ = tx
                                    .send(LoadItem::Retried {
                                        stream: stream_name.clone(),
                                        attempt,
                                    })
                                    .await;
                                tokio::select! {
                                    _ = tokio::time::sleep(delay) => {}
                                    _ = stream_cancel.cancelled() => {
                                        break 'attempts Err(RdltError::Cancelled);
                                    }
                                }
                                continue 'attempts;
                            }
                            _ => {
                                break 'attempts Err(RdltError::source(
                                    stream_name.clone(),
                                    source_msg(&e),
                                ));
                            }
                        }
                    }
                    (Ok(()), Err(join_err)) => {
                        break 'attempts Err(RdltError::source(
                            stream_name.clone(),
                            format!("source task: {join_err}"),
                        ));
                    }
                }
            }
            .inspect(|()| {
                let _ = stream_events.send(rdlt_core::PipelineEvent::StreamFinished {
                    stream: stream_name,
                });
            })
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

    // ---- Join stream tasks; first error wins and cancels the rest ----
    let mut first_error: Option<RdltError> = None;
    while let Some(joined) = stream_tasks.join_next().await {
        let outcome = match joined {
            Ok(res) => res,
            Err(join_err) => Err(RdltError::config(format!(
                "stream task panicked: {join_err}"
            ))),
        };
        if let Err(e) = outcome
            && first_error.is_none()
        {
            cancel.cancel();
            first_error = Some(e);
        }
    }

    if let Some(error) = first_error {
        return Err(error);
    }
    loader_result?;

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

fn source_msg(e: &SourceError) -> String {
    match e {
        SourceError::Transient(inner) => format!("transient: {inner}"),
        SourceError::RateLimited { source, .. } => format!("rate limited: {source}"),
        SourceError::Fatal(inner) => format!("fatal: {inner}"),
        _ => e.to_string(),
    }
}
