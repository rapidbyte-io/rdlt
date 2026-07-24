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

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use rdlt_connector::{
    Destination, DestinationCapabilities, LoadSession, OpenCtx, PushPayload, ReadRequest, Source,
    SourceError, StreamSpec, records_channel,
};
use rdlt_core::naming::normalize_ident;
use rdlt_core::{
    Cursor, LoadId, PipelineEvent, RdltError, ResumedFrom, RunReport, SchemaPolicy, StateDoc,
    StreamName, TableName, WriteMode,
};
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::EngineConfig;
use crate::load::{LoadItem, Loader};
use crate::runtime::channel::{ByteRx, ByteTx, byte_channel};
use crate::schema::registry::SchemaRegistry;
use crate::shred::TapeShredder;
use crate::shred::tape::PushError;

static LOAD_COUNTER: AtomicU64 = AtomicU64::new(0);

fn new_load_id() -> LoadId {
    // A wall clock before the Unix epoch yields no usable millis; fall back to 0.
    // The load id only needs to be UNIQUE within a pipeline, not monotonic, and
    // the process-id + atomic sequence below already guarantee that — the millis
    // are a human-readable prefix, not the uniqueness source.
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let seq = LOAD_COUNTER.fetch_add(1, Ordering::Relaxed);
    LoadId::new(format!("{millis:x}-{:x}-{seq:x}", std::process::id()))
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
        retries += 1;
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
    let caps = destination.capabilities();

    // ---- Discovery & build-time validation ----
    let streams = source
        .streams()
        .await
        .map_err(|e| classify_source_error(StreamName::new("<discovery>"), &e))?;
    validate_streams(config, &streams, caps, destination.as_ref())?;

    // ---- Workdir lock (one process per pipeline). Held for the whole run. ----
    let _lock = match &config.workdir {
        Some(dir) => Some(crate::runtime::lock::WorkdirLock::acquire(dir)?),
        None => None,
    };
    let wal_dir = config.workdir.as_ref().map(|d| d.join("wal"));

    // ---- Session open + state recovery + WAL replay ----
    let (session, base_state, resumed_from) = recover_wal(
        destination.as_ref(),
        config,
        &load_id,
        wal_dir.as_deref(),
        caps,
    )
    .await?;

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
    let (load_tx, load_rx) = byte_channel::<LoadItem>(config.byte_budget);
    let mut stream_tasks: JoinSet<Result<(), RdltError>> = JoinSet::new();

    for spec in streams {
        let mode = config.mode_for(&spec.name);
        // Replace streams are full-refresh by definition: they never resume from a
        // cursor (and full-refresh sources typically have none to honor).
        let since = if matches!(mode, WriteMode::Replace) {
            None
        } else {
            base_state.cursors.get(&spec.name).cloned()
        };
        let root_table = TableName::new(normalize_ident(spec.name.as_str(), caps.ident_rules));

        let _ = events.send(rdlt_core::PipelineEvent::StreamStarted {
            stream: spec.name.clone(),
        });

        stream_tasks.spawn(stream_task(
            spec,
            Arc::clone(&source),
            load_tx.clone(),
            cancel.clone(),
            caps,
            since,
            mode,
            root_table,
            config.byte_budget,
            load_id.clone(),
            config.schema_policy.clone(),
            events.clone(),
        ));
    }
    drop(load_tx);

    // ---- Loader: drain the channel, join the streams, commit the tail ----
    let loader = Loader::new(
        crate::load::Sink { session, caps },
        report,
        base_state,
        load_id.clone(),
        config.commit_policy,
        wal,
        events.clone(),
    );
    let mut report = drain_loader(loader, load_rx, stream_tasks, &cancel).await?;

    // Clean finish: nothing left to replay.
    if let Some(dir) = &wal_dir {
        crate::wal::clear(dir);
    }

    report.elapsed_ms = started.elapsed().as_millis() as u64;
    Ok(report)
}

/// Build-time validation over the discovered streams: one owning stream per
/// destination table (two streams writing one table would interleave
/// unowned rows), Merge only where the destination supports it,
/// and structured Merge only against a declared primary key. Fails before any
/// session is opened.
fn validate_streams(
    config: &EngineConfig,
    streams: &[StreamSpec],
    caps: DestinationCapabilities,
    destination: &dyn Destination,
) -> Result<(), RdltError> {
    let mut root_tables: BTreeMap<TableName, StreamName> = BTreeMap::new();
    for spec in streams {
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
        // Structured streams merge ONLY by a declared key — accepted iff the
        // stream declares a non-empty primary_key AND Merge{key} names exactly
        // that key (the destination's merge capability was checked above).
        // Keyless structured streams keep the original rejection.
        if spec.structured
            && let WriteMode::Merge { key } = config.mode_for(&spec.name)
        {
            let declared = spec.primary_key.clone().unwrap_or_default();
            if declared.is_empty() {
                return Err(RdltError::config(format!(
                    "stream `{}` is structured with no declared primary_key and \
                     cannot use Merge; declare a key on the \
                     stream and set Merge {{ key }} to it, or use Append/Replace",
                    spec.name
                )));
            }
            // Order-insensitive: the key is a SET (reflection returns
            // attnum order, users write DDL order).
            let mut key_set = key.clone();
            key_set.sort_unstable();
            let mut declared_set = declared.clone();
            declared_set.sort_unstable();
            if key_set != declared_set {
                return Err(RdltError::config(format!(
                    "stream `{}`: Merge key {:?} must name exactly the stream's \
                     declared primary_key columns {:?} (order does not matter)",
                    spec.name, key, declared
                )));
            }
        }
    }
    Ok(())
}

/// Open the destination session, recover persisted pipeline state, and replay
/// the uncommitted WAL span of a crashed run — or degrade to cursor
/// re-extraction on damage (slower, never wrong). Returns the open session, the
/// state to resume from, and how far recovery got.
async fn recover_wal(
    destination: &dyn Destination,
    config: &EngineConfig,
    load_id: &LoadId,
    wal_dir: Option<&Path>,
    caps: DestinationCapabilities,
) -> Result<(Box<dyn LoadSession>, StateDoc, ResumedFrom), RdltError> {
    let mut session = destination
        .open(OpenCtx::new(config.pipeline.clone(), load_id.clone()))
        .await
        .map_err(|e| crate::runtime::run::classify_dest_error(&e))?;
    let recovered = session
        .read_state(&config.pipeline)
        .await
        .map_err(|e| crate::runtime::run::classify_dest_error(&e))?;
    if let Some(state) = &recovered {
        state
            .check_readable()
            .map_err(|e| RdltError::config(e.to_string()))?;
    }
    let mut resumed_from = match &recovered {
        Some(state) if !state.cursors.is_empty() => ResumedFrom::Cursor,
        _ => ResumedFrom::Fresh,
    };
    let mut base_state = recovered
        .unwrap_or_else(|| StateDoc::new(config.pipeline.clone(), env!("CARGO_PKG_VERSION")));

    // WAL recovery: replay the uncommitted span of a crashed run (row 2), or
    // degrade to cursor re-extraction on damage (row 4 — slower, never wrong).
    if let Some(wal_dir) = wal_dir {
        match crate::wal::resume::scan(wal_dir) {
            crate::wal::resume::Scan::Nothing => {}
            crate::wal::resume::Scan::Recover(span) => {
                match crate::wal::resume::replay(
                    wal_dir,
                    span,
                    &mut *session,
                    &mut base_state,
                    caps,
                )
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

    Ok((session, base_state, resumed_from))
}

/// Single-owner shred/passthrough state for one stream. `run_blocking`-style
/// methods consume `self`, move it onto the blocking pool, and hand it back —
/// so the CPU-bound work stays lock-free and single-owner WITHOUT the take/expect
/// dance an `Option` would need (the owner is never absent by construction).
struct ShredOwner {
    shredder: TapeShredder,
    registry: SchemaRegistry,
}

impl ShredOwner {
    /// Shred one raw-JSON slab on the blocking pool. `Err` is a task panic; the
    /// inner `Result` is the shred outcome (invalid JSON classified per stream).
    async fn shred(
        mut self,
        bytes: Bytes,
        load_id: LoadId,
        mode: WriteMode,
        policy: SchemaPolicy,
        stream: StreamName,
    ) -> Result<(Self, Result<Vec<LoadItem>, RdltError>), RdltError> {
        tokio::task::spawn_blocking(move || {
            let span = tracing::info_span!("rdlt.shred");
            let _guard = span.enter();
            let ctx = crate::shred::ShredCtx {
                registry: &mut self.registry,
                load_id: &load_id,
                mode: &mode,
                policy: &policy,
            };
            let items = self
                .shredder
                .push_and_drain(&bytes, ctx)
                .map_err(|e| match e {
                    PushError::Json(e) => {
                        RdltError::source(stream, format!("invalid JSON from source: {e}"))
                    }
                    PushError::Engine(e) => e,
                });
            (self, items)
        })
        .await
        .map_err(|e| RdltError::internal(format!("shred task panicked: {e}")))
    }

    /// Pass one already-structured batch through on the blocking pool: the common
    /// path is cheap (schema map + one constant column), but a widened column
    /// casts real data — that work must not sit on the async executor.
    async fn passthrough(
        mut self,
        batch: arrow::record_batch::RecordBatch,
        table: TableName,
        load_id: LoadId,
        mode: WriteMode,
        policy: SchemaPolicy,
        caps: DestinationCapabilities,
    ) -> Result<(Self, Result<Vec<LoadItem>, RdltError>), RdltError> {
        tokio::task::spawn_blocking(move || {
            let span = tracing::info_span!("rdlt.passthrough");
            let _guard = span.enter();
            let ctx = crate::shred::ShredCtx {
                registry: &mut self.registry,
                load_id: &load_id,
                mode: &mode,
                policy: &policy,
            };
            let items = crate::shred::passthrough::passthrough_items(&batch, &table, ctx, caps);
            (self, items)
        })
        .await
        .map_err(|e| RdltError::internal(format!("passthrough task panicked: {e}")))
    }
}

/// One stream's task: read from the source, shred (or pass structured batches
/// through), forward the emitted load items, and classify the source outcome.
/// Classification only — the retry decision lives in `run` (run-level).
#[allow(clippy::too_many_arguments)]
async fn stream_task(
    spec: StreamSpec,
    source: Arc<dyn Source>,
    tx: ByteTx<LoadItem>,
    cancel: CancellationToken,
    caps: DestinationCapabilities,
    since: Option<Cursor>,
    mode: WriteMode,
    root_table: TableName,
    byte_budget: usize,
    load_id: LoadId,
    policy: SchemaPolicy,
    events: broadcast::Sender<PipelineEvent>,
) -> Result<(), RdltError> {
    let stream_name = spec.name.clone();
    let span = tracing::info_span!("rdlt.extract", stream = %stream_name);
    let _guard = span.enter();

    let arrow_table = root_table.clone();
    // Single-owner by construction: each blocking method consumes the owner and
    // returns it, so it is moved out and reassigned in place — never absent.
    let mut owner = ShredOwner {
        shredder: TapeShredder::new(spec.clone(), caps, root_table),
        registry: SchemaRegistry::default(),
    };

    let (out, mut input) = records_channel(byte_budget);
    let request = ReadRequest::new(spec.clone(), since, out);
    let read_source = Arc::clone(&source);
    let mut reader = tokio::spawn(async move { read_source.read(request).await });

    let push_result: Result<(), RdltError> = loop {
        let push = tokio::select! {
            push = input.recv() => push,
            _ = cancel.cancelled() => {
                input.close();
                break Err(RdltError::Cancelled);
            }
        };
        let Some(push) = push else { break Ok(()) };
        match push.payload {
            PushPayload::RawJson(bytes) => {
                // CPU-bound shred on the blocking pool; the owner keeps the
                // shredder single-owner without locks. The tape path parses the
                // slab into an arena and drains it in one call — no per-row trees.
                let (returned, items) = owner
                    .shred(
                        bytes,
                        load_id.clone(),
                        mode.clone(),
                        policy.clone(),
                        stream_name.clone(),
                    )
                    .await?;
                owner = returned;
                for item in items? {
                    if tx.send(item).await.is_err() {
                        break;
                    }
                }
            }
            PushPayload::Arrow(batch) => {
                // Structured fast path; undeclared streams are a
                // contract violation.
                if !spec.structured {
                    break Err(RdltError::source(
                        stream_name.clone(),
                        "source pushed Arrow batches on a stream not declared \
                         `structured`",
                    ));
                }
                let (returned, items) = owner
                    .passthrough(
                        batch,
                        arrow_table.clone(),
                        load_id.clone(),
                        mode.clone(),
                        policy.clone(),
                        caps,
                    )
                    .await?;
                owner = returned;
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

    // However the push loop ended, release the source BEFORE awaiting it:
    // closing wakes a sender parked on the byte budget so it observes the
    // closure — without this, a break on a contract violation would await
    // a reader that can never finish. (Idempotent; the Cancelled arm
    // already closed early for faster teardown.)
    input.close();
    // Surface source-side failures even when the push loop ended first.
    let read_result = (&mut reader).await;
    match (push_result, read_result) {
        (Err(e), _) => Err(e),
        (Ok(()), Ok(Ok(()))) => {
            let _ = events.send(rdlt_core::PipelineEvent::StreamFinished {
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
}

/// Drain the loader over the load channel, join the stream tasks, and commit the
/// trailing work. Precedence on error: a concrete stream error > a concrete
/// loader error > `Cancelled` (a loader failure cancels the streams, whose
/// induced `Cancelled` must not mask the original destination error).
async fn drain_loader(
    mut loader: Loader,
    mut load_rx: ByteRx<LoadItem>,
    mut stream_tasks: JoinSet<Result<(), RdltError>>,
    cancel: &CancellationToken,
) -> Result<RunReport, RdltError> {
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
            Err(join_err) => Err(RdltError::internal(format!(
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

    if let Some(error) = first_error {
        return Err(error);
    }
    match loader_result {
        Err(e) => return Err(e),
        Ok(()) if saw_cancelled => return Err(RdltError::Cancelled),
        Ok(()) => {}
    }

    // ---- Final commit: trailing work; state travels with the data ----
    loader.finish().await?;
    Ok(loader.report)
}

/// Map a connector-classified destination error onto the embedder taxonomy,
/// preserving retryability for the run-level driver — a transient warehouse
/// failure (lock, rate limit, network) restarts the run from committed state
/// exactly like a transient source failure, instead of aborting.
pub(crate) fn classify_dest_error(e: &rdlt_connector::DestinationError) -> RdltError {
    use rdlt_connector::DestinationError;
    match e {
        DestinationError::Transient(inner) => {
            RdltError::destination_retryable(format!("transient: {inner}"), None)
        }
        DestinationError::RateLimited {
            retry_after,
            source,
        } => RdltError::destination_retryable(format!("rate limited: {source}"), *retry_after),
        DestinationError::Fatal(inner) => RdltError::destination(format!("fatal: {inner}")),
        other => RdltError::destination(other.to_string()),
    }
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
