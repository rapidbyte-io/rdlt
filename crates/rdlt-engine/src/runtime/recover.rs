//! Session open, persisted-state recovery, and WAL replay of a crashed run —
//! degrading to cursor re-extraction on any damage (slower, never wrong).

use std::path::Path;

use rdlt_connector::destination::{Capabilities, Destination, LoadSession, OpenContext};
use rdlt_core::error::Error;
use rdlt_core::id::LoadId;
use rdlt_core::report::ResumedFrom;
use rdlt_core::state::StateDoc;

use crate::EngineConfig;
use crate::classify::classify_dest_error;
use crate::wal::scan::{self, ScanOutcome};
use crate::wal::{dir, replay};

/// Run one piece of blocking file work off the async runtime.
///
/// Recovery is entirely file I/O — reading the manifest, opening segments,
/// decoding Arrow IPC — and rdlt is an EMBEDDABLE engine, so this future may
/// be polled on a host's runtime alongside the host's own work. Doing that
/// I/O inline occupies a worker thread for the whole of recovery; on a
/// single-threaded runtime it stalls the host completely. Neither is ours to
/// spend.
///
/// Panic policy, both halves: a panic that reaches THIS seam is re-raised on
/// the calling thread — but the DECODE seats never let one reach it. Replay
/// wraps every Arrow IPC decode in `catch_unwind` INSIDE the closure it hands
/// over (`segment::caught_decode` and the per-batch step), because arrow's
/// decoder has panic arms reachable from malformed but FlatBuffer-valid
/// segment bytes, and WAL bytes are external recovery input — such an unwind
/// IS damaged data and belongs on the same degrade-to-re-extraction path as
/// an ordinary decode error. For everything else that crosses here (manifest
/// line reads, fsyncs, filesystem walks) a panic is a bug in our own logic,
/// not corrupt data, and folding it into "degrade to re-extraction" would
/// hide the defect behind a slower correct path — so the default posture
/// stays re-raise, and a decode seat opts out at its closure, never here.
pub(crate) async fn off_runtime<T, F>(work: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(work).await {
        Ok(value) => value,
        Err(joined) => match joined.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            // spawn_blocking tasks are never cancelled by this code, so a
            // non-panic join failure means the runtime itself is shutting down.
            Err(_) => panic!("WAL recovery task cancelled: runtime is shutting down"),
        },
    }
}

/// Open the destination session, recover persisted pipeline state, and replay
/// the uncommitted WAL span of a crashed run — or degrade to cursor
/// re-extraction on damage (slower, never wrong). Returns the open session, the
/// state to resume from, and how far recovery got.
pub(super) async fn recover_wal(
    destination: &dyn Destination,
    config: &EngineConfig,
    load_id: &LoadId,
    wal_dir: Option<&Path>,
    capabilities: Capabilities,
    events: &tokio::sync::broadcast::Sender<rdlt_core::event::PipelineEvent>,
    output_totals: &std::sync::Arc<std::sync::Mutex<std::collections::BTreeMap<String, u64>>>,
) -> Result<(Box<dyn LoadSession>, StateDoc, ResumedFrom, WalResidue), Error> {
    // Replay runs on its OWN session, opened under the CRASHED run's load id —
    // scanning therefore has to happen before any session exists. A session's
    // load id is not decoration: a destination that publishes straight into
    // its targets decides "has this load already cleared this target?" from
    // it, so replaying a dead load's batches through the new run's session
    // would attribute them to the wrong load and could clear a target the new
    // run had already filled. The recovery session is opened, drained,
    // committed and dropped before the run's own session is opened.
    let mut resumed_from = None;
    let mut residue = WalResidue::None;
    if let Some(wal_dir) = wal_dir {
        // Every resolved arm CLEARS. The clear's failure REFUSES the
        // run where the residue is a HAZARD: after a Recover, surviving
        // residue re-replays the
        // span next run (idempotence absorbs it, but the run proceeds
        // believing the workdir clean); after Damaged/Unsupported the
        // residue re-degrades every following run. The DISCARD arm is
        // different — its manifest holds nothing replayable (an
        // already-committed span, or a crash before the first
        // checkpoint), so the residue is RESOLVED state: main's
        // best-effort tolerance returns there as a warning, and the
        // one caller threads the tolerance to `Wal::open`, whose
        // residue refusal stands for every OTHER path.
        let cleared = |dir: &Path| -> Result<(), Error> {
            dir::clear(dir).map_err(|e| {
                Error::wal(format!(
                    "clearing the WAL directory `{}` failed: {e} — refusing to run over \
                     unresolved residue",
                    dir.display()
                ))
            })
        };
        // The scan reads the manifest line by line — blocking file I/O,
        // off an embedder's runtime for the same reason replay's decoding is.
        let scanned = {
            let (dir, rules, pipeline) = (
                wal_dir.to_path_buf(),
                capabilities.ident_rules,
                config.pipeline.clone(),
            );
            off_runtime(move || scan::scan(&dir, rules, &pipeline, scan::MAX_MANIFEST_TOTAL_BYTES))
                .await
        };
        match scanned {
            ScanOutcome::Nothing => {}
            // Nothing to replay, but something to clean: a crash before the
            // first checkpoint leaves a manifest and its segments behind, and a
            // pipeline that keeps failing there would grow both without bound.
            // Not a warning — dying before the first checkpoint is ordinary.
            ScanOutcome::Discard => {
                if let Err(e) = dir::clear(wal_dir) {
                    tracing::warn!(
                        "clearing the resolved WAL directory `{}` failed: {e} — proceeding \
                         (its manifest holds nothing replayable; the new run's records \
                         append after it)",
                        wal_dir.display()
                    );
                    residue = WalResidue::Resolved;
                }
            }
            ScanOutcome::Recover(span) => {
                resumed_from =
                    replay_span(destination, config, wal_dir, span, capabilities).await?;
                cleared(wal_dir)?;
            }
            // The ONE arm that must not clear: the workdir is another
            // pipeline's territory. Replaying its span would commit that
            // pipeline's rows and cursors under this one; clearing it
            // would destroy that pipeline's recovery material. Refuse
            // the run, leaving the directory exactly as found —
            // configuration-class, because the fix is the operator's
            // (each pipeline gets its own workdir).
            ScanOutcome::ForeignPipeline { occupant } => {
                return Err(Error::config(format!(
                    "the WAL at `{}` belongs to pipeline `{occupant}` — this run is pipeline \
                     `{}`; replaying another pipeline's crash residue would commit its rows \
                     and cursors under the wrong pipeline, and clearing it would destroy its \
                     recovery material, so the run refuses: give each pipeline its own \
                     workdir, or move or delete that directory if `{occupant}` is retired",
                    wal_dir.display(),
                    config.pipeline
                )));
            }
            ScanOutcome::Damaged(reason) => {
                tracing::warn!(%reason, "WAL manifest damaged; re-extracting from cursors");
                cleared(wal_dir)?;
            }
            ScanOutcome::Unsupported { found, supported } => {
                tracing::warn!(
                    found,
                    supported,
                    "WAL was written in a different format version; re-extracting from \
                     cursors (slower, never wrong)"
                );
                cleared(wal_dir)?;
            }
        }
    }

    // The run's own session. Its state read happens AFTER any replay commit,
    // so it observes the recovered cursors rather than the pre-crash ones.
    let mut session = destination
        .open(
            OpenContext::new(config.pipeline.clone(), load_id.clone()).with_part_events(
                part_event_forwarder(events.clone(), std::sync::Arc::clone(output_totals)),
            ),
        )
        .await
        .map_err(|e| classify_dest_error(&e))?;
    let recovered = read_state_checked(&mut *session, config).await?;
    let resumed_from = resumed_from.unwrap_or(match &recovered {
        Some(state) if !state.cursors.is_empty() => ResumedFrom::Cursor,
        _ => ResumedFrom::Fresh,
    });
    let base_state = recovered
        .unwrap_or_else(|| StateDoc::new(config.pipeline.clone(), env!("CARGO_PKG_VERSION")));

    Ok((session, base_state, resumed_from, residue))
}

/// What recovery left in the WAL directory — [`crate::wal::writer::Wal::open`]
/// refuses a surviving manifest EXCEPT when recovery itself vouched
/// for it: a Discard-class manifest that could not be
/// cleared holds nothing replayable, so tolerating it is main's old
/// best-effort posture, warned; every other surviving manifest stays
/// the sequencing defect the refusal exists for.
#[derive(Clone, Copy, PartialEq)]
pub(super) enum WalResidue {
    /// The directory was cleared (or held nothing): a fresh open
    /// expects it clean.
    None,
    /// A RESOLVED manifest survived a failed clear — tolerated for
    /// this run only, with the warning already logged.
    Resolved,
}

/// Read persisted state and reject a document this build cannot honour before
/// anything is resumed from it.
async fn read_state_checked(
    session: &mut dyn LoadSession,
    config: &EngineConfig,
) -> Result<Option<StateDoc>, Error> {
    let recovered = session
        .read_state(&config.pipeline)
        .await
        .map_err(|e| classify_dest_error(&e))?;
    if let Some(state) = &recovered {
        state
            .check_readable()
            .map_err(|e| Error::config(e.to_string()))?;
        // Cross-pipeline isolation is the destination's SPI
        // obligation and the reference connector enforces it, but a
        // non-conforming third-party backend's answer was adopted
        // unchecked — one identity comparison here closes the
        // defense-in-depth gap for every backend at once.
        if state.pipeline != config.pipeline {
            return Err(Error::config(format!(
                "the destination returned state for pipeline `{}`, not `{}` — resuming a \
                 foreign pipeline's cursors here would mis-attribute its committed rows",
                state.pipeline, config.pipeline
            )));
        }
    }
    Ok(recovered)
}

/// Replay one uncommitted span through a session that carries the SPAN's load
/// id, then drop it. `None` means the span was unreadable and the caller
/// should fall back to cursor re-extraction (slower, never wrong).
async fn replay_span(
    destination: &dyn Destination,
    config: &EngineConfig,
    wal_dir: &Path,
    span: scan::RecoverySpan,
    capabilities: Capabilities,
) -> Result<Option<ResumedFrom>, Error> {
    // The replay session gets NO part-event listener: its parts belong
    // to the CRASHED load, and the feed describes THIS run — replayed
    // batches never emit BatchLoaded either, and report totals draw
    // the same line. Wiring it (an earlier draft did) also broke the
    // "RunStarted is the first event" guarantee, since replay finishes
    // before the run's identity is known. `resumed_from: wal` is the
    // feed's record that replay happened.
    let mut session = destination
        .open(OpenContext::new(
            config.pipeline.clone(),
            span.load_id.clone(),
        ))
        .await
        .map_err(|e| classify_dest_error(&e))?;
    // From here on, `session` exists and every failure path below is an
    // ABANDONMENT of it (this attempt's replay
    // fails and falls back to cursor re-extraction) — best-effort close
    // before propagating, same reasoning as `drain_loader`'s abandonment
    // paths: the lease protects concurrent sessions, not a dead attempt.
    let mut state = match read_state_checked(&mut *session, config).await {
        Ok(recovered) => recovered
            .unwrap_or_else(|| StateDoc::new(config.pipeline.clone(), env!("CARGO_PKG_VERSION"))),
        Err(e) => {
            session.close().await.ok();
            return Err(e);
        }
    };

    let replayed =
        match replay::replay(wal_dir, span, &mut *session, &mut state, capabilities).await {
            Ok(replayed) => replayed,
            Err(e) => {
                session.close().await.ok();
                return Err(e);
            }
        };
    match replayed {
        Some(replayed_batches) => {
            tracing::info!(replayed_batches, "recovered WAL span into destination");
            // The replay's own commit just landed — this session's last
            // (and only) commit succeeded, so its orderly STRICT close
            // belongs here, symmetric with the run's own session in
            // `drain_loader`. Non-retryable and prefixed like
            // `Loader::close`: the
            // commit is already durable, so a close failure here can
            // never mean lost data, and retrying the whole run would
            // re-execute a commit that already landed. The run's own
            // session opens next, under the SAME `destination` reference
            // (one connector instance for this whole attempt) — its
            // `Lease::acquire` hits the same-owner reacquire branch
            // regardless of whether this close ran, so this is prompt
            // cleanup, not a correctness requirement for what follows.
            session.close().await.map_err(|e| {
                Error::destination(format!(
                    "session close failed AFTER all commits were durable (the data is \
                     committed): {e}"
                ))
            })?;
            Ok(Some(ResumedFrom::Wal { replayed_batches }))
        }
        None => {
            // No commit ever reached this session (degraded before or
            // during pass 2) — nothing for `close` to make durable, and
            // the SPI contract reserves the STRICT close for a session
            // whose last commit succeeded. Best-effort closed instead,
            // exactly like the two error arms above — this is still an
            // abandonment, not a success; only
            // its own log line is `warn` rather than an error return.
            session.close().await.ok();
            tracing::warn!("WAL segments unreadable; falling back to cursor re-extraction");
            Ok(None)
        }
    }
}

/// Bridge a destination's part reports into the event feed. One
/// translation, used by every session the engine opens — the SPI's
/// reason vocabulary and the event enum's must not drift apart, and
/// having exactly one match is what enforces that.
fn part_event_forwarder(
    events: tokio::sync::broadcast::Sender<rdlt_core::event::PipelineEvent>,
    output_totals: std::sync::Arc<std::sync::Mutex<std::collections::BTreeMap<String, u64>>>,
) -> rdlt_connector::destination::PartEventFn {
    std::sync::Arc::new(move |part: rdlt_connector::destination::PartClosed| {
        if let Ok(mut totals) = output_totals.lock() {
            *totals.entry(part.table.as_str().to_owned()).or_default() += part.encoded_bytes;
        }
        use rdlt_connector::destination::PartCloseReason as Spi;
        let reason = match part.reason {
            Spi::Target => rdlt_core::event::PartCloseReason::Target,
            Spi::Time => rdlt_core::event::PartCloseReason::Time,
            Spi::Budget => rdlt_core::event::PartCloseReason::Budget,
            Spi::Commit => rdlt_core::event::PartCloseReason::Commit,
            Spi::Schema => rdlt_core::event::PartCloseReason::Schema,
            // The SPI enum is non_exhaustive: an unknown reason is
            // still a closed part, and Commit is the least-wrong
            // attribution for "the protocol closed it".
            _ => rdlt_core::event::PartCloseReason::Commit,
        };
        let _ = events.send(rdlt_core::event::PipelineEvent::PartClosed {
            table: rdlt_core::id::TableName::new(part.table.as_str()),
            encoded_bytes: part.encoded_bytes,
            reason,
        });
    })
}
