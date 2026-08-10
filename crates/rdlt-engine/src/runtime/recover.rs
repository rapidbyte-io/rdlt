//! Session open, persisted-state recovery, and WAL replay of a crashed run —
//! degrading to cursor re-extraction on any damage (slower, never wrong).

use std::path::Path;

use rdlt_connector::{Destination, DestinationCapabilities, LoadSession, OpenContext};
use rdlt_core::{LoadId, RdltError, ResumedFrom, StateDoc};

use crate::EngineConfig;

use super::classify::classify_dest_error;

/// Open the destination session, recover persisted pipeline state, and replay
/// the uncommitted WAL span of a crashed run — or degrade to cursor
/// re-extraction on damage (slower, never wrong). Returns the open session, the
/// state to resume from, and how far recovery got.
pub(super) async fn recover_wal(
    destination: &dyn Destination,
    config: &EngineConfig,
    load_id: &LoadId,
    wal_dir: Option<&Path>,
    capabilities: DestinationCapabilities,
    events: &tokio::sync::broadcast::Sender<rdlt_core::PipelineEvent>,
    output_totals: &std::sync::Arc<std::sync::Mutex<std::collections::BTreeMap<String, u64>>>,
) -> Result<(Box<dyn LoadSession>, StateDoc, ResumedFrom), RdltError> {
    // Replay runs on its OWN session, opened under the CRASHED run's load id —
    // scanning therefore has to happen before any session exists. A session's
    // load id is not decoration: a destination that publishes straight into
    // its targets decides "has this load already cleared this target?" from
    // it, so replaying a dead load's batches through the new run's session
    // would attribute them to the wrong load and could clear a target the new
    // run had already filled. The recovery session is opened, drained,
    // committed and dropped before the run's own session is opened.
    let mut resumed_from = None;
    if let Some(wal_dir) = wal_dir {
        match crate::wal::resume::scan_off_runtime(wal_dir, capabilities.ident_rules).await {
            crate::wal::resume::ScanOutcome::Nothing => {}
            // Nothing to replay, but something to clean: a crash before the
            // first checkpoint leaves a manifest and its segments behind, and a
            // pipeline that keeps failing there would grow both without bound.
            // Not a warning — dying before the first checkpoint is ordinary.
            crate::wal::resume::ScanOutcome::Discard => crate::wal::clear(wal_dir),
            crate::wal::resume::ScanOutcome::Recover(span) => {
                resumed_from =
                    replay_span(destination, config, wal_dir, span, capabilities).await?;
                crate::wal::clear(wal_dir);
            }
            crate::wal::resume::ScanOutcome::Damaged(reason) => {
                tracing::warn!(%reason, "WAL manifest damaged; re-extracting from cursors");
                crate::wal::clear(wal_dir);
            }
            crate::wal::resume::ScanOutcome::Unsupported { found, supported } => {
                tracing::warn!(
                    found,
                    supported,
                    "WAL was written in a different format version; re-extracting from \
                     cursors (slower, never wrong)"
                );
                crate::wal::clear(wal_dir);
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

    Ok((session, base_state, resumed_from))
}

/// Read persisted state and reject a document this build cannot honour before
/// anything is resumed from it.
async fn read_state_checked(
    session: &mut dyn LoadSession,
    config: &EngineConfig,
) -> Result<Option<StateDoc>, RdltError> {
    let recovered = session
        .read_state(&config.pipeline)
        .await
        .map_err(|e| classify_dest_error(&e))?;
    if let Some(state) = &recovered {
        state
            .check_readable()
            .map_err(|e| RdltError::config(e.to_string()))?;
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
    span: crate::wal::resume::RecoverySpan,
    capabilities: DestinationCapabilities,
) -> Result<Option<ResumedFrom>, RdltError> {
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
    // 037 US2 fix round 2, I1: from here on, `session` exists and every
    // failure path below is an ABANDONMENT of it (this attempt's replay
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
        match crate::wal::resume::replay(wal_dir, span, &mut *session, &mut state, capabilities)
            .await
        {
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
            // belongs here (037 US2 T7 fix round 1), symmetric with the
            // run's own session in `drain_loader`. Non-retryable and
            // prefixed like `Loader::close` (fix round 2, M4): the
            // commit is already durable, so a close failure here can
            // never mean lost data, and retrying the whole run would
            // re-execute a commit that already landed. The run's own
            // session opens next, under the SAME `destination` reference
            // (one connector instance for this whole attempt) — its
            // `Lease::acquire` hits the same-owner reacquire branch
            // regardless of whether this close ran, so this is prompt
            // cleanup, not a correctness requirement for what follows.
            session.close().await.map_err(|e| {
                RdltError::destination(format!(
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
            // exactly like the two error arms above (037 US2 fix round
            // 2, I1) — this is still an abandonment, not a success; only
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
    events: tokio::sync::broadcast::Sender<rdlt_core::PipelineEvent>,
    output_totals: std::sync::Arc<std::sync::Mutex<std::collections::BTreeMap<String, u64>>>,
) -> rdlt_connector::PartEventFn {
    std::sync::Arc::new(move |part: rdlt_connector::PartClosed| {
        if let Ok(mut totals) = output_totals.lock() {
            *totals.entry(part.table.as_str().to_owned()).or_default() += part.encoded_bytes;
        }
        use rdlt_connector::PartCloseReason as Spi;
        let reason = match part.reason {
            Spi::Target => rdlt_core::PartClose::Target,
            Spi::Time => rdlt_core::PartClose::Time,
            Spi::Budget => rdlt_core::PartClose::Budget,
            Spi::Commit => rdlt_core::PartClose::Commit,
            Spi::Schema => rdlt_core::PartClose::Schema,
            // The SPI enum is non_exhaustive: an unknown reason is
            // still a closed part, and Commit is the least-wrong
            // attribution for "the protocol closed it".
            _ => rdlt_core::PartClose::Commit,
        };
        let _ = events.send(rdlt_core::PipelineEvent::PartClosed {
            table: rdlt_core::TableName::new(part.table.as_str()),
            encoded_bytes: part.encoded_bytes,
            reason,
        });
    })
}
