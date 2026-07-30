//! Session open, persisted-state recovery, and WAL replay of a crashed run —
//! degrading to cursor re-extraction on any damage (slower, never wrong).

use std::path::Path;

use rdlt_connector::{Destination, DestinationCapabilities, LoadSession, OpenCtx};
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
    caps: DestinationCapabilities,
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
        match crate::wal::resume::scan_off_runtime(wal_dir).await {
            crate::wal::resume::ScanOutcome::Nothing => {}
            // Nothing to replay, but something to clean: a crash before the
            // first checkpoint leaves a manifest and its segments behind, and a
            // pipeline that keeps failing there would grow both without bound.
            // Not a warning — dying before the first checkpoint is ordinary.
            crate::wal::resume::ScanOutcome::Discard => crate::wal::clear(wal_dir),
            crate::wal::resume::ScanOutcome::Recover(span) => {
                resumed_from = replay_span(destination, config, wal_dir, span, caps).await?;
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
        .open(OpenCtx::new(config.pipeline.clone(), load_id.clone()))
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
    caps: DestinationCapabilities,
) -> Result<Option<ResumedFrom>, RdltError> {
    let mut session = destination
        .open(OpenCtx::new(config.pipeline.clone(), span.load_id.clone()))
        .await
        .map_err(|e| classify_dest_error(&e))?;
    let mut state = read_state_checked(&mut *session, config)
        .await?
        .unwrap_or_else(|| StateDoc::new(config.pipeline.clone(), env!("CARGO_PKG_VERSION")));

    let replayed =
        crate::wal::resume::replay(wal_dir, span, &mut *session, &mut state, caps).await?;
    match replayed {
        Some(replayed_batches) => {
            tracing::info!(replayed_batches, "recovered WAL span into destination");
            Ok(Some(ResumedFrom::Wal { replayed_batches }))
        }
        None => {
            tracing::warn!("WAL segments unreadable; falling back to cursor re-extraction");
            Ok(None)
        }
    }
}
