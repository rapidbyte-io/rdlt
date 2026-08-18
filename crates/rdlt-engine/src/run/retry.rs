//! The run driver: each attempt is a full run from committed state, and a
//! transient failure on EITHER side restarts through the crash-recovery
//! path (session re-open tears down staging, cursors resume from committed
//! state, the WAL replays). Retrying a single stream in place would leave
//! rows staged after the last checkpoint and publish them twice on
//! re-extraction — the exactly-once bug the crash path exists to prevent.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use rdlt_connector::destination::Destination;
use rdlt_connector::source::Source;
use rdlt_core::error::Error;
use rdlt_core::event::PipelineEvent;
use rdlt_core::id::LoadId;
use rdlt_core::report;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use super::once::run_once;
use crate::config::Config;

static LOAD_COUNTER: AtomicU64 = AtomicU64::new(0);

fn process_entropy() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    static ENTROPY: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *ENTROPY.get_or_init(|| {
        std::collections::hash_map::RandomState::new()
            .build_hasher()
            .finish()
    })
}

pub(super) fn new_load_id() -> LoadId {
    // A wall clock before the Unix epoch yields no usable millis; fall back to 0.
    // The load id must be UNIQUE across every pipeline sharing a destination
    // store, not merely within one pipeline: destination receipt lookups key on
    // `(load_id, commit_seq)` alone (a snapshot-history scan, a receipt
    // table), so a collision would make one
    // pipeline's commit replay-mask another's. Not monotonic — the millis are a
    // human-readable prefix; process-id + atomic sequence keep one host's
    // store, not merely within one pipeline: destination receipt lookups key on
    // `(load_id, commit_seq)` alone (a snapshot-history scan, a receipt table),
    // so a collision would make one
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
    config: Config,
    source: Arc<dyn Source>,
    destination: Arc<dyn Destination>,
    cancel: CancellationToken,
    events: broadcast::Sender<PipelineEvent>,
) -> Result<report::Run, Error> {
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
            Err(Error::Source {
                stream,
                message,
                retryable: true,
                retry_after_ms,
            }) if attempt + 1 < MAX_RUN_ATTEMPTS && !cancel.is_cancelled() => {
                (Some(stream), message, retry_after_ms)
            }
            Err(Error::Destination {
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
        let _ = events.send(rdlt_core::event::PipelineEvent::Retried { stream, attempt });
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = cancel.cancelled() => return Err(Error::Cancelled),
        }
    }
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
    /// The retry backoff curve, by value.
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
