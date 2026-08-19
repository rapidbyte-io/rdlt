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
use crate::config::{Config, Jitter, RetryPolicy};

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
    // table), so a collision would make one pipeline's commit replay-mask
    // another's. Not monotonic — the millis are a human-readable prefix;
    // process-id + atomic sequence keep one host's processes apart, and the per-process entropy suffix is the CROSS-HOST
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

/// The backoff BOUND for one retry, before jitter: `base_delay` for the
/// first retry (`retry` is 1-based), doubling per retry, capped at
/// `max_delay`. The 1-based numbering is deliberate and pinned: the
/// first retry must wait ~one base delay, not two — an off-by-one here
/// silently doubles every wait in the whole ladder.
fn backoff_bound(policy: &RetryPolicy, retry: u32) -> std::time::Duration {
    let doublings = retry.saturating_sub(1).min(31);
    policy
        .base_delay
        .saturating_mul(1u32 << doublings)
        .min(policy.max_delay)
}

/// One retry's actual sleep: the jittered bound, or a `Retry-After`
/// hint where the failure carried one. The hint keeps precedence — the
/// service named its own delay — but bounded by `max_delay` and never
/// jittered.
fn retry_delay(policy: &RetryPolicy, retry: u32, hint_ms: Option<u64>) -> std::time::Duration {
    match hint_ms {
        Some(ms) => std::time::Duration::from_millis(ms).min(policy.max_delay),
        None => {
            let bound = backoff_bound(policy, retry);
            match policy.jitter {
                Jitter::None => bound,
                Jitter::Full => uniform_within(bound),
            }
        }
    }
}

/// Uniform in `0..=bound` (full jitter), millisecond granularity.
fn uniform_within(bound: std::time::Duration) -> std::time::Duration {
    let bound_ms = u64::try_from(bound.as_millis()).unwrap_or(u64::MAX);
    // Widening-multiply reduction maps a 64-bit word onto 0..=bound_ms.
    let sampled = ((u128::from(jitter_word()) * (u128::from(bound_ms) + 1)) >> 64) as u64;
    std::time::Duration::from_millis(sampled)
}

/// A 64-bit word from splitmix64 over a Weyl sequence seeded with the
/// process entropy. Statistical spread is all jitter needs — this is
/// scheduling noise, not cryptography — and the workspace carries no
/// rng dependency to reach for.
fn jitter_word() -> u64 {
    static WEYL: AtomicU64 = AtomicU64::new(0);
    let step = WEYL.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed);
    let mut word = process_entropy().wrapping_add(step);
    word = (word ^ (word >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    word = (word ^ (word >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    word ^ (word >> 31)
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
    // u64 so the `attempt + 1` guards below cannot overflow even in
    // principle (a u32 counter wraps after 2³² attempts — practically
    // unreachable, but a guard should not carry an unreachable wrap).
    // The guard also caps `attempt` under `max_attempts` (a u32), so
    // the narrowing casts at the event and delay seats are lossless.
    let mut attempt: u64 = 0;
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
                attempt,
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
            }) if attempt + 1 < u64::from(config.retry.max_attempts.get())
                && !cancel.is_cancelled() =>
            {
                (Some(stream), message, retry_after_ms)
            }
            Err(Error::Destination {
                message,
                retryable: true,
                retry_after_ms,
            }) if attempt + 1 < u64::from(config.retry.max_attempts.get())
                && !cancel.is_cancelled() =>
            {
                (None, message, retry_after_ms)
            }
            other => return other,
        };
        // `attempt` becomes the 1-based retry number: the first retry
        // sleeps ~base_delay (the numbering `backoff_bound` pins).
        attempt += 1;
        let delay = retry_delay(&config.retry, attempt as u32, retry_after_ms);
        tracing::warn!(
            stream = ?stream, attempt, %message,
            "transient failure; restarting run from committed state"
        );
        let _ = events.send(rdlt_core::event::PipelineEvent::Retried {
            stream,
            attempt: attempt as u32,
        });
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
    use std::time::Duration;

    use super::*;

    /// THE PRODUCTION NUMBERING, at the seam the driver calls: the
    /// first retry's bound is exactly `base_delay`. The regression this
    /// pins was real — the driver once incremented the attempt counter
    /// before computing the delay, so the first retry slept 2×base and
    /// every later one rode the doubled ladder.
    #[test]
    fn the_first_retrys_bound_is_one_base_delay() {
        let policy = RetryPolicy::default();
        assert_eq!(backoff_bound(&policy, 1), policy.base_delay);
        assert_eq!(backoff_bound(&policy, 2), policy.base_delay * 2);
        assert_eq!(backoff_bound(&policy, 3), policy.base_delay * 4);
        assert_eq!(backoff_bound(&policy, 4), policy.base_delay * 8);
    }

    /// `max_delay` caps the ladder — including the shift-saturating far
    /// end, where the doubling count itself is clamped.
    #[test]
    fn the_bound_caps_at_max_delay() {
        let policy = RetryPolicy::default();
        assert_eq!(backoff_bound(&policy, 10), policy.max_delay);
        assert_eq!(backoff_bound(&policy, u32::MAX), policy.max_delay);
    }

    /// Full jitter samples uniformly within `0..=bound`: never over,
    /// and over many samples the spread reaches both ends — a sampler
    /// stuck at one value (or quietly halved) fails the spread checks.
    #[test]
    fn full_jitter_stays_within_the_bound_and_spreads() {
        let policy = RetryPolicy {
            jitter: Jitter::Full,
            ..Default::default()
        };
        let bound = backoff_bound(&policy, 1);
        assert_eq!(bound, Duration::from_millis(100));
        let samples: Vec<Duration> = (0..1_000).map(|_| retry_delay(&policy, 1, None)).collect();
        assert!(samples.iter().all(|d| *d <= bound), "never over the bound");
        let min = samples.iter().min().expect("samples");
        let max = samples.iter().max().expect("samples");
        assert!(
            *min < Duration::from_millis(30),
            "1,000 uniform samples reach the low end: min {min:?}"
        );
        assert!(
            *max > Duration::from_millis(70),
            "1,000 uniform samples reach the high end: max {max:?}"
        );
    }

    /// `Jitter::None` sleeps the bound exactly — the deterministic arm
    /// the paused-clock numbering pin (and coordinating embedders) lean
    /// on.
    #[test]
    fn no_jitter_sleeps_the_bound_exactly() {
        let policy = RetryPolicy {
            jitter: Jitter::None,
            ..Default::default()
        };
        assert_eq!(retry_delay(&policy, 1, None), Duration::from_millis(100));
        assert_eq!(retry_delay(&policy, 2, None), Duration::from_millis(200));
    }

    /// A `Retry-After` hint keeps precedence over the computed backoff
    /// — the service named its own delay — but bounded by `max_delay`
    /// and never jittered: an adversarial or confused hint cannot park
    /// the driver for an hour.
    #[test]
    fn a_retry_after_hint_wins_bounded_by_max_delay() {
        let policy = RetryPolicy::default();
        assert_eq!(
            retry_delay(&policy, 1, Some(5_000)),
            Duration::from_millis(5_000),
            "an in-bound hint is taken verbatim"
        );
        assert_eq!(
            retry_delay(&policy, 1, Some(3_600_000)),
            policy.max_delay,
            "an over-bound hint clamps to max_delay"
        );
    }
}
