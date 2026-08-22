//! The engine's configuration: working defaults for everything but the
//! pipeline id, overridden through the `with_*` builders.

use std::num::NonZeroU32;
use std::time::Duration;
use std::{collections::BTreeMap, path::PathBuf};

use rdlt_core::commit::{CommitPolicy, WriteMode};
use rdlt_core::id::{PipelineId, StreamName};

use crate::policy::SchemaPolicy;

/// How the run driver retries transient failures: each retry is a full
/// run from committed state, slept into under exponential backoff.
///
/// The first retry sleeps ~`base_delay` (jittered), each further retry
/// doubles the bound, and `max_delay` caps it. A `Retry-After` hint
/// from a rate-limited connector takes precedence over the computed
/// backoff, bounded by `max_delay` and never jittered — the service
/// named its own delay.
///
/// Fields are public and the struct is constructible; start from
/// `Default` and override what differs:
/// `RetryPolicy { max_attempts: .., ..Default::default() }`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total run attempts, the first included: 5 means one run and up
    /// to four retries. `NonZeroU32` because zero attempts is not a
    /// policy, it is refusing to run.
    pub max_attempts: NonZeroU32,
    /// The first retry's backoff bound; each further retry doubles it.
    /// [`Config::with_retry_policy`] floors it at one millisecond.
    pub base_delay: Duration,
    /// The ceiling on any retry delay — computed backoff and
    /// `Retry-After` hints alike. [`Config::with_retry_policy`] raises
    /// it to at least `base_delay`.
    pub max_delay: Duration,
    /// How the computed bound becomes a sleep.
    pub jitter: Jitter,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: NonZeroU32::new(5).expect("5 is non-zero"),
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            jitter: Jitter::Full,
        }
    }
}

/// How a computed backoff bound becomes the actual sleep.
///
/// `#[non_exhaustive]`: a future strategy (equal jitter, decorrelated)
/// arrives as a new arm, not a breaking change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Jitter {
    /// Sleep the bound exactly. Deterministic — for tests and for
    /// embedders that coordinate retry timing themselves.
    None,
    /// Sleep uniformly within `0..=bound` (full jitter): retries from
    /// many pipelines hitting one recovering service spread out instead
    /// of arriving in synchronized waves.
    Full,
}

/// Configuration for an [`Engine`](crate::engine::Engine).
///
/// [`Config::new`] provides working defaults for everything except the pipeline
/// id; the `with_*` methods override them. The facade (`rdlt` crate) fronts this with
/// a typestate builder; embedders using the engine directly construct it here.
#[derive(Debug, Clone)]
pub struct Config {
    pub(crate) pipeline: PipelineId,
    pub(crate) write_mode: WriteMode,
    pub(crate) write_modes: BTreeMap<StreamName, WriteMode>,
    pub(crate) schema_policy: SchemaPolicy,
    pub(crate) commit_policy: CommitPolicy,
    pub(crate) batch_policy: rdlt_core::commit::BatchPolicy,
    pub(crate) workdir: Option<PathBuf>,
    pub(crate) byte_budget: usize,
    pub(crate) max_batch_cells: usize,
    pub(crate) max_streams_per_source: usize,
    pub(crate) max_concurrent_streams: usize,
    pub(crate) retry: RetryPolicy,
}

impl Config {
    /// Default bound on in-flight bytes per stage channel: 64 MiB. This is the
    /// engine's resident-memory cap; a producer that would exceed it parks until
    /// the consumer drains. Public because it is THE engine default other layers
    /// must agree with: the facade threads it into the connector provider's dial
    /// windows when a pipeline document sets no byte budget of its own — one
    /// constant, never a second literal that could drift.
    pub const DEFAULT_BYTE_BUDGET: usize = 64 << 20;

    /// The default bound on batch-assembly CELLS — `columns × rows` per
    /// outgoing batch: assembly null-fills absent columns and stamps lineage
    /// per cell, so the product bounds the engine-side expansion transient
    /// independently of the input's encoded size. 2²⁸ cells ≈ 1 GiB of
    /// null-fill worst case. Honest maximal pipelines (≤ ~260 columns at the
    /// 1M-row cap) sit under it; wider shapes raise it here.
    pub const DEFAULT_MAX_BATCH_CELLS: usize = 1 << 28;

    /// The default bound on streams one source may declare for a run: every
    /// stream costs plan-time validation and its own shred state, and
    /// discovery's list length is the one axis a source controls directly.
    /// The SPI's shared ceiling — the served side refuses a streams reply
    /// past the same number, so a host that raises this knob past it over a
    /// served source is refused at discovery, loudly, not silently
    /// truncated. Far past every honest discovery; a pipeline that
    /// genuinely reads more raises both here and at the wire.
    pub const DEFAULT_MAX_STREAMS_PER_SOURCE: usize =
        rdlt_connector::gate::MAX_DECLARED_STREAM_SPECS;

    /// The default bound on streams READING at once: 16. Discovery may
    /// declare up to [`Self::DEFAULT_MAX_STREAMS_PER_SOURCE`] streams, but
    /// only this many hold a read slot at a time — every concurrent read
    /// costs a connection (or file handle) on the source and its share of
    /// scheduler churn, and a thousand at once helps nobody. Streams past
    /// the bound wait; they hold no rows while waiting, so commits never
    /// wait on them.
    pub const DEFAULT_MAX_CONCURRENT_STREAMS: usize = 16;

    /// The most any caller may raise the byte budget to: 16 GiB. The
    /// budget backs a semaphore whose permit count has a ceiling of
    /// its own (a larger request would panic inside the constructor),
    /// and far below that a budget is no longer a containment bound at
    /// all — a pipeline document is not the place a process's memory
    /// ceiling is switched off.
    pub const MAX_BYTE_BUDGET: usize = 16 << 30;

    /// The most any caller may raise the cell budget to: 2³⁶ cells, the
    /// null-fill transient of a terabyte-scale batch. Past it the
    /// budget would bound nothing a machine could hold.
    pub const MAX_MAX_BATCH_CELLS: usize = 1 << 36;

    /// The most streams any caller may let one source declare: 64 Ki.
    /// Every declared stream costs plan-time state, and the wire
    /// refuses a declaration past its own ceiling anyway; a document
    /// raising this further asks for unbounded discovery state.
    pub const MAX_MAX_STREAMS_PER_SOURCE: usize = 64 * 1024;

    /// The most streams any caller may let read at once: 4096. A read
    /// slot is a connection on the source and a share of the byte
    /// budget; four thousand at once is already past any honest
    /// source, and the count backs a semaphore too.
    pub const MAX_MAX_CONCURRENT_STREAMS: usize = 4096;

    pub fn new(pipeline: impl Into<PipelineId>) -> Self {
        Self {
            pipeline: pipeline.into(),
            write_mode: WriteMode::Append,
            write_modes: BTreeMap::new(),
            schema_policy: SchemaPolicy::evolve(),
            commit_policy: CommitPolicy::default(),
            batch_policy: rdlt_core::commit::BatchPolicy::default(),
            workdir: None,
            byte_budget: Self::DEFAULT_BYTE_BUDGET,
            max_batch_cells: Self::DEFAULT_MAX_BATCH_CELLS,
            max_streams_per_source: Self::DEFAULT_MAX_STREAMS_PER_SOURCE,
            max_concurrent_streams: Self::DEFAULT_MAX_CONCURRENT_STREAMS,
            retry: RetryPolicy::default(),
        }
    }
    /// Sets the default write disposition for every stream.
    ///
    /// Streams named in [`Config::with_write_mode_for`] keep their own mode.
    pub fn with_write_mode(mut self, mode: WriteMode) -> Self {
        self.write_mode = mode;
        self
    }

    /// Overrides the write disposition for one stream.
    pub fn with_write_mode_for(mut self, stream: impl Into<StreamName>, mode: WriteMode) -> Self {
        self.write_modes.insert(stream.into(), mode);
        self
    }

    /// Sets the policy applied when a stream's observed schema drifts from the
    /// registered one.
    pub fn with_schema_policy(mut self, policy: SchemaPolicy) -> Self {
        self.schema_policy = policy;
        self
    }

    /// How much to accumulate before each destination write.
    ///
    /// Destination-agnostic: the engine does the accumulating, so
    /// every connector benefits without re-solving it. The default
    /// writes each source batch straight through.
    pub fn with_batch_policy(mut self, policy: rdlt_core::commit::BatchPolicy) -> Self {
        self.batch_policy = policy;
        self
    }

    /// Sets when the engine commits: per checkpoint batch, or at run end.
    ///
    /// Deliberately infallible: both product surfaces
    /// validate the policy before reaching here — the YAML parse and
    /// the facade's `build()` — and an engine-direct embedder setting a
    /// threshold-less policy gets the degenerate whole-run-one-crash-
    /// window shape the loader's final flush still commits safely;
    /// tightening this to a checked setter would break every existing
    /// chain site for no added protection.
    pub fn with_commit_policy(mut self, policy: CommitPolicy) -> Self {
        self.commit_policy = policy;
        self
    }

    /// The configured commit policy: the facade's `build()`
    /// runs `CommitPolicy::check` on it — the same refusal the YAML
    /// parse makes — so a threshold-less policy cannot reach a run
    /// through the builder either.
    pub fn commit_policy(&self) -> &CommitPolicy {
        &self.commit_policy
    }

    /// Refuse configuration this destination cannot honor, and policies
    /// that could never fire — THE validation a constructed pipeline
    /// passes before it exists. No network or destination I/O happens
    /// here; the checks are purely against the declared capabilities
    /// and the config's own values. This is where the checks live (not
    /// in the facade's builder): the engine owns what its config means.
    ///
    /// Three rules:
    ///
    /// - **Merge capability.** Which streams request Merge — the
    ///   default write mode, and any named per-stream overrides — must
    ///   be a mode the destination declared.
    /// - **Merge keys are non-empty.** `WriteMode::Merge { key }` and a
    ///   source's `StreamSpec::primary_key` are NOT independent — for a
    ///   structured stream the two must AGREE (name the same columns,
    ///   as a set), and the engine enforces that agreement at plan time
    ///   in `validate_streams` once the source's streams are known.
    ///   Validation runs before any stream exists, so it enforces the
    ///   stream-agnostic half only: a Merge write mode must carry at
    ///   least one key column.
    /// - **The commit policy can fire.** A threshold-less policy never
    ///   fires — it would hold the whole run in one crash window, the
    ///   exact shape the type refuses everywhere else.
    pub fn validate(
        &self,
        capabilities: &rdlt_connector::destination::Capabilities,
        destination_name: impl std::fmt::Display,
    ) -> Result<(), rdlt_core::error::Error> {
        self.check_resources()?;
        let merge_default = matches!(self.write_mode(), WriteMode::Merge { .. });
        let merge_overrides: Vec<&StreamName> = self
            .write_modes()
            .iter()
            .filter(|(_, mode)| matches!(mode, WriteMode::Merge { .. }))
            .map(|(stream, _)| stream)
            .collect();
        if !capabilities.merge && (merge_default || !merge_overrides.is_empty()) {
            return Err(rdlt_core::error::Error::config(format!(
                "destination `{destination_name}` does not support Merge (requested {})",
                if merge_default {
                    "as the default write mode".to_owned()
                } else {
                    format!(
                        "for streams: {}",
                        merge_overrides
                            .iter()
                            .map(|s| s.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                }
            )));
        }
        for (stream, mode) in self.write_modes() {
            if let WriteMode::Merge { key } = mode
                && key.is_empty()
            {
                return Err(rdlt_core::error::Error::config(format!(
                    "stream `{stream}`: Merge requires at least one key column"
                )));
            }
        }
        if let WriteMode::Merge { key } = self.write_mode()
            && key.is_empty()
        {
            return Err(rdlt_core::error::Error::config(
                "Merge requires at least one key column",
            ));
        }
        self.commit_policy
            .check()
            .map_err(|reason| rdlt_core::error::Error::config(reason.to_string()))
    }

    /// Every resource knob against its ceiling — the one boundary both
    /// a document's `resources:` block and the builder's own setters
    /// answer to, so a value that would disable a containment bound,
    /// or panic the semaphore behind it, refuses typed before any
    /// connector is dialed. The setters clamp only at the bottom (a
    /// zero is a request for the smallest enforceable value); the top
    /// is refused here rather than clamped, because a caller asking for
    /// the impossible should hear so.
    pub fn check_resources(&self) -> Result<(), rdlt_core::error::Error> {
        let ceilings = [
            ("byte_budget", self.byte_budget, Self::MAX_BYTE_BUDGET),
            (
                "max_batch_cells",
                self.max_batch_cells,
                Self::MAX_MAX_BATCH_CELLS,
            ),
            (
                "max_streams_per_source",
                self.max_streams_per_source,
                Self::MAX_MAX_STREAMS_PER_SOURCE,
            ),
            (
                "max_concurrent_streams",
                self.max_concurrent_streams,
                Self::MAX_MAX_CONCURRENT_STREAMS,
            ),
        ];
        for (knob, value, ceiling) in ceilings {
            if value > ceiling {
                return Err(rdlt_core::error::Error::config(format!(
                    "resources.{knob} is {value}, over the {ceiling} ceiling — the bound \
                     exists to contain what a connector may make the engine hold, and a \
                     value past it would contain nothing"
                )));
            }
        }
        Ok(())
    }

    /// Sets the working directory that holds the write-ahead log.
    ///
    /// Without a workdir the engine runs without durable recovery: a crash loses
    /// staged-but-uncommitted work instead of replaying it.
    pub fn with_workdir(mut self, workdir: impl Into<PathBuf>) -> Self {
        self.workdir = Some(workdir.into());
        self
    }

    /// Sets the bound on in-flight bytes per stage channel — the resident-memory cap.
    pub fn with_byte_budget(mut self, bytes: usize) -> Self {
        // A caller asking for no buffering receives the smallest enforceable
        // window; the memory ceiling is never silently switched off.
        self.byte_budget = bytes.max(1);
        self
    }

    /// Sets the batch-assembly cell budget (`columns × rows`, see
    /// [`Self::DEFAULT_MAX_BATCH_CELLS`]). Raise it for honestly wide × large
    /// batches; lowering tightens the engine-side expansion ceiling. Zero
    /// clamps to one cell, never off — the budget is what refuses the
    /// 16 GiB null-fill amplification.
    pub fn with_max_batch_cells(mut self, cells: usize) -> Self {
        self.max_batch_cells = cells.max(1);
        self
    }

    /// Sets the most streams one source may declare for a run (see
    /// [`Self::DEFAULT_MAX_STREAMS_PER_SOURCE`]). Raise it for genuinely huge
    /// discoveries; the bound exists because the stream list is the one
    /// discovery axis a source controls directly. Over a served
    /// (out-of-process) source, raising this past 1024 needs TWO
    /// connector-side ceilings raised in step, and neither is yet
    /// configurable — both a fixed 1024 today: the wire client refuses
    /// a streams reply declaring more specs than that (discovery fails
    /// outright), and a served source admits at most that many
    /// concurrent reads (extra reads refused, not merely throttled).
    pub fn with_max_streams_per_source(mut self, streams: usize) -> Self {
        self.max_streams_per_source = streams.max(1);
        self
    }

    /// Sets how many streams may read at once (see
    /// [`Self::DEFAULT_MAX_CONCURRENT_STREAMS`]). Zero clamps to one —
    /// a run must always admit at least one reader. Raising it toward
    /// the stream count restores the read-everything-at-once shape.
    pub fn with_max_concurrent_streams(mut self, streams: usize) -> Self {
        self.max_concurrent_streams = streams.max(1);
        self
    }

    /// Sets how transient failures are retried (see [`RetryPolicy`]).
    ///
    /// Clamped silently like the sibling knobs: `base_delay` floors at
    /// one millisecond (a zero base would make every computed backoff
    /// zero — hot-loop retries against a recovering destination), and
    /// `max_delay` rises to at least `base_delay` (a ceiling below the
    /// base would silently shrink every bound instead of capping the
    /// ladder).
    pub fn with_retry_policy(mut self, mut policy: RetryPolicy) -> Self {
        policy.base_delay = policy.base_delay.max(Duration::from_millis(1));
        policy.max_delay = policy.max_delay.max(policy.base_delay);
        self.retry = policy;
        self
    }

    /// The configured retry policy.
    pub fn retry_policy(&self) -> &RetryPolicy {
        &self.retry
    }

    /// Returns the default write disposition.
    pub fn write_mode(&self) -> &WriteMode {
        &self.write_mode
    }

    /// Returns the per-stream write-mode overrides.
    pub fn write_modes(&self) -> &BTreeMap<StreamName, WriteMode> {
        &self.write_modes
    }

    pub(crate) fn write_mode_for(&self, stream: &StreamName) -> WriteMode {
        self.write_modes
            .get(stream)
            .cloned()
            .unwrap_or_else(|| self.write_mode.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every knob refuses one past its ceiling and admits the ceiling
    /// itself — typed, naming the knob — and `usize::MAX` (what a
    /// document can spell) never reaches a semaphore constructor.
    #[test]
    fn resource_knobs_refuse_past_their_ceilings_and_admit_them() {
        type Set = fn(Config, usize) -> Config;
        let knobs: [(&str, Set, usize); 4] = [
            (
                "byte_budget",
                Config::with_byte_budget,
                Config::MAX_BYTE_BUDGET,
            ),
            (
                "max_batch_cells",
                Config::with_max_batch_cells,
                Config::MAX_MAX_BATCH_CELLS,
            ),
            (
                "max_streams_per_source",
                Config::with_max_streams_per_source,
                Config::MAX_MAX_STREAMS_PER_SOURCE,
            ),
            (
                "max_concurrent_streams",
                Config::with_max_concurrent_streams,
                Config::MAX_MAX_CONCURRENT_STREAMS,
            ),
        ];
        for (knob, set, ceiling) in knobs {
            set(Config::new("p"), ceiling)
                .check_resources()
                .expect("the ceiling is admitted");
            for over in [ceiling + 1, usize::MAX] {
                let error = set(Config::new("p"), over)
                    .check_resources()
                    .expect_err("past the ceiling refuses");
                assert!(
                    error
                        .to_string()
                        .contains(&format!("resources.{knob} is {over}")),
                    "{knob}: {error}"
                );
            }
        }
        // The semaphore-backed ceilings stay under tokio's permit
        // ceiling: the constructors at the ceiling do not panic.
        let _budget = tokio::sync::Semaphore::new(Config::MAX_BYTE_BUDGET);
        let _readers = tokio::sync::Semaphore::new(Config::MAX_MAX_CONCURRENT_STREAMS);
    }

    #[test]
    fn a_zero_byte_budget_clamps_to_the_smallest_enforceable_window() {
        assert_eq!(Config::new("p").with_byte_budget(0).byte_budget, 1);
    }

    /// The concurrency and retry knobs: defaults as documented, the
    /// zero clamp (a run must admit at least one reader), and the
    /// plumb-through of an overriding policy.
    #[test]
    fn concurrency_and_retry_knobs_default_clamp_and_plumb() {
        let config = Config::new("p");
        assert_eq!(config.max_concurrent_streams, 16);
        assert_eq!(config.retry, RetryPolicy::default());
        assert_eq!(config.retry.max_attempts.get(), 5);
        assert_eq!(config.retry.base_delay, Duration::from_millis(100));
        assert_eq!(config.retry.max_delay, Duration::from_secs(30));
        assert_eq!(config.retry.jitter, Jitter::Full);

        assert_eq!(
            Config::new("p")
                .with_max_concurrent_streams(0)
                .max_concurrent_streams,
            1
        );

        let policy = RetryPolicy {
            max_attempts: NonZeroU32::new(2).expect("non-zero"),
            base_delay: Duration::from_millis(5),
            jitter: Jitter::None,
            ..Default::default()
        };
        assert_eq!(
            Config::new("p").with_retry_policy(policy.clone()).retry,
            policy
        );
    }

    /// The delay floors, clamped silently like the sibling knobs: a
    /// zero `base_delay` would make every computed backoff zero — a
    /// hot-loop retry against a recovering destination — so it floors
    /// at one millisecond; a `max_delay` below `base_delay` would
    /// silently degrade every bound to the smaller value, so it raises
    /// to meet the base. In-policy values pass untouched (the roundtrip
    /// pin above).
    #[test]
    fn retry_delays_clamp_at_their_floors() {
        let floored = Config::new("p")
            .with_retry_policy(RetryPolicy {
                base_delay: Duration::ZERO,
                ..Default::default()
            })
            .retry;
        assert_eq!(
            floored.base_delay,
            Duration::from_millis(1),
            "a zero base delay floors at 1ms"
        );

        let raised = Config::new("p")
            .with_retry_policy(RetryPolicy {
                base_delay: Duration::from_secs(60),
                max_delay: Duration::from_secs(1),
                ..Default::default()
            })
            .retry;
        assert_eq!(
            raised.max_delay,
            Duration::from_secs(60),
            "a ceiling below the base raises to meet it"
        );
        assert_eq!(raised.base_delay, Duration::from_secs(60));
    }
}
