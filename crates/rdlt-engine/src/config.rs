//! Engine configuration: resources, the batch, commit and retry policies, validated at build.

mod batch;
mod commit;
mod growth;
mod limits;
#[cfg(test)]
mod tests;

use std::num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize};
use std::time::Duration;

use crate::error::Error;

pub use batch::BatchPolicy;
pub use commit::CommitPolicy;
pub use growth::GrowthLimits;

/// How many attempts a run makes and how long it waits between them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    max_attempts: NonZeroU32,
    initial: Duration,
    max_delay: Duration,
    reset_after_progress: bool,
}

impl RetryPolicy {
    /// Sets the number of attempts, the first included; zero means one.
    #[must_use]
    pub fn max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = NonZeroU32::new(attempts).unwrap_or(NonZeroU32::MIN);
        self
    }

    /// Sets the longest delay after the first failure.
    #[must_use]
    pub fn initial(mut self, delay: Duration) -> Self {
        self.initial = delay;
        self
    }

    /// Sets the longest delay after any failure.
    #[must_use]
    pub fn max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = delay;
        self
    }

    /// Sets whether a commit resets the count of failed attempts.
    #[must_use]
    pub fn reset_after_progress(mut self, reset: bool) -> Self {
        self.reset_after_progress = reset;
        self
    }

    /// The number of attempts, the first included.
    pub fn attempts(&self) -> NonZeroU32 {
        self.max_attempts
    }

    /// Whether a commit resets the count of failed attempts.
    pub fn resets_after_progress(&self) -> bool {
        self.reset_after_progress
    }

    /// The wait a failure asked for, `asked`, held between the first delay and the longest: a
    /// connector's answer is untrusted, and one that asks for no wait would spin the run, one
    /// that asks for years park it.
    pub(crate) fn within(&self, asked: Duration) -> Duration {
        asked.max(self.initial).min(self.max_delay)
    }

    /// The delay after `failures` consecutive failures: exponential backoff with full jitter,
    /// drawn from `random`.
    pub(crate) fn delay(&self, failures: NonZeroU32, random: u64) -> Duration {
        let doublings = failures.get().saturating_sub(1).min(31);
        let ceiling = self
            .initial
            .saturating_mul(1 << doublings)
            .min(self.max_delay);
        let nanos = u64::try_from(ceiling.as_nanos()).unwrap_or(u64::MAX);
        Duration::from_nanos(random % nanos.saturating_add(1))
    }
}

impl Default for RetryPolicy {
    /// Five attempts, delays from one second up to five minutes, reset after progress.
    fn default() -> Self {
        Self {
            max_attempts: NonZeroU32::new(5).unwrap_or(NonZeroU32::MIN),
            initial: Duration::from_secs(1),
            max_delay: Duration::from_secs(300),
            reset_after_progress: true,
        }
    }
}

/// Resources and policies of an [`Engine`](crate::Engine); built by [`EngineConfig::builder`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineConfig {
    memory: NonZeroU64,
    lanes: Option<NonZeroU16>,
    lane_window: NonZeroUsize,
    partitions: NonZeroUsize,
    partition_buffer: NonZeroUsize,
    barrier_wait: Duration,
    memory_wait: Duration,
    connector_wait: Duration,
    stop_wait: Duration,
    close_wait: Duration,
    batch: BatchPolicy,
    /// The commit policy set, where one is; each run resolves an unset one for what it reads.
    commit: Option<CommitPolicy>,
    replan: Duration,
    retry: RetryPolicy,
    /// The limits configured, before the memory budget lowers them.
    limits: rdlt_wire::Limits,
    growth: GrowthLimits,
}

impl EngineConfig {
    /// A builder holding the defaults.
    pub fn builder() -> EngineConfigBuilder {
        EngineConfigBuilder {
            memory: None,
            lanes: None,
            lane_window: None,
            partitions: None,
            partition_buffer: None,
            barrier_wait: None,
            memory_wait: None,
            connector_wait: None,
            stop_wait: None,
            close_wait: None,
            batch: None,
            commit: None,
            replan: None,
            retry: None,
            limits: None,
            growth: None,
        }
    }

    /// Bytes of in-flight batches the engine holds at most.
    pub fn memory(&self) -> NonZeroU64 {
        self.memory
    }

    /// Writers per destination; `None` uses one per core, up to the destination's limit.
    pub fn lanes(&self) -> Option<NonZeroU16> {
        self.lanes
    }

    /// Writes queued per lane.
    pub fn lane_window(&self) -> NonZeroUsize {
        self.lane_window
    }

    /// Partitions read at once.
    pub fn partitions(&self) -> NonZeroUsize {
        self.partitions
    }

    /// Events buffered between a partition's read and the engine.
    pub fn partition_buffer(&self) -> NonZeroUsize {
        self.partition_buffer
    }

    /// How long a commit waits for partitions to answer its barrier.
    pub fn barrier_wait(&self) -> Duration {
        self.barrier_wait
    }

    /// How long a request waits for room in the memory budget before the attempt fails.
    pub fn memory_wait(&self) -> Duration {
        self.memory_wait
    }

    /// How long one call into a connector other than a read may take before it fails as
    /// transient.
    ///
    /// The calls are a check, a discovery, a plan, an acknowledgement, an open, a schema change,
    /// a writer's opening, a write, a flush, a commit and a close.
    pub fn connector_wait(&self) -> Duration {
        self.connector_wait
    }

    /// How long a read asked to stop may take to end; one that takes longer is dropped, as if
    /// it had ended where it stood.
    pub fn stop_wait(&self) -> Duration {
        self.stop_wait
    }

    /// How long a failed attempt waits for the destination to close the session it opened; one
    /// that takes longer is left to the destination to release.
    pub fn close_wait(&self) -> Duration {
        self.close_wait
    }

    /// How pushes are coalesced and JSON is shredded.
    pub fn batch(&self) -> &BatchPolicy {
        &self.batch
    }

    /// When the engine commits, where a policy was set; an unset one resolves per run
    /// ([`CommitPolicy::default`], or [`CommitPolicy::streaming`] where the run follows its
    /// source or reads changes).
    pub fn commit(&self) -> Option<&CommitPolicy> {
        self.commit.as_ref()
    }

    /// The commit policy of a run that follows its source or reads changes where `streaming`.
    pub(crate) fn commit_for(&self, streaming: bool) -> CommitPolicy {
        match (self.commit, streaming) {
            (Some(policy), _) => policy,
            (None, true) => CommitPolicy::streaming(),
            (None, false) => CommitPolicy::default(),
        }
    }

    /// How often a following run plans its streams again.
    pub fn replan(&self) -> Duration {
        self.replan
    }

    /// How the engine retries failed attempts.
    pub fn retry(&self) -> &RetryPolicy {
        &self.retry
    }

    /// What a pipeline's tables and state may grow to.
    pub fn growth(&self) -> &GrowthLimits {
        &self.growth
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            memory: NonZeroU64::new(256 << 20).unwrap_or(NonZeroU64::MIN),
            lanes: None,
            lane_window: NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
            partitions: NonZeroUsize::new(16).unwrap_or(NonZeroUsize::MIN),
            partition_buffer: NonZeroUsize::new(16).unwrap_or(NonZeroUsize::MIN),
            barrier_wait: Duration::from_secs(5),
            memory_wait: crate::limits::BUDGET_WAIT,
            connector_wait: Duration::from_mins(30),
            stop_wait: Duration::from_secs(60),
            close_wait: Duration::from_secs(60),
            batch: BatchPolicy::default(),
            commit: None,
            replan: Duration::from_secs(60),
            retry: RetryPolicy::default(),
            limits: rdlt_wire::Limits::default(),
            growth: GrowthLimits::default(),
        }
    }
}

/// Builds an [`EngineConfig`], validating every value in [`build`](Self::build).
#[derive(Clone, Debug)]
pub struct EngineConfigBuilder {
    memory: Option<u64>,
    lanes: Option<u16>,
    lane_window: Option<usize>,
    partitions: Option<usize>,
    partition_buffer: Option<usize>,
    barrier_wait: Option<Duration>,
    memory_wait: Option<Duration>,
    connector_wait: Option<Duration>,
    stop_wait: Option<Duration>,
    close_wait: Option<Duration>,
    batch: Option<BatchPolicy>,
    commit: Option<CommitPolicy>,
    replan: Option<Duration>,
    retry: Option<RetryPolicy>,
    limits: Option<rdlt_wire::Limits>,
    growth: Option<GrowthLimits>,
}

impl EngineConfigBuilder {
    /// Bytes of in-flight batches (default 256 MiB).
    #[must_use]
    pub fn memory(mut self, bytes: u64) -> Self {
        self.memory = Some(bytes);
        self
    }

    /// Destination writers (default: one per core, up to the destination's limit).
    #[must_use]
    pub fn lanes(mut self, lanes: u16) -> Self {
        self.lanes = Some(lanes);
        self
    }

    /// Writes queued per lane (default 4).
    #[must_use]
    pub fn lane_window(mut self, writes: usize) -> Self {
        self.lane_window = Some(writes);
        self
    }

    /// Partitions read at once (default 16).
    #[must_use]
    pub fn partitions(mut self, partitions: usize) -> Self {
        self.partitions = Some(partitions);
        self
    }

    /// Events buffered per partition (default 16).
    #[must_use]
    pub fn partition_buffer(mut self, events: usize) -> Self {
        self.partition_buffer = Some(events);
        self
    }

    /// How long a commit waits for barrier answers (default 5 s).
    #[must_use]
    pub fn barrier_wait(mut self, wait: Duration) -> Self {
        self.barrier_wait = Some(wait);
        self
    }

    /// How long a request waits for room in the memory budget before the attempt fails with
    /// what held the budget (default an hour): longer than any call of the destination may take.
    #[must_use]
    pub fn memory_wait(mut self, wait: Duration) -> Self {
        self.memory_wait = Some(wait);
        self
    }

    /// How long one call into a connector, other than a read, may take (default 30 min); more
    /// than zero.
    #[must_use]
    pub fn connector_wait(mut self, wait: Duration) -> Self {
        self.connector_wait = Some(wait);
        self
    }

    /// How long a read asked to stop may take to end (default 60 s); more than zero.
    #[must_use]
    pub fn stop_wait(mut self, wait: Duration) -> Self {
        self.stop_wait = Some(wait);
        self
    }

    /// How long a failed attempt waits for its session to close (default 60 s); more than zero.
    #[must_use]
    pub fn close_wait(mut self, wait: Duration) -> Self {
        self.close_wait = Some(wait);
        self
    }

    /// How to coalesce pushes and shred JSON (default: [`BatchPolicy::default`]).
    #[must_use]
    pub fn batch(mut self, policy: BatchPolicy) -> Self {
        self.batch = Some(policy);
        self
    }

    /// When to commit (default: every 60 s or 1 GiB, or every 10 s where a run follows its source
    /// or reads changes).
    #[must_use]
    pub fn commit(mut self, policy: CommitPolicy) -> Self {
        self.commit = Some(policy);
        self
    }

    /// How often a following run plans its streams again (default 60 s); more than zero.
    #[must_use]
    pub fn replan(mut self, every: Duration) -> Self {
        self.replan = Some(every);
        self
    }

    /// How to retry (default: [`RetryPolicy::default`]).
    #[must_use]
    pub fn retry(mut self, policy: RetryPolicy) -> Self {
        self.retry = Some(policy);
        self
    }

    /// What tables and state may grow to (default: [`GrowthLimits::default`]).
    #[must_use]
    pub fn growth(mut self, limits: GrowthLimits) -> Self {
        self.growth = Some(limits);
        self
    }

    /// Validates the settings: every count and size is more than zero, the retry policy's first
    /// delay is no longer than its longest, and the memory is at least what
    /// [`EngineConfig::least_memory`] says its partitions need.
    pub fn build(self) -> Result<EngineConfig, Error> {
        let defaults = EngineConfig::default();
        let invalid = |name: &str| {
            Error::config(format!("{name} must be more than zero")).with_code("config_invalid")
        };
        let retry = self.retry.unwrap_or(defaults.retry);
        if retry.initial > retry.max_delay {
            return Err(
                Error::config("retry: initial delay exceeds max_delay").with_code("config_invalid")
            );
        }
        let config = EngineConfig {
            memory: nonzero(self.memory, defaults.memory, NonZeroU64::new)
                .ok_or_else(|| invalid("memory"))?,
            lanes: match self.lanes {
                Some(lanes) => Some(NonZeroU16::new(lanes).ok_or_else(|| invalid("lanes"))?),
                None => None,
            },
            lane_window: nonzero(self.lane_window, defaults.lane_window, NonZeroUsize::new)
                .ok_or_else(|| invalid("lane_window"))?,
            partitions: nonzero(self.partitions, defaults.partitions, NonZeroUsize::new)
                .ok_or_else(|| invalid("partitions"))?,
            partition_buffer: nonzero(
                self.partition_buffer,
                defaults.partition_buffer,
                NonZeroUsize::new,
            )
            .ok_or_else(|| invalid("partition_buffer"))?,
            barrier_wait: self.barrier_wait.unwrap_or(defaults.barrier_wait),
            memory_wait: match self.memory_wait {
                Some(Duration::ZERO) => return Err(invalid("memory_wait")),
                wait => wait.unwrap_or(defaults.memory_wait),
            },
            connector_wait: positive(self.connector_wait, defaults.connector_wait)
                .ok_or_else(|| invalid("connector_wait"))?,
            stop_wait: positive(self.stop_wait, defaults.stop_wait)
                .ok_or_else(|| invalid("stop_wait"))?,
            close_wait: positive(self.close_wait, defaults.close_wait)
                .ok_or_else(|| invalid("close_wait"))?,
            batch: self.batch.unwrap_or(defaults.batch),
            commit: self.commit,
            replan: match self.replan {
                Some(Duration::ZERO) => return Err(invalid("replan")),
                replan => replan.unwrap_or(defaults.replan),
            },
            retry,
            limits: self.limits.unwrap_or(defaults.limits),
            growth: self.growth.unwrap_or(defaults.growth),
        };
        config.admit_memory()?;
        Ok(config)
    }
}

/// `value`, or `default` when unset; `None` when `value` is zero.
fn positive(value: Option<Duration>, default: Duration) -> Option<Duration> {
    Some(value.unwrap_or(default)).filter(|wait| !wait.is_zero())
}

/// `value` as a non-zero number, or `default` when unset; `None` when `value` is zero.
fn nonzero<T, N>(value: Option<T>, default: N, make: fn(T) -> Option<N>) -> Option<N> {
    value.map_or(Some(default), make)
}
