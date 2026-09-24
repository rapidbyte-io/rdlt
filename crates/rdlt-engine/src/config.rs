//! Engine configuration: resources, the batch, commit and retry policies, validated at build.

mod batch;
#[cfg(test)]
mod tests;

use std::num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize};
use std::time::Duration;

use crate::error::Error;

pub use batch::BatchPolicy;

/// When the engine commits: whichever threshold is reached first.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommitPolicy {
    every: Option<Duration>,
    rows: Option<NonZeroU64>,
    bytes: Option<NonZeroU64>,
}

impl CommitPolicy {
    /// A policy that commits after `every` elapses, after `rows` rows or after `bytes` bytes;
    /// at least one threshold must be set, and none may be zero.
    pub fn new(
        every: Option<Duration>,
        rows: Option<u64>,
        bytes: Option<u64>,
    ) -> Result<Self, Error> {
        let invalid = |what: &str| {
            Error::config(format!("commit policy: {what}")).with_code("commit_policy_invalid")
        };
        if every.is_none() && rows.is_none() && bytes.is_none() {
            return Err(invalid("set at least one of every, rows and bytes"));
        }
        if every == Some(Duration::ZERO) {
            return Err(invalid("every must be longer than zero"));
        }
        let nonzero = |value: Option<u64>, name: &str| match value {
            Some(value) => NonZeroU64::new(value)
                .map(Some)
                .ok_or_else(|| invalid(&format!("{name} must be more than zero"))),
            None => Ok(None),
        };
        Ok(Self {
            every,
            rows: nonzero(rows, "rows")?,
            bytes: nonzero(bytes, "bytes")?,
        })
    }

    /// The commit interval.
    pub fn every(&self) -> Option<Duration> {
        self.every
    }

    /// The row threshold.
    pub fn rows(&self) -> Option<NonZeroU64> {
        self.rows
    }

    /// The byte threshold.
    pub fn bytes(&self) -> Option<NonZeroU64> {
        self.bytes
    }

    /// Whether `rows` and `bytes` written since the last commit reach a threshold.
    pub(crate) fn is_due(&self, rows: u64, bytes: u64) -> bool {
        self.rows.is_some_and(|limit| rows >= limit.get())
            || self.bytes.is_some_and(|limit| bytes >= limit.get())
    }
}

impl Default for CommitPolicy {
    /// Every 60 seconds or 1 GiB, whichever comes first.
    fn default() -> Self {
        Self {
            every: Some(Duration::from_secs(60)),
            rows: None,
            bytes: NonZeroU64::new(1 << 30),
        }
    }
}

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
    batch: BatchPolicy,
    commit: CommitPolicy,
    retry: RetryPolicy,
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
            batch: None,
            commit: None,
            retry: None,
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

    /// How pushes are coalesced and JSON is shredded.
    pub fn batch(&self) -> &BatchPolicy {
        &self.batch
    }

    /// When the engine commits.
    pub fn commit(&self) -> &CommitPolicy {
        &self.commit
    }

    /// How the engine retries failed attempts.
    pub fn retry(&self) -> &RetryPolicy {
        &self.retry
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
            batch: BatchPolicy::default(),
            commit: CommitPolicy::default(),
            retry: RetryPolicy::default(),
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
    batch: Option<BatchPolicy>,
    commit: Option<CommitPolicy>,
    retry: Option<RetryPolicy>,
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

    /// How to coalesce pushes and shred JSON (default: [`BatchPolicy::default`]).
    #[must_use]
    pub fn batch(mut self, policy: BatchPolicy) -> Self {
        self.batch = Some(policy);
        self
    }

    /// When to commit (default: every 60 s or 1 GiB).
    #[must_use]
    pub fn commit(mut self, policy: CommitPolicy) -> Self {
        self.commit = Some(policy);
        self
    }

    /// How to retry (default: [`RetryPolicy::default`]).
    #[must_use]
    pub fn retry(mut self, policy: RetryPolicy) -> Self {
        self.retry = Some(policy);
        self
    }

    /// Validates the settings: every count and size is more than zero, and the retry policy's
    /// first delay is no longer than its longest.
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
        Ok(EngineConfig {
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
            batch: self.batch.unwrap_or(defaults.batch),
            commit: self.commit.unwrap_or(defaults.commit),
            retry,
        })
    }
}

/// `value` as a non-zero number, or `default` when unset; `None` when `value` is zero.
fn nonzero<T, N>(value: Option<T>, default: N, make: fn(T) -> Option<N>) -> Option<N> {
    value.map_or(Some(default), make)
}
