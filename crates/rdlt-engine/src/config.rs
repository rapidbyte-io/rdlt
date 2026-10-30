//! Engine configuration: resources, the batch, commit and retry policies, validated at build.

mod batch;
mod commit;
mod growth;
mod limits;
#[cfg(test)]
mod tests;

use std::num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize};
use std::time::Duration;

use rdlt_connector::limits::MAX_PLAN_PARTITIONS;

use crate::error::{Error, codes};
use crate::limits::BUDGET_WAIT;

pub use batch::BatchPolicy;
pub use commit::CommitPolicy;
pub use growth::GrowthLimits;

/// How many attempts a run makes and how long it waits between them; a commit the destination
/// answers transiently is sent again in place as many times, with as long between its tries.
///
/// A run that makes no progress sends a commit at most the square of its attempts times: each
/// attempt sends it as many times as the run makes attempts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    max_attempts: NonZeroU32,
    initial: Duration,
    max_delay: Duration,
    reset_after_progress: bool,
}

impl RetryPolicy {
    /// Sets the number of attempts, the first included, and of tries of a commit; zero means
    /// one.
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

    /// The number of attempts, the first included, and of tries of a commit.
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

    /// Bytes: the memory budget, across all its shares, which the engine's reservations never pass.
    ///
    /// The shares are fixed parts of it: pushes and what lowering makes of them, cursors waiting
    /// for a commit, the log's frames and a carry's read, what commits record of tables,
    /// what reads keep, and what decoding connectors' answers holds.
    pub fn memory(&self) -> NonZeroU64 {
        self.memory
    }

    /// Writers per destination; `None` uses one per core of [`Env::cores`](crate::Env::cores),
    /// up to the destination's limit.
    pub fn lanes(&self) -> Option<NonZeroU16> {
        self.lanes
    }

    /// Writes queued per lane.
    pub fn lane_window(&self) -> NonZeroUsize {
        self.lane_window
    }

    /// Partitions read at once, at most [`MAX_PLAN_PARTITIONS`], the most an attempt reads.
    ///
    /// What reads keep beside their events, a quarter of the memory, is divided into a part a
    /// partition, or into as many parts of twice the protocol's least dictionary as it holds where
    /// those are fewer: 4 MiB a part at the defaults, and 512 KiB from 128 partitions up at the
    /// default memory. The cursor, schema, column and dictionary limits [`EngineConfig::limits`]
    /// gives fall as this rises, to a schema's message of 52,428 bytes and a table of 936 columns
    /// at the default memory, against 419,430 bytes and 7,489 columns at the default 16.
    ///
    /// A read that is not followed holds a whole part while it reads, so no more of them read at
    /// once than the parts. A following run follows fewer unbounded partitions than this, however
    /// much memory it has: their reads hold no part of their own, but keep what they keep in the
    /// parts no other read holds, all but one, and fail with `limit_exceeded` where that needs a
    /// part none is free for.
    ///
    /// A source whose calls are bounded ([`Source::max_calls`](rdlt_connector::Source::max_calls))
    /// is read no more at once than its calls less one, where those are fewer, the parts still
    /// divided as for this: a served source, 198 at once, of which 197 followed unbounded
    /// partitions.
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

    /// How long one call into a connector other than a read may take before it fails with the
    /// engine's own retryable error, coded [`DEADLINE_EXCEEDED`](crate::codes::DEADLINE_EXCEEDED).
    ///
    /// The calls are a check, a discovery, a plan, an acknowledgement, an open, a schema change,
    /// a writer's opening, a write, a flush, a commit and a close. A wait for room in the memory
    /// budget ends at twice this, and an hour at least, so a call that hangs fails at its own
    /// deadline before a wait that began while it ran.
    pub fn connector_wait(&self) -> Duration {
        self.connector_wait
    }

    /// How long a request waits for room in the memory budget before the attempt fails with
    /// what held the budget: twice [`EngineConfig::connector_wait`], and [`BUDGET_WAIT`] at
    /// least.
    ///
    /// It is a backstop against a defect in the budget's own protocol, not a bound on latency: a
    /// call that hangs fails at its own deadline, naming the call, before a wait that began less
    /// than one connector deadline before the call reaches this one.
    pub(crate) fn budget_wait(&self) -> Duration {
        BUDGET_WAIT.max(self.connector_wait.saturating_mul(2))
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
    /// Bytes: the memory budget, across all its shares (default 256 MiB).
    #[must_use]
    pub fn memory(mut self, bytes: u64) -> Self {
        self.memory = Some(bytes);
        self
    }

    /// Destination writers (default: one per core of [`Env::cores`](crate::Env::cores), up to
    /// the destination's limit).
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

    /// Partitions read at once (default 16), at most [`MAX_PLAN_PARTITIONS`]; more lower what
    /// each read may send, as [`EngineConfig::partitions`] says.
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

    /// How long one call into a connector, other than a read, may take (default 30 min); more
    /// than zero.
    ///
    /// A wait for room in the memory budget ends at twice it, and an hour at least.
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

    /// Validates the settings: every count and size is more than zero, the partitions read at
    /// once are no more than [`MAX_PLAN_PARTITIONS`], the retry policy's first delay is no longer
    /// than its longest, and the memory is at least what [`EngineConfig::least_memory`] says its
    /// partitions need.
    pub fn build(self) -> Result<EngineConfig, Error> {
        let defaults = EngineConfig::default();
        let invalid = |name: &str| {
            Error::config(format!("{name} must be more than zero")).with_code(codes::CONFIG_INVALID)
        };
        let retry = self.retry.unwrap_or(defaults.retry);
        if retry.initial > retry.max_delay {
            return Err(Error::config("retry: initial delay exceeds max_delay")
                .with_code(codes::CONFIG_INVALID));
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
            partitions: read_at_once(self.partitions, defaults.partitions)?,
            partition_buffer: nonzero(
                self.partition_buffer,
                defaults.partition_buffer,
                NonZeroUsize::new,
            )
            .ok_or_else(|| invalid("partition_buffer"))?,
            barrier_wait: self.barrier_wait.unwrap_or(defaults.barrier_wait),
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

/// The partitions read at once, `partitions` or `default` when unset: more than zero, and no more
/// than an attempt reads at once.
fn read_at_once(partitions: Option<usize>, default: NonZeroUsize) -> Result<NonZeroUsize, Error> {
    let read = nonzero(partitions, default, NonZeroUsize::new).ok_or_else(|| {
        Error::config("partitions must be more than zero").with_code(codes::CONFIG_INVALID)
    })?;
    if read.get() > MAX_PLAN_PARTITIONS {
        return Err(Error::config(format!(
            "partitions must be at most {MAX_PLAN_PARTITIONS}, the most an attempt reads at once"
        ))
        .with_code(codes::CONFIG_INVALID));
    }
    Ok(read)
}

/// `value`, or `default` when unset; `None` when `value` is zero.
fn positive(value: Option<Duration>, default: Duration) -> Option<Duration> {
    Some(value.unwrap_or(default)).filter(|wait| !wait.is_zero())
}

/// `value` as a non-zero number, or `default` when unset; `None` when `value` is zero.
fn nonzero<T, N>(value: Option<T>, default: N, make: fn(T) -> Option<N>) -> Option<N> {
    value.map_or(Some(default), make)
}
