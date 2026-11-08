//! How a run commits, what it may use, and how it retries.

use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::read;
use super::units::{self, SizeText, SpanText};

/// When a run commits: whichever threshold it reaches first.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Commit {
    /// How long after the last commit the next is due.
    #[serde(
        default,
        with = "units::duration",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "Option<SpanText>")]
    pub every: Option<Duration>,
    /// Rows after which a commit is due.
    #[serde(
        default,
        deserialize_with = "read::optional_count",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(extend("maximum" = u64::MAX))]
    pub rows: Option<u64>,
    /// Bytes after which a commit is due.
    #[serde(
        default,
        with = "units::bytes",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "Option<SizeText>")]
    pub bytes: Option<u64>,
}

impl Commit {
    /// No threshold yet; a pipeline's commit needs at least one.
    pub fn new() -> Self {
        Self::default()
    }

    /// The policy, committing `every` after the last commit too.
    #[must_use]
    pub fn every(mut self, every: Duration) -> Self {
        self.every = Some(every);
        self
    }

    /// The policy, committing after `rows` rows too.
    #[must_use]
    pub fn rows(mut self, rows: u64) -> Self {
        self.rows = Some(rows);
        self
    }

    /// The policy, committing after `bytes` bytes too.
    #[must_use]
    pub fn bytes(mut self, bytes: u64) -> Self {
        self.bytes = Some(bytes);
        self
    }
}

/// What a run may use; the engine's default for each it does not say.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Resources {
    /// Bytes: the memory budget.
    #[serde(
        default,
        with = "units::bytes",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "Option<SizeText>")]
    pub memory: Option<u64>,
    /// Concurrent destination writers.
    #[serde(
        default,
        deserialize_with = "read::optional_count",
        skip_serializing_if = "Option::is_none"
    )]
    pub writers: Option<u16>,
    /// Partitions read at once.
    #[serde(
        default,
        deserialize_with = "read::optional_count",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(extend("maximum" = u64::MAX))]
    pub partitions: Option<usize>,
}

impl Resources {
    /// The engine's defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether nothing is set.
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// The resources, with a memory budget of `bytes`.
    #[must_use]
    pub fn memory(mut self, bytes: u64) -> Self {
        self.memory = Some(bytes);
        self
    }

    /// The resources, with `writers` concurrent destination writers.
    #[must_use]
    pub fn writers(mut self, writers: u16) -> Self {
        self.writers = Some(writers);
        self
    }

    /// The resources, reading `partitions` partitions at once.
    #[must_use]
    pub fn partitions(mut self, partitions: usize) -> Self {
        self.partitions = Some(partitions);
        self
    }
}

/// How a run retries a failed attempt; the engine's default for each it does not say.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Retry {
    /// Attempts, the first included, and tries of a commit.
    #[serde(
        default,
        deserialize_with = "read::optional_count",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(extend("maximum" = u32::MAX))]
    pub max_attempts: Option<u32>,
    /// The longest delay after the first failure.
    #[serde(
        default,
        with = "units::duration",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "Option<SpanText>")]
    pub initial: Option<Duration>,
    /// The longest delay after any failure.
    #[serde(
        default,
        with = "units::duration",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "Option<SpanText>")]
    pub max_delay: Option<Duration>,
}

impl Retry {
    /// The engine's defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// The policy, making at most `attempts` attempts.
    #[must_use]
    pub fn max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = Some(attempts);
        self
    }

    /// The policy, waiting at most `delay` after the first failure.
    #[must_use]
    pub fn initial(mut self, delay: Duration) -> Self {
        self.initial = Some(delay);
        self
    }

    /// The policy, waiting at most `delay` after any failure.
    #[must_use]
    pub fn max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = Some(delay);
        self
    }
}
