//! When the engine commits.

use std::num::NonZeroU64;
use std::time::Duration;

use crate::error::Error;

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

impl CommitPolicy {
    /// Every 10 seconds or 1 GiB, whichever comes first: a run that follows its source, or
    /// reads changes, commits what it reads soon after it arrives (spec §6.4).
    pub fn streaming() -> Self {
        Self {
            every: Some(Duration::from_secs(10)),
            ..Self::default()
        }
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
