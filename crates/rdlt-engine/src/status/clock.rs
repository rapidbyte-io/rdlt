//! A run's clock: when it began on the environment's monotonic and wall clocks, so every instant
//! a status names reads as a wall-clock time; and stamps, instants that fit an atomic.

use std::time::{Duration, Instant, SystemTime};

use crate::env::Env;

/// When a run began, on its environment's monotonic and wall clocks.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RunClock {
    began: Instant,
    wall: SystemTime,
}

/// The largest stamp: about 146 years of nanoseconds, so a stamp shifted two bits left fits a
/// `u64`.
const LATEST: u64 = (1 << 62) - 1;

impl RunClock {
    /// The clock of a run beginning now on `env`.
    pub(crate) fn new(env: &dyn Env) -> Self {
        Self {
            began: env.instant(),
            wall: env.now(),
        }
    }

    /// When the run began, on the monotonic clock.
    pub(crate) fn began(&self) -> Instant {
        self.began
    }

    /// `at` as a wall-clock time: the run's start, and the time since.
    pub(crate) fn wall(&self, at: Instant) -> SystemTime {
        self.wall + at.saturating_duration_since(self.began)
    }
}

/// `at` as a stamp from `origin`: the nanoseconds since, and one more, so no stamp is zero; at
/// most [`LATEST`].
pub(crate) fn stamp(origin: Instant, at: Instant) -> u64 {
    let since = at.saturating_duration_since(origin).as_nanos();
    u64::try_from(since).map_or(LATEST, |since| since.saturating_add(1).min(LATEST))
}

/// The instant of `stamp`, a stamp from `origin`.
pub(crate) fn unstamp(origin: Instant, stamp: u64) -> Instant {
    origin + Duration::from_nanos(stamp.saturating_sub(1))
}
