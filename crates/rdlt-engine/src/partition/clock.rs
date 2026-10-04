//! The clock a load's batches are received by.

use std::time::SystemTime;

use crate::env::Env;

/// When an attempt's load started, and when its batches are received: the clock's reading, but
/// never before the start or an earlier one, so the versions a history stream begins as their
/// batches arrive follow each other.
#[derive(Debug)]
pub(crate) struct LoadClock {
    started: SystemTime,
    latest: parking_lot::Mutex<SystemTime>,
}

impl LoadClock {
    /// The clock of a load started at `started`.
    pub(crate) fn new(started: SystemTime) -> Self {
        Self {
            started,
            latest: parking_lot::Mutex::new(started),
        }
    }

    /// When the load started.
    pub(crate) fn started(&self) -> SystemTime {
        self.started
    }

    /// When a batch is received now, by `env`'s clock.
    pub(crate) fn received(&self, env: &dyn Env) -> SystemTime {
        let mut latest = self.latest.lock();
        *latest = (*latest).max(env.now());
        *latest
    }
}
