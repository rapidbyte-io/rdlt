//! The listening connectors' lives: up, crashed, or stopping at their operator's word.

use std::time::Duration;

use parking_lot::Mutex;

use super::Side;

/// How often a connector's host looks at what its operator asked.
const POLL: Duration = Duration::from_millis(10);

/// What the operator asked of one connector.
#[derive(Clone, Copy, Debug, Default)]
struct Life {
    /// How often it crashed.
    crashes: u64,
    /// Whether it is down after a crash.
    down: bool,
    /// Whether it was asked to stop.
    stopping: bool,
}

/// The lives of the source and the destination.
#[derive(Debug, Default)]
pub(crate) struct Connectors {
    lives: Mutex<[Life; 2]>,
}

impl Connectors {
    fn life(&self, side: Side) -> Life {
        self.lives.lock()[index(side)]
    }

    fn change(&self, side: Side, change: impl FnOnce(&mut Life)) {
        change(&mut self.lives.lock()[index(side)]);
    }

    /// Crashes `side`'s connector: it drops every connection at once, and stays down until
    /// restarted.
    pub(crate) fn crash(&self, side: Side) {
        self.change(side, |life| {
            life.crashes += 1;
            life.down = true;
        });
    }

    /// Asks `side`'s connector to stop gracefully; it stays stopped until restarted.
    pub(crate) fn stop(&self, side: Side) {
        self.change(side, |life| life.stopping = true);
    }

    /// Starts `side`'s connector again after a crash or a stop.
    pub(crate) fn restart(&self, side: Side) {
        self.change(side, |life| {
            life.down = false;
            life.stopping = false;
        });
    }

    /// How often `side`'s connector crashed.
    pub(super) fn crashes(&self, side: Side) -> u64 {
        self.life(side).crashes
    }

    /// Ends once `side`'s connector may listen: neither down nor stopping.
    pub(super) async fn up(&self, side: Side) {
        while {
            let life = self.life(side);
            life.down || life.stopping
        } {
            tokio::time::sleep(POLL).await;
        }
    }

    /// Ends once `side`'s connector crashed more than `crashes` times.
    pub(super) async fn crashed(&self, side: Side, crashes: u64) {
        while self.crashes(side) <= crashes {
            tokio::time::sleep(POLL).await;
        }
    }

    /// Ends once `side`'s connector is asked to stop.
    pub(super) async fn stopping(&self, side: Side) {
        while !self.life(side).stopping {
            tokio::time::sleep(POLL).await;
        }
    }
}

fn index(side: Side) -> usize {
    match side {
        Side::Source => 0,
        Side::Destination => 1,
    }
}
