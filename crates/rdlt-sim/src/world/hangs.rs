//! Connector calls that never return until their caller gives up on them, and placements on a
//! peer that stalls, drawn apart from every other fault, so the faults a seed met before fall as
//! they did.

use std::sync::atomic::Ordering;

use super::{FaultPoint, World};
use crate::rng::SplitMix64;

/// Calls in a thousand, at each point but a read, that hang while faults are on.
const HANG_PER_MILLE: u64 = 2;

/// Placements in a thousand, while faults are on, made on the peer that stalls.
const STALL_PER_MILLE: u64 = 50;

/// The draws of which calls hang and which placements stall.
#[derive(Debug)]
pub(super) struct Hangs {
    rng: SplitMix64,
    /// Whether calls hang as the draws say: not where the clock is real.
    drawn: bool,
    /// The point a test has every call at hang.
    at: Option<FaultPoint>,
}

impl Hangs {
    /// Draws from `seed`, with calls hanging as drawn.
    pub(super) fn new(seed: u64) -> Self {
        Self {
            rng: SplitMix64::new(seed),
            drawn: true,
            at: None,
        }
    }

    /// Whether the call at `point` hangs: every one at the point a test named, and otherwise,
    /// where `faulty`, two in a thousand of the calls their caller bounds, every call but a read.
    fn hangs(&mut self, point: FaultPoint, faulty: bool) -> bool {
        if self.at == Some(point) {
            return true;
        }
        faulty && self.drawn && point != FaultPoint::Read && self.rng.chance(HANG_PER_MILLE)
    }

    /// Whether the connector placed next goes to the peer that stalls: one placement in twenty,
    /// where `faulty`.
    fn stalls(&mut self, faulty: bool) -> bool {
        faulty && self.rng.chance(STALL_PER_MILLE)
    }
}

impl World {
    /// Waits for ever where the draw says the call at `point` hangs: a connector call that never
    /// returns until its caller gives up on it, as the engine does at its wait for every call but
    /// a read.
    pub(crate) async fn hang(&self, point: FaultPoint) {
        let faulty = self.faulty.load(Ordering::SeqCst);
        let hangs = self.hangs.lock().hangs(point, faulty);
        if hangs {
            std::future::pending::<()>().await;
        }
    }

    /// Whether the connector placed next goes to the peer that stalls, as the draw says where
    /// faults are on.
    pub(crate) fn stalls(&self) -> bool {
        let faulty = self.faulty.load(Ordering::SeqCst);
        self.hangs.lock().stalls(faulty)
    }

    /// Lets no call hang from now on: on the real clock a hang lasts as long as the engine's wait
    /// really is.
    pub(crate) fn without_hangs(&self) {
        self.hangs.lock().drawn = false;
    }

    /// Has every call at `point` hang from now on, faults on or off.
    #[cfg(test)]
    pub(crate) fn hang_at(&self, point: FaultPoint) {
        self.hangs.lock().at = Some(point);
    }
}
