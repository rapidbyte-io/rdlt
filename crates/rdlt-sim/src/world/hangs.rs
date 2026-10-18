//! Connector calls that never return until their caller gives up on them, and placements on a
//! peer that stalls, drawn apart from every other fault, so the faults a seed met before fall as
//! they did.

use super::FaultPoint;
use crate::rng::SplitMix64;

/// Calls in a thousand, at each point but a read, that hang while faults are on.
const HANG_PER_MILLE: u64 = 2;

/// Placements in a thousand, while faults are on, made on the peer that stalls.
#[cfg_attr(not(test), expect(dead_code, reason = "only tests draw the stalls"))]
const STALL_PER_MILLE: u64 = 50;

/// The draws of which calls hang and which placements stall.
#[derive(Debug)]
pub(super) struct Hangs {
    rng: SplitMix64,
    /// Whether calls hang as the draws say: not where the clock is real.
    pub(super) drawn: bool,
    /// The point a test has every call at hang.
    pub(super) at: Option<FaultPoint>,
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
    pub(super) fn hangs(&mut self, point: FaultPoint, faulty: bool) -> bool {
        if self.at == Some(point) {
            return true;
        }
        faulty && self.drawn && point != FaultPoint::Read && self.rng.chance(HANG_PER_MILLE)
    }

    /// Whether the connector placed next goes to the peer that stalls: one placement in twenty,
    /// where `faulty`.
    #[cfg_attr(not(test), expect(dead_code, reason = "only tests draw the stalls"))]
    pub(super) fn stalls(&mut self, faulty: bool) -> bool {
        faulty && self.rng.chance(STALL_PER_MILLE)
    }
}
