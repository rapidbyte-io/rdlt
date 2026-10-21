//! How many of a phase's runs have faults injected, four to sixteen, and what those past the
//! fourth draw their disruptions from.

#[cfg(test)]
mod tests;

use crate::rng::SplitMix64;

/// The fewest runs of a phase with faults injected.
pub(super) const FEWEST: usize = 4;

/// The most runs of a phase with faults injected.
pub(super) const MOST: usize = 16;

/// What a phase's draw of its faulty runs mixes into the seed: "faulty" in ASCII.
const FAULTY: u64 = 0x6661_756c_7479;

/// A phase's faulty runs.
#[derive(Debug)]
pub(super) struct Faulty {
    /// How many there are.
    runs: usize,
    /// What those past the fourth draw their disruptions from.
    beyond: SplitMix64,
}

impl Faulty {
    /// The faulty runs of phase `phase` of a simulation whose seed, mixed with what its oracle
    /// mixes in, is `salted`, drawn apart from every other draw.
    pub(super) fn draw(salted: u64, phase: usize) -> Self {
        let phase = u64::try_from(phase).unwrap_or(u64::MAX);
        let mut rng = SplitMix64::new(salted ^ FAULTY ^ phase.rotate_left(48));
        let span = u64::try_from(MOST - FEWEST + 1).unwrap_or(1);
        let runs = FEWEST + usize::try_from(rng.below(span)).unwrap_or(0);
        Self { runs, beyond: rng }
    }

    /// Whether the phase's run `run`, counted from none, has faults injected.
    pub(super) fn faulty(&self, run: usize) -> bool {
        run < self.runs
    }

    /// What run `run` draws its disruption from: the first four from `rng`, as they always did,
    /// the rest from draws of their own, so `rng` draws alike however many there are.
    pub(super) fn draws<'a>(
        &'a mut self,
        run: usize,
        rng: &'a mut SplitMix64,
    ) -> &'a mut SplitMix64 {
        if run < FEWEST { rng } else { &mut self.beyond }
    }
}
