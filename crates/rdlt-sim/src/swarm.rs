//! Swarm testing: each seed turns a random subset of the simulation's features on, so rare
//! combinations get seeds of their own rather than being diluted among every feature at once.

#[cfg(test)]
mod tests;

use crate::rng::SplitMix64;

/// Which of the simulation's features one seed exercises.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "each feature is on or off, independently"
)]
pub struct Features {
    /// Columns that come and go and change type.
    pub drift: bool,
    /// How many levels drift columns nest: 0 for scalars only.
    pub depth: u32,
    /// Encodings besides each type's plain one: dictionaries, run ends, views, maps and the rest.
    pub encodings: bool,
    /// Streams that push JSON rather than Arrow.
    pub json: bool,
    /// Streams that normalize nested values into child tables.
    pub normalize: bool,
    /// Batches that are slices of larger ones.
    pub sliced: bool,
    /// Injected connector faults and latency.
    pub faults: bool,
    /// Runs that crash, stop, or race a second run.
    pub disruptions: bool,
    /// Destinations that store only some types natively, and the rest as text.
    pub narrow: bool,
    /// Schema settings at every level, type hints, drift columns the source declares, and
    /// destinations that cannot add columns.
    pub settings: bool,
    /// Merge keys that change type, collide across partitions or span two columns.
    pub keys: bool,
}

impl Features {
    /// Every feature on: the simulation as it ran before swarm testing.
    pub const ALL: Self = Self {
        drift: true,
        depth: 3,
        encodings: true,
        json: true,
        normalize: true,
        sliced: true,
        faults: true,
        disruptions: true,
        narrow: true,
        settings: true,
        keys: true,
    };

    /// The features one seed exercises: every feature one time in eight, else each on or off by
    /// a coin, drift more often than not.
    pub fn draw(rng: &mut SplitMix64) -> Self {
        if rng.chance(125) {
            return Self::ALL;
        }
        Self {
            drift: rng.chance(800),
            depth: u32::try_from(rng.below(4)).unwrap_or(0),
            encodings: rng.chance(500),
            json: rng.chance(500),
            normalize: rng.chance(500),
            sliced: rng.chance(500),
            faults: rng.chance(500),
            disruptions: rng.chance(500),
            narrow: rng.chance(500),
            settings: rng.chance(500),
            keys: rng.chance(500),
        }
    }

    /// Whether the runs' reports count every commit that landed: no run is dropped mid-flight, and
    /// no commit's response is lost, which only a later attempt of the same run would credit.
    pub fn reports_complete(self) -> bool {
        !self.disruptions && !self.faults
    }
}
