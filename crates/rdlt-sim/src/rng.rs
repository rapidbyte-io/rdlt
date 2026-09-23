//! A small seedable pseudo-random generator.

#[cfg(test)]
mod tests;

/// The `SplitMix64` generator: fast, and fully determined by its seed.
#[derive(Clone, Debug)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    /// Creates a generator whose sequence is determined by `seed`.
    pub const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// The next value in the sequence.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A value in `0..bound`; `bound` must be more than zero.
    pub fn below(&mut self, bound: u64) -> u64 {
        self.next_u64() % bound
    }

    /// `true` with probability `per_mille / 1000`.
    pub fn chance(&mut self, per_mille: u64) -> bool {
        self.below(1000) < per_mille
    }
}
