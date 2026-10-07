//! A deterministic source of numbers, for generated corpora and for resampling.

#[cfg(test)]
mod tests;

/// The `SplitMix64` generator: the same numbers from the same seed on every run and platform.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Mix(u64);

impl Mix {
    /// A source whose numbers follow from `seed`.
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// The next number.
    pub fn draw(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// The next number reduced below `bound`.
    ///
    /// # Panics
    ///
    /// Panics where `bound` is zero.
    pub fn below(&mut self, bound: u64) -> u64 {
        self.draw() % bound
    }
}
