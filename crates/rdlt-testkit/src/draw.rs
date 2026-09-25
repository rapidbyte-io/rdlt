//! Values drawn from a seed instead of by a property test's runner: the simulation draws from the
//! strategies property tests shrink, each draw a pure function of its seed.

#[cfg(test)]
mod tests;

use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};

/// The value `strategy` draws from `seed`.
pub fn draw<S: Strategy>(strategy: &S, seed: u64) -> S::Value {
    let mut bytes = [0_u8; 32];
    let mut state = seed;
    for chunk in bytes.chunks_mut(8) {
        state = mix(state);
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    let rng = TestRng::from_seed(RngAlgorithm::ChaCha, &bytes);
    let mut runner = TestRunner::new_with_rng(Config::default(), rng);
    strategy
        .new_tree(&mut runner)
        .expect("the strategies drawn from reject few values")
        .current()
}

/// The `SplitMix64` step of `state`.
pub fn mix(state: u64) -> u64 {
    let mut z = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}
