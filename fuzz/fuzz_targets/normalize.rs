//! Normalizing drawn Arrow batches never panics, and agrees with the reference normalizer: every
//! row's lineage names exactly the rows it comes from, no id names two rows, and the tables hold
//! the same rows, however the batches are cut into pieces, segments and sessions, and whether a
//! merge identifies the roots by a key.

#![forbid(unsafe_code)]
#![no_main]

use arrow_array::RecordBatch;
use libfuzzer_sys::fuzz_target;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};
use rdlt_engine::bench::{Cut, normalized_against_reference};
use rdlt_testkit::drawn::json;

fuzz_target!(|bytes: &[u8]| {
    // The input seeds the strategies' randomness, folded into a seed's 32 bytes: passed through
    // as it is, its zeros once it runs out would keep a sample of distinct items drawing forever.
    let mut seed = [0_u8; 32];
    for (at, byte) in bytes.iter().enumerate() {
        seed[at % seed.len()] ^= byte.rotate_left(u32::try_from(at / seed.len() % 8).unwrap_or(0));
    }
    let rng = TestRng::from_seed(RngAlgorithm::ChaCha, &seed);
    let mut runner = TestRunner::new_with_rng(Config::default(), rng);
    let cut = prop_oneof![Just(Cut::Piece), Just(Cut::Segment), Just(Cut::Session)];
    let drawing = (
        json::batches(),
        proptest::collection::vec(cut, 4),
        0_u8..4,
        any::<bool>(),
    );
    let Ok(tree) = drawing.new_tree(&mut runner) else {
        return;
    };
    let (drawn, cuts, max_depth, keyed) = tree.current();
    let batches: Vec<RecordBatch> = drawn.iter().map(json::batch).collect();
    let batches = if keyed {
        json::numbered(&batches, "k")
    } else {
        batches
    };
    let pieces: Vec<(RecordBatch, Cut)> = batches.into_iter().zip(cuts).collect();
    let key = keyed.then_some("k");
    if let Err(disagreed) = normalized_against_reference(&pieces, max_depth, key) {
        panic!("{disagreed}");
    }
});
