use std::num::{NonZeroU64, NonZeroUsize};

use super::Normalized;
use crate::Cores;
use crate::bench::{Corpus, normalize, shred};

fn normalized(roots: u64, per_push: u64) -> Normalized {
    let cores = Cores::from_count(NonZeroUsize::new(2).unwrap());
    Normalized::try_new(cores, roots, NonZeroU64::new(per_push).unwrap()).unwrap()
}

#[test]
fn a_run_loads_every_root_item_and_tag_on_every_run() {
    let bench = normalized(203, 50);
    let pushes = Corpus::Orders.rows(203, NonZeroU64::new(50).unwrap());
    let bytes: usize = pushes.iter().map(bytes::Bytes::len).sum();
    assert_eq!(bench.bytes(), bytes as u64);
    // The rows normalizing the corpus makes, counted apart from the engine.
    let shredded = shred(&pushes, 1 << 20).unwrap();
    let parts = shredded
        .iter()
        .flat_map(|batch| normalize(batch, 8, &[]).unwrap());
    let rows: usize = parts.map(|(_, batch)| batch.num_rows()).sum();
    assert_eq!(bench.rows(), rows as u64);
    for _ in 0..2 {
        assert_eq!(bench.run(), bench.rows());
    }
}

#[test]
fn a_flush_is_cut_into_one_unit_a_chunk_of_its_json() {
    assert_eq!(normalized(2_000, 2_000).units(), (1, 1));
    let (fewest, most) = normalized(20_000, 10_000).units();
    assert!(fewest >= 2 && most <= 3, "{fewest} to {most}");
    let (fewest, _) = normalized(100_000, 50_000).units();
    assert!(fewest >= 4, "{fewest}");
}

#[test]
fn the_rows_loaded_follow_from_the_roots_alone() {
    // Roots 0 to 6 hold 0, 1, 2, 3, 0, 1 and 2 items, each item two tags.
    assert_eq!(Normalized::loaded(7), 7 + 3 * 9);
    assert_eq!(Normalized::loaded(4), 4 + 3 * 6);
    assert_eq!(Normalized::loaded(0), 0);
}
