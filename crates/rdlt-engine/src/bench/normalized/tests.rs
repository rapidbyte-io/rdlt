use std::num::{NonZeroU64, NonZeroUsize};

use super::Normalized;
use crate::Cores;
use crate::bench::{Corpus, normalize, shred};

fn normalized(roots: u64, per_push: u64) -> Normalized {
    let cores = Cores::new(NonZeroUsize::new(2).unwrap());
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
fn a_flush_is_cut_into_the_units_the_engine_cuts_it_into() {
    for (roots, per_push) in [(2_000, 2_000), (20_000, 10_000), (100_000, 50_000)] {
        let bench = normalized(roots, per_push);
        let (fewest, most) = bench.units();
        let pushes = roots / per_push;
        // Each chunk is parsed once: its records hold no number the fast parse misreads.
        let parsed = bench.report().streams["events"].shred.parsed;
        assert!(
            (pushes * fewest as u64..=pushes * most as u64).contains(&parsed),
            "{roots} roots: {parsed} chunks parsed, {fewest} to {most} units a flush"
        );
    }
    let (_, few) = normalized(2_000, 2_000).units();
    let (many, _) = normalized(100_000, 50_000).units();
    assert!(many > few, "{many} units a flush of 50,000 rows, {few} of 2,000");
}

#[test]
fn the_rows_loaded_follow_from_the_roots_alone() {
    // Roots 0 to 6 hold 0, 1, 2, 3, 0, 1 and 2 items, each item two tags.
    assert_eq!(Normalized::loaded(7), 7 + 3 * 9);
    assert_eq!(Normalized::loaded(4), 4 + 3 * 6);
    assert_eq!(Normalized::loaded(0), 0);
}

#[test]
fn the_largest_push_is_the_length_of_the_longest() {
    let pushes = Corpus::Orders.rows(203, NonZeroU64::new(50).unwrap());
    let longest = pushes.iter().map(bytes::Bytes::len).max().unwrap();
    assert!(pushes.iter().any(|push| push.len() < longest));
    assert_eq!(normalized(203, 50).largest_push(), longest as u64);
}

#[test]
fn a_load_split_over_partitions_within_a_small_budget_loads_every_root_item_and_tag() {
    let cores = Cores::new(NonZeroUsize::new(2).unwrap());
    let four = NonZeroUsize::new(4).unwrap();
    let per_push = NonZeroU64::new(100).unwrap();
    let bench = Normalized::within(cores, 2_000, per_push, 256 << 20)
        .unwrap()
        .partitioned(four);
    assert_eq!(bench.partitions(), four);
    assert_eq!(normalized(2_000, 100).partitions(), NonZeroUsize::MIN);
    for _ in 0..2 {
        let report = bench.report();
        assert_eq!(report.rows, bench.rows());
        let written = report
            .counters
            .lanes
            .iter()
            .filter(|lane| !lane.writing.is_zero())
            .count();
        assert!(written > 1, "{written} lanes written");
    }
}

#[test]
fn the_partitioned_cells_are_four_and_sixteen_partitions_within_two_budgets() {
    let partitions: Vec<usize> = Normalized::PARTITIONS
        .iter()
        .map(|count| count.get())
        .collect();
    assert_eq!(partitions, [4, 16]);
    assert_eq!(Normalized::BUDGETS, [256 << 20, 1 << 30]);
    assert_eq!(
        (Normalized::SPLIT_ROOTS, Normalized::SPLIT_PUSH_ROWS.get()),
        (200_000, 10_000)
    );
}
