use std::collections::BTreeSet;

use super::{FEWEST, Faulty, MOST};
use crate::rng::SplitMix64;

#[test]
fn a_phase_has_four_to_sixteen_faulty_runs_each_seed_drawing_alike() {
    let drawn: BTreeSet<usize> = (0..2000).map(|seed| Faulty::draw(seed, 0).runs).collect();
    assert_eq!(drawn, (FEWEST..=MOST).collect());
    for seed in 0..100 {
        assert_eq!(Faulty::draw(seed, 1).runs, Faulty::draw(seed, 1).runs);
    }
    let phases: BTreeSet<usize> = (0..8).map(|phase| Faulty::draw(7, phase).runs).collect();
    assert!(phases.len() > 1, "every phase of a seed drew {phases:?}");
}

#[test]
fn the_first_four_runs_draw_from_the_simulation_s_generator_and_the_rest_apart() {
    let mut faults = Faulty::draw(7, 0);
    let (mut rng, mut kept) = (SplitMix64::new(11), SplitMix64::new(11));
    for run in 0..FEWEST {
        assert_eq!(
            faults.draws(run, &mut rng).next_u64(),
            kept.next_u64(),
            "run {run}"
        );
    }
    for run in FEWEST..MOST {
        faults.draws(run, &mut rng).next_u64();
    }
    assert_eq!(
        rng.next_u64(),
        kept.next_u64(),
        "a later run drew from the generator"
    );
    assert!(faults.faulty(FEWEST - 1));
    assert!(!faults.faulty(MOST));
    assert!(faults.faulty(faults.runs - 1));
    assert!(!faults.faulty(faults.runs));
}
