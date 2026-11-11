use std::collections::BTreeMap;

use rdlt_connector::ReadMode;
use rdlt_engine::WriteMode;

use super::{Combination, End, Ends, SWEPT, unreached};
use crate::reached::Reached;
use crate::rng::SplitMix64;
use crate::swarm::Features;
use crate::workload::{SimStream, Workload};

/// How many of the workloads of seeds `0..SWEPT` draw each combination, by its place in the list,
/// and any they draw that the list leaves out.
fn drawn() -> (BTreeMap<usize, usize>, Vec<Combination>) {
    let listed = Combination::listed();
    let (mut counts, mut unlisted) = (BTreeMap::new(), Vec::new());
    for seed in 0..u64::try_from(SWEPT).unwrap() {
        let mut rng = SplitMix64::new(seed);
        let features = Features::draw(&mut rng);
        let workload = Workload::generate(&mut rng, features);
        let combinations = workload.streams.iter().filter_map(SimStream::combination);
        for combination in combinations {
            match listed.iter().position(|listed| *listed == combination) {
                Some(index) => *counts.entry(index).or_default() += 1,
                None => unlisted.push(combination),
            }
        }
    }
    (counts, unlisted)
}

#[test]
fn the_list_holds_each_combination_a_stream_may_be_in_once() {
    let listed = Combination::listed();
    assert_eq!(listed.len(), 40);
    assert!(listed.len() <= 64, "a set of them is a 64-bit word");
    for (index, combination) in listed.iter().enumerate() {
        assert!(!listed[..index].contains(combination), "{combination}");
        let incremental = combination.read == ReadMode::Incremental || combination.changed;
        assert!(
            !(incremental && combination.write == WriteMode::Replace),
            "{combination}"
        );
        let full = combination.read == ReadMode::Full || combination.changed;
        assert!(
            !(full && combination.ends == Ends::Followed),
            "{combination}"
        );
    }
}

#[test]
fn the_workloads_of_a_sweep_draw_every_listed_combination_and_no_other() {
    let (counts, unlisted) = drawn();
    assert!(
        unlisted.is_empty(),
        "drawn, though never listed: {unlisted:?}"
    );
    // Each is drawn often enough that a sweep, whose checks may stop a seed short, reaches it.
    let rare: Vec<String> = Combination::listed()
        .iter()
        .enumerate()
        .map(|(index, combination)| (counts.get(&index).copied().unwrap_or(0), combination))
        .filter(|(count, _)| *count < 5)
        .map(|(count, combination)| format!("{count} drew {combination}"))
        .collect();
    assert!(rare.is_empty(), "of {SWEPT} seeds' workloads: {rare:#?}");
}

#[test]
fn a_combination_is_unreached_until_some_seed_reached_it() {
    let listed = Combination::listed();
    let mut reached = Reached::default();
    assert_eq!(unreached([reached]), listed);
    let first = Combination {
        read: ReadMode::Full,
        changed: true,
        ends: Ends::Unbounded,
        end: End::Rows,
        write: WriteMode::Merge,
    };
    reached.add_combination(first);
    assert!(reached.has_combination(first));
    let missed = unreached([Reached::default(), reached]);
    assert_eq!(missed.len(), listed.len() - 1);
    assert!(!missed.contains(&first));
    let everything = listed
        .iter()
        .fold(Reached::default(), |mut all, combination| {
            all.add_combination(*combination);
            all
        });
    assert!(unreached([everything]).is_empty());
}
