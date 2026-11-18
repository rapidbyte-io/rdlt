use std::collections::BTreeSet;

use super::{Swapping, first, shift, swapping, toggle};
use crate::rng::SplitMix64;
use crate::seed::Seed;
use crate::swarm::Features;
use crate::workload::{SimStream, Workload};
use crate::world::{World, churning};

#[test]
fn churn_retires_adds_and_names_again_only_the_partitions_of_followed_streams() {
    let (registered, _) = churning("churn-draws");
    let world = registered.world();
    let mut rng = SplitMix64::new(5);
    let mut naming = first(world, &Swapping::new(), &mut rng);
    let followed: BTreeSet<usize> = world
        .workload
        .streams
        .iter()
        .enumerate()
        .filter(|(_, stream)| stream.follows)
        .map(|(index, _)| index)
        .collect();
    assert_eq!(naming.keys().copied().collect::<BTreeSet<_>>(), followed);
    let (mut retired, mut again) = (BTreeSet::new(), false);
    for _ in 0..200 {
        let before = naming.clone();
        toggle(world, &mut naming, &mut rng);
        for (stream, named) in &naming {
            let partitions = world.workload.streams[*stream].partitions.len();
            assert!(named.iter().all(|partition| *partition < partitions));
            retired.extend(
                before[stream]
                    .difference(named)
                    .map(|gone| (*stream, *gone)),
            );
            again |= named
                .difference(&before[stream])
                .any(|back| retired.contains(&(*stream, *back)));
        }
    }
    assert!(!retired.is_empty() && again, "retired {retired:?}");
    drop(registered);
}

/// Whether `stream` is followed, never ends and has more than one partition, so a source can
/// name fewer of them than all.
fn several(stream: &SimStream) -> bool {
    stream.follows && stream.unbounded && stream.partitions.len() > 1
}

#[test]
fn half_the_churning_worlds_swap_some_of_each_followed_unbounded_stream_of_several_partitions() {
    let (mut churning, mut swapped) = (0, 0);
    for value in 0..4_000 {
        let mut rng = SplitMix64::new(value);
        let features = Features::draw(&mut rng);
        let workload = Workload::generate(&mut rng, features);
        let drawn = swapping(Seed::new(value), &workload);
        if !(features.churn && workload.streams.iter().any(several)) {
            assert!(drawn.is_empty(), "seed {value} swaps {drawn:?}");
            continue;
        }
        churning += 1;
        swapped += usize::from(!drawn.is_empty());
        for (index, stream) in workload.streams.iter().enumerate() {
            let most = drawn.get(&index).copied();
            let swaps = several(stream) && !drawn.is_empty();
            assert_eq!(
                most.is_some_and(|most| (1..stream.partitions.len()).contains(&most)),
                swaps,
                "seed {value}: stream {index} of {} partitions names {most:?}",
                stream.partitions.len()
            );
        }
    }
    assert!(
        churning > 100 && (churning / 3..churning * 2 / 3).contains(&swapped),
        "{swapped} of {churning} churning worlds swap"
    );
}

#[test]
fn a_swapping_source_names_as_many_of_a_swapped_stream_and_swaps_each_in_and_out() {
    let (registered, stream) = (0..)
        .find_map(|value| {
            let registered = World::register("churn-swaps", &mut SplitMix64::new(value));
            let stream = registered
                .world()
                .workload
                .streams
                .iter()
                .position(several)?;
            Some((registered, stream))
        })
        .expect("some world follows an unbounded stream of several partitions");
    let world = registered.world();
    let partitions = world.workload.streams[stream].partitions.len();
    let most = partitions - 1;
    let swapping = Swapping::from([(stream, most)]);
    let mut rng = SplitMix64::new(5);
    let mut naming = first(world, &swapping, &mut rng);
    let others = naming.clone();
    let (mut retired, mut named) = (BTreeSet::new(), BTreeSet::new());
    for _ in 0..200 {
        assert_eq!(naming[&stream].len(), most, "{naming:?}");
        let before = naming[&stream].clone();
        shift(world, &swapping, &mut naming, &mut rng);
        retired.extend(before.difference(&naming[&stream]).copied());
        named.extend(naming[&stream].difference(&before).copied());
        for (index, partitions) in &naming {
            if *index != stream {
                assert_eq!(*partitions, others[index], "stream {index} changed");
            }
        }
    }
    let every: BTreeSet<usize> = (0..partitions).collect();
    assert_eq!((retired, named), (every.clone(), every));
    drop(registered);
}
