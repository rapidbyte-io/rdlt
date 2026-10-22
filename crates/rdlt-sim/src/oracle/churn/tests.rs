use std::collections::BTreeSet;

use super::{first, toggle};
use crate::rng::SplitMix64;
use crate::world::churning;

#[test]
fn churn_retires_adds_and_names_again_only_the_partitions_of_followed_streams() {
    let (registered, _) = churning("churn-draws");
    let world = registered.world();
    let mut rng = SplitMix64::new(5);
    let mut naming = first(world, &mut rng);
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
