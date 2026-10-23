use rdlt_sim::{Features, Reach, Seed, SplitMix64, Weight, check_exactly_once, for_each_seed};

/// The first seeds the swarm draws whose connectors are on the network beside broken ones.
fn framing(count: usize) -> impl Iterator<Item = Seed> {
    (0..)
        .map(Seed::new)
        .filter(|seed| {
            let features = Features::draw(&mut SplitMix64::new(seed.value()));
            features.network && features.framing
        })
        .take(count)
}

/// Seeds that fail if a defect they once caught comes back.
#[rustfmt::skip]
const FOUND: [(u64, Reach); 2] = [
    // A heartbeat that waited for room for each ping never lost a connector that read none.
    (155, Reach::Framing), (576, Reach::Framing),
];

#[test]
fn connectors_that_break_the_protocol_are_refused_or_lost_in_time() {
    let first = framing(8).filter(|seed| !FOUND.iter().any(|(kept, _)| *kept == seed.value()));
    let seeds = FOUND
        .into_iter()
        .map(|(seed, _)| Seed::new(seed))
        .chain(first);
    let checked = for_each_seed("framing", seeds, Weight::One, |seed| {
        let checked = check_exactly_once(seed);
        crate::reaches(&FOUND, seed, checked.reached);
        checked
    });
    assert!(
        checked
            .iter()
            .any(|checked| checked.reached.has(Reach::Framing)),
        "no seed met a broken connector on a healed network"
    );
}
