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

#[test]
fn connectors_that_break_the_protocol_are_refused_or_lost_in_time() {
    let checked = for_each_seed("framing", framing(8), Weight::One, check_exactly_once);
    assert!(
        checked
            .iter()
            .any(|checked| checked.reached.has(Reach::Framing)),
        "no seed met a broken connector on a healed network"
    );
}
