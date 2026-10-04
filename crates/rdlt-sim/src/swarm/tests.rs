use super::Features;
use crate::rng::SplitMix64;

/// Whether a feature is on.
type Flag = fn(&Features) -> bool;

#[test]
fn every_feature_is_drawn_on_and_off_and_now_and_then_all_at_once() {
    // Every feature at once is one seed in 4096: the network, the log, streaming and resetting
    // are drawn apart, one time in four each, and the log's object store one time in two,
    // beside every other feature's one time in eight.
    let drawn: Vec<Features> = (0..16000)
        .map(|seed| Features::draw(&mut SplitMix64::new(seed)))
        .collect();
    let flags: [(&str, Flag); 18] = [
        ("drift", |features| features.drift),
        ("encodings", |features| features.encodings),
        ("json", |features| features.json),
        ("normalize", |features| features.normalize),
        ("sliced", |features| features.sliced),
        ("faults", |features| features.faults),
        ("disruptions", |features| features.disruptions),
        ("narrow", |features| features.narrow),
        ("settings", |features| features.settings),
        ("keys", |features| features.keys),
        ("identifiers", |features| features.identifiers),
        ("shared", |features| features.shared),
        ("perturb", |features| features.perturb),
        ("network", |features| features.network),
        ("wal", |features| features.wal),
        ("objects", |features| features.objects),
        ("streaming", |features| features.streaming),
        ("reset", |features| features.reset),
    ];
    for (name, flag) in flags {
        assert!(drawn.iter().any(flag), "{name} is never on");
        assert!(!drawn.iter().all(flag), "{name} is never off");
    }
    for depth in 0..=3 {
        assert!(
            drawn.iter().any(|features| features.depth == depth),
            "depth {depth}"
        );
    }
    assert!(drawn.contains(&Features::ALL), "every feature at once");
    assert!(
        drawn
            .iter()
            .any(|features| features.reports_complete() && features.drift),
        "some seed drifts with complete reports, so its discards are counted"
    );
}
