use super::Features;
use crate::rng::SplitMix64;

/// Whether a feature is on.
type Flag = fn(&Features) -> bool;

#[test]
fn every_feature_is_drawn_on_and_off_and_now_and_then_all_at_once() {
    let drawn: Vec<Features> = (0..400)
        .map(|seed| Features::draw(&mut SplitMix64::new(seed)))
        .collect();
    let flags: [(&str, Flag); 8] = [
        ("drift", |features| features.drift),
        ("encodings", |features| features.encodings),
        ("json", |features| features.json),
        ("normalize", |features| features.normalize),
        ("sliced", |features| features.sliced),
        ("faults", |features| features.faults),
        ("disruptions", |features| features.disruptions),
        ("narrow", |features| features.narrow),
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
