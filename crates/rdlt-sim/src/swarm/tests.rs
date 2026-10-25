use super::Features;
use crate::rng::SplitMix64;

/// Whether a feature is on.
type Flag = fn(&Features) -> bool;

#[test]
fn every_feature_is_drawn_on_and_off_and_now_and_then_all_at_once() {
    // Every feature at once is about one seed in 21 845: one in eight, by one in four for each of
    // the network, sources that forget, streaming and resetting, three in four for no fat values,
    // and one in two for each of a hostile host, churn and broken connectors.
    let drawn: Vec<Features> = (0..256_000)
        .map(|seed| Features::draw(&mut SplitMix64::new(seed)))
        .collect();
    let flags: [(&str, Flag); 22] = [
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
        ("process", |features| features.process),
        ("forgetting", |features| features.forgetting),
        ("streaming", |features| features.streaming),
        ("churn", |features| features.churn),
        ("reset", |features| features.reset),
        ("fat", |features| features.fat),
        ("hostile", |features| features.hostile),
        ("framing", |features| features.framing),
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

#[test]
fn features_that_exclude_or_need_one_another_never_clash() {
    let drawn: Vec<Features> = (0..256_000)
        .map(|seed| Features::draw(&mut SplitMix64::new(seed)))
        .collect();
    assert!(
        !drawn
            .iter()
            .any(|features| features.network && features.process),
        "a seed is placed one way"
    );
    assert!(
        !drawn
            .iter()
            .any(|features| features.churn && !features.streaming),
        "only a streaming seed churns"
    );
    assert!(
        !drawn
            .iter()
            .any(|features| features.framing && !features.hostile),
        "only a hostile seed meets broken connectors"
    );
}

#[test]
fn fat_values_change_nothing_else_a_workload_draws() {
    let (mut any, mut forgetting) = (false, false);
    for seed in 0..1000 {
        let mut rng = SplitMix64::new(seed);
        let features = Features::draw(&mut rng);
        forgetting |= features.fat && features.forgetting;
        let lean = Features {
            fat: false,
            ..features
        };
        let mut fat = crate::workload::Workload::generate(&mut rng.clone(), features);
        let plain = crate::workload::Workload::generate(&mut rng, lean);
        any |= fat.streams.iter().any(|stream| stream.fat > 0);
        for stream in &mut fat.streams {
            stream.fat = 0;
        }
        fat.features = lean;
        // Drawn floats include NaN, which is not equal to itself, so the workloads are compared
        // by what they print.
        assert_eq!(format!("{fat:?}"), format!("{plain:?}"), "seed {seed}");
    }
    assert!(any, "some stream has fat values");
    assert!(
        forgetting,
        "some seed has fat values and sources that forget"
    );
}

#[test]
fn sources_that_forget_are_drawn_apart_and_change_nothing_a_workload_draws_but_whether_they_forget()
{
    let mut forgot = 0;
    for seed in 0..1000 {
        let mut rng = SplitMix64::new(seed);
        let features = Features::draw(&mut rng);
        let remembering = Features {
            forgetting: false,
            ..features
        };
        let mut forgetting = crate::workload::Workload::generate(&mut rng.clone(), features);
        let plain = crate::workload::Workload::generate(&mut rng, remembering);
        for (index, stream) in forgetting.streams.iter_mut().enumerate() {
            let incremental = stream.read == rdlt_connector::ReadMode::Incremental;
            let forgets = features.forgetting && incremental && index % 2 == 0;
            assert_eq!(stream.replayable, !forgets, "seed {seed}: stream {index}");
            forgot += usize::from(forgets);
            stream.replayable = true;
        }
        forgetting.features = remembering;
        // Drawn floats include NaN, which is not equal to itself, so the workloads are compared
        // by what they print.
        assert_eq!(
            format!("{forgetting:?}"),
            format!("{plain:?}"),
            "seed {seed}"
        );
    }
    assert!(forgot > 0, "some stream forgets");
}

#[test]
fn churn_is_drawn_apart_and_changes_nothing_a_workload_draws() {
    let mut any = false;
    for seed in 0..1000 {
        let mut rng = SplitMix64::new(seed);
        let features = Features::draw(&mut rng);
        any |= features.churn;
        let still = Features {
            churn: false,
            ..features
        };
        let mut churned = crate::workload::Workload::generate(&mut rng.clone(), features);
        let plain = crate::workload::Workload::generate(&mut rng, still);
        churned.features = still;
        // Drawn floats include NaN, which is not equal to itself, so the workloads are compared
        // by what they print.
        assert_eq!(format!("{churned:?}"), format!("{plain:?}"), "seed {seed}");
    }
    assert!(any, "some seed churns");
}

#[test]
fn framing_is_drawn_apart_and_changes_nothing_a_workload_draws() {
    let mut any = false;
    for seed in 0..1000 {
        let mut rng = SplitMix64::new(seed);
        let features = Features::draw(&mut rng);
        any |= features.framing;
        let still = Features {
            framing: false,
            ..features
        };
        let mut framed = crate::workload::Workload::generate(&mut rng.clone(), features);
        let plain = crate::workload::Workload::generate(&mut rng, still);
        framed.features = still;
        // Drawn floats include NaN, which is not equal to itself, so the workloads are compared
        // by what they print.
        assert_eq!(format!("{framed:?}"), format!("{plain:?}"), "seed {seed}");
    }
    assert!(any, "some seed meets broken connectors");
}
