use std::collections::BTreeSet;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use rdlt_connector::{CommitKind, ConnectorErrorKind, IdentifierChars};

use super::{FaultPoint, World, capabilities};
use crate::rng::SplitMix64;
use crate::swarm::Features;

#[test]
fn some_destinations_cannot_add_columns_until_granted() {
    let drawn: Vec<_> = (0..300)
        .map(|seed| capabilities(&mut SplitMix64::new(seed), Features::ALL))
        .collect();
    assert!(drawn.iter().any(|caps| !caps.schema_changes.add_column));
    let unset = Features {
        settings: false,
        ..Features::ALL
    };
    for seed in 0..100 {
        let caps = capabilities(&mut SplitMix64::new(seed), unset);
        assert!(caps.schema_changes.add_column);
    }
    let registered = World::register("granted", &mut SplitMix64::new(3));
    let world = registered.world();
    world.grant_add_column();
    assert!(world.capabilities().schema_changes.add_column);
    drop(registered);
}

#[test]
fn destinations_differ_in_commits_and_identifier_rules() {
    let drawn: Vec<_> = (0..300)
        .map(|seed| capabilities(&mut SplitMix64::new(seed), Features::ALL))
        .collect();
    assert!(drawn.iter().any(|caps| caps.commit == CommitKind::Manifest));
    assert!(
        drawn
            .iter()
            .any(|caps| caps.commit == CommitKind::Transactional)
    );
    assert!(
        drawn
            .iter()
            .any(|caps| caps.identifiers.chars == IdentifierChars::Any)
    );
    assert!(
        drawn
            .iter()
            .any(|caps| caps.identifiers.reserved_table_prefixes.contains("s"))
    );
    assert!(
        drawn
            .iter()
            .any(|caps| caps.identifiers.reserved.contains("id"))
    );
    let plain = Features {
        identifiers: false,
        ..Features::ALL
    };
    for seed in 0..100 {
        let caps = capabilities(&mut SplitMix64::new(seed), plain);
        assert_eq!(caps.identifiers.chars, IdentifierChars::AsciiWord);
        assert!(caps.identifiers.reserved_table_prefixes.is_empty());
    }
}

#[test]
fn a_fault_is_transient_rate_limited_permanent_or_a_panic() {
    let name = "faults";
    let registered = World::register(name, &mut SplitMix64::new(3));
    let world = registered.world();
    world.set_faulty(true);
    let mut kinds = BTreeSet::new();
    for _ in 0..20_000 {
        let kind = match catch_unwind(AssertUnwindSafe(|| world.fault(FaultPoint::Writer))) {
            Err(_) => "panic",
            Ok(None) => continue,
            Ok(Some(error)) => match error.kind() {
                ConnectorErrorKind::Transient => "transient",
                ConnectorErrorKind::RateLimited => "rate limited",
                ConnectorErrorKind::Data => "permanent",
                other => panic!("an injected fault of kind {other:?}"),
            },
        };
        kinds.insert(kind);
    }
    drop(registered);
    assert_eq!(
        kinds,
        BTreeSet::from(["panic", "permanent", "rate limited", "transient"])
    );
}

#[test]
fn every_partition_s_cursor_together_stays_within_a_quarter_of_the_state_an_open_answers() {
    let limits = rdlt_wire::Limits {
        state_bytes: 1 << 20,
        ..rdlt_wire::Limits::default()
    };
    for partitions in [1_u64, 4, 64, 1_000] {
        let most = (0..300)
            .map(|seed| {
                super::Pressure::draw(&mut SplitMix64::new(seed), &limits, 4 << 20, partitions).pad
            })
            .max()
            .unwrap();
        let together = u64::try_from(most).unwrap() * partitions;
        assert!(together > 0, "{partitions} partitions press their cursors");
        assert!(
            together <= limits.state_bytes / 4,
            "{partitions}: {together}"
        );
    }
}

#[test]
fn a_world_leaves_the_registry_when_its_simulation_panics() {
    let name = "panicked";
    let unwound = catch_unwind(AssertUnwindSafe(|| {
        let _registered = World::register(name, &mut SplitMix64::new(3));
        assert!(World::named(name).is_some());
        panic!("the simulation fails");
    }));
    assert!(unwound.is_err());
    assert!(World::named(name).is_none());
}

#[test]
fn a_name_registered_twice_at_once_is_refused_and_keeps_its_first_world() {
    let name = "twice";
    let first = World::register(name, &mut SplitMix64::new(3));
    let again = catch_unwind(AssertUnwindSafe(|| {
        World::register(name, &mut SplitMix64::new(4))
    }));
    assert!(again.is_err());
    let named = World::named(name).expect("the first world stays registered");
    assert!(Arc::ptr_eq(&named, first.world()));
    drop(first);
    assert!(World::named(name).is_none());
}
