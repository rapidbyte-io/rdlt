use std::collections::BTreeSet;
use std::panic::{AssertUnwindSafe, catch_unwind};

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
    let world = World::register("granted", &mut SplitMix64::new(3));
    world.grant_add_column();
    assert!(world.capabilities().schema_changes.add_column);
    World::unregister("granted");
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
    let world = World::register(name, &mut SplitMix64::new(3));
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
    World::unregister(name);
    assert_eq!(
        kinds,
        BTreeSet::from(["panic", "permanent", "rate limited", "transient"])
    );
}
