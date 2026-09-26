use rdlt_connector::{CommitKind, IdentifierChars};

use super::{World, capabilities};
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
