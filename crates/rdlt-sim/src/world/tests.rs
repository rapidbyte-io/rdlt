use std::collections::BTreeSet;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{ConnectorErrorKind, IdentifierChars};

use super::{FaultPoint, World, capabilities};
use crate::rng::SplitMix64;
use crate::seed::{Seed, run};
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
fn destinations_differ_in_identifier_rules() {
    let drawn: Vec<_> = (0..300)
        .map(|seed| capabilities(&mut SplitMix64::new(seed), Features::ALL))
        .collect();
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

/// How many of `calls` calls at `point` hang, with faults `faulty`.
fn hung(world: &Arc<World>, point: FaultPoint, faulty: bool, calls: usize) -> usize {
    let world = Arc::clone(world);
    run(Seed::new(3), move |_| async move {
        world.set_faulty(faulty);
        let mut hung = 0;
        for _ in 0..calls {
            let call = tokio::time::timeout(Duration::from_millis(1), world.hang(point)).await;
            hung += usize::from(call.is_err());
        }
        hung
    })
}

#[test]
fn a_bounded_call_now_and_then_hangs_where_faults_are_on_and_a_read_never_does() {
    let registered = World::register("hangs", &mut SplitMix64::new(3));
    let world = Arc::clone(registered.world());
    let commits = hung(&world, FaultPoint::CommitBefore, true, 20_000);
    assert!(
        (10..=100).contains(&commits),
        "{commits} of 20000 commits hung"
    );
    assert_eq!(hung(&world, FaultPoint::Read, true, 20_000), 0);
    assert_eq!(hung(&world, FaultPoint::CommitBefore, false, 20_000), 0);
    world.without_hangs();
    assert_eq!(hung(&world, FaultPoint::CommitBefore, true, 20_000), 0);
    drop(registered);
}

#[test]
fn a_point_a_test_names_hangs_every_call_there_faults_on_or_off() {
    let registered = World::register("hang-at", &mut SplitMix64::new(3));
    let world = Arc::clone(registered.world());
    world.hang_at(FaultPoint::Write);
    assert_eq!(hung(&world, FaultPoint::Write, false, 10), 10);
    assert_eq!(hung(&world, FaultPoint::Flush, false, 10), 0);
    drop(registered);
}

#[test]
fn some_placements_stall_where_faults_are_on_and_none_elsewhere() {
    let registered = World::register("stalls", &mut SplitMix64::new(3));
    let world = Arc::clone(registered.world());
    world.set_faulty(true);
    let stalled = (0..2_000).filter(|_| world.stalls()).count();
    assert!(
        (40..=200).contains(&stalled),
        "{stalled} of 2000 placements stalled"
    );
    world.set_faulty(false);
    assert_eq!((0..2_000).filter(|_| world.stalls()).count(), 0);
    drop(registered);
}

#[test]
fn a_world_notes_the_largest_push_its_source_made() {
    let registered = World::register("largest-push", &mut SplitMix64::new(3));
    let world = registered.world();
    assert_eq!(world.largest_push(), 0);
    world.pushed(10);
    world.pushed(4);
    assert_eq!(world.largest_push(), 10);
}
