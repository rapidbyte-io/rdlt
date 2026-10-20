use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use super::factories::{Asked, Counted};
use crate::network::Net;
use crate::rng::SplitMix64;
use crate::world::World;

#[test]
fn a_watch_records_a_hostile_connection_holding_more_than_it_may() {
    let registered = World::register("hostile-watch", &mut SplitMix64::new(3));
    let net = Arc::new(Net::new(true));
    let first = Counted::enter(&net, 9, 1, "hostile-watch");
    assert!(registered.world().violations().is_empty());
    let second = Counted::enter(&net, 9, 1, "hostile-watch");
    let other = Counted::enter(&net, 10, 1, "hostile-watch");
    let violations = registered.world().violations();
    assert_eq!(violations.len(), 1, "{violations:#?}");
    assert_eq!(
        *net.watch.configurations.lock(),
        BTreeMap::from([(9, 2), (10, 1)]),
        "only connection 9 had more than one configuration connecting"
    );
    drop((first, second, other));
    let again = Counted::enter(&net, 9, 1, "hostile-watch");
    assert_eq!(
        registered.world().violations().len(),
        1,
        "a count goes back as its entries drop"
    );
    drop((again, registered));
}

#[test]
fn a_connector_started_again_after_an_odd_count_of_crashes_names_itself_after_its_token() {
    let token = "rdlt-canary-token-oracle-18446744073709551615";
    let hostile = Arc::new(Net::new(true));
    hostile.watch.token.lock()[0] = Some(token.to_owned());
    let renamed = hostile.source_factory(1).spec().version.clone();
    assert_eq!(renamed, format!("0.0.0-{token}"));
    // As the host requires of a version: letters, digits, `.`, `+` and `-`, 64 bytes at most.
    assert!(
        renamed.len() <= 64
            && renamed
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || ".+-".contains(c))
    );
    assert_eq!(hostile.source_factory(2).spec().version, "0.0.0");
    assert_eq!(
        hostile.destination_factory(1).spec().version,
        "0.0.0",
        "no token was heard"
    );
    let plain = Arc::new(Net::new(false));
    plain.watch.token.lock()[0] = Some(token.to_owned());
    assert_eq!(plain.source_factory(1).spec().version, "0.0.0");
}

#[test]
fn a_hostile_configuration_says_what_it_asks() {
    let asked = Asked::opening(7, Duration::from_millis(150));
    let config: serde_json::Value = serde_json::from_str(&asked.config("w")).expect("JSON");
    let read = Asked::of(&config).expect("a hostile configuration");
    assert_eq!(
        (read.connection, format!("{read:?}")),
        (7, format!("{asked:?}"))
    );
    assert!(Asked::of(&serde_json::json!({ "world": "w" })).is_none());
}
