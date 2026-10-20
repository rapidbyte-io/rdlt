use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use rdlt_wire::Limits;
use rdlt_wire::limits::{Class, TRANSPORT_FRAME_BYTES};

use super::factories::{Asked, Counted};
use super::window::{HOLDERS, PREFIX, Raw, SETTLE, Write};
use super::{Attack, Attacker, PATIENCE};
use crate::canary::Canary;
use crate::network::{Net, Placing, Side, options, run_networked};
use crate::oracle::Window;
use crate::rng::SplitMix64;
use crate::seed::Seed;
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

#[test]
fn serving_meets_every_attack_of_a_hostile_host_as_the_protocol_says() {
    let attacks = [
        Attack::Order(Side::Source),
        Attack::Order(Side::Destination),
        Attack::Configurations(Side::Source),
        Attack::Configurations(Side::Destination),
        Attack::Abandoned(Side::Source),
        Attack::Abandoned(Side::Destination),
        Attack::Sessions,
        Attack::Oversized(Side::Destination),
        Attack::Window,
    ];
    let violations = run_networked(Seed::new(3), true, move |_env, net| async move {
        let mut rng = SplitMix64::new(7);
        let name = "hostile-attacks";
        let registered = World::register(name, &mut rng);
        let placing = Placing::new(
            net,
            &options(&mut rng, Limits::default()),
            &Canary::of(name),
        );
        for (index, attack) in attacks.into_iter().enumerate() {
            let attacker = Attacker {
                placing: &placing,
                world: registered.world(),
                name,
                strict: true,
            };
            attacker
                .attack(
                    attack,
                    SplitMix64::new(11 + u64::try_from(index).unwrap_or(0)),
                )
                .await;
        }
        let violations = registered.world().violations();
        drop(registered);
        violations
    });
    assert!(violations.is_empty(), "{violations:#?}");
}

/// Writes that wait for room while the heap is measured.
const WAITING: usize = 16;

/// Bytes: what one waiting write may hold beside the frame it read, its call's state on both ends
/// among it.
const BESIDE: usize = 620 * 1024;

#[test]
fn a_write_waiting_for_room_holds_the_frame_it_read_and_no_copy_of_it() {
    let frame = usize::try_from(TRANSPORT_FRAME_BYTES).expect("a frame's length fits");
    // With its prefix and a byte past it, each waiting message fills one transport frame.
    let message = frame - PREFIX - 1;
    let (held, unanswered) = run_networked(Seed::new(1), false, move |_env, net| async move {
        let mut rng = SplitMix64::new(7);
        let canary = Canary::of("hostile-heap");
        let placing = Placing::new(net, &options(&mut rng, Limits::default()), &canary);
        let wire = placing
            .wire(Side::Destination)
            .await
            .expect("the destination is reached");
        let mut raw = Raw::over(wire, Side::Destination.host())
            .await
            .expect("HTTP/2 opens");
        let largest = Limits::default().decoding(Class::Data) - PREFIX;
        let holders: Vec<Write> = (0..HOLDERS).map(|_| raw.write(largest, 0)).collect();
        tokio::time::sleep(SETTLE).await;
        let window = Window::open();
        let waiting: Vec<Write> = (0..WAITING)
            .map(|_| raw.write(message, message + 1))
            .collect();
        tokio::time::sleep(SETTLE * 5).await;
        let held = window.close();
        drop(holders);
        let mut unanswered = 0;
        for write in waiting {
            if !write.ends_within(PATIENCE).await {
                unanswered += 1;
            }
        }
        (held, unanswered)
    });
    assert_eq!(
        unanswered, 0,
        "writes waiting for room were not answered once it came back"
    );
    let most = u64::try_from(WAITING * (frame + BESIDE)).unwrap_or(u64::MAX);
    assert!(
        held <= most,
        "{WAITING} writes waiting for room held {held} bytes, beyond {most}: more than the frame \
         each read"
    );
}

#[test]
fn a_write_with_room_is_seen_answered_and_then_ends() {
    let (seen, ended) = run_networked(Seed::new(1), false, move |_env, net| async move {
        let mut rng = SplitMix64::new(7);
        let canary = Canary::of("hostile-answered");
        let placing = Placing::new(net, &options(&mut rng, Limits::default()), &canary);
        let wire = placing
            .wire(Side::Destination)
            .await
            .expect("the destination is reached");
        let mut raw = Raw::over(wire, Side::Destination.host())
            .await
            .expect("HTTP/2 opens");
        let write = raw.write(16, 17);
        tokio::time::sleep(SETTLE).await;
        let seen = write.answered();
        (seen, write.ends_within(PATIENCE).await)
    });
    assert!(seen, "a write with room was not answered");
    assert!(ended, "a write seen answered did not end");
}
