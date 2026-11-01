use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::limits::ListenLimits;
use rdlt_connector::wire::{INVALID_MESSAGE, MALFORMED_FRAME};
use rdlt_connector::{ConnectorError, ConnectorErrorKind, ConnectorId};
use rdlt_host::{CONNECTOR_LOST, ConnectorRef};
use rdlt_wire::Limits;

use super::attack::{Breaker, Ended, attacking, bound, judge};
use super::{Expect, ID, catalogue, port};
use crate::canary::Canary;
use crate::network::faults::cut;
use crate::network::{
    BROKEN, ENGINE, Net, Networked, Peers, Serving, options, patience, run_networked,
};
use crate::reached::Reach;
use crate::rng::SplitMix64;
use crate::seed::Seed;
use crate::world::World;

/// A network with the broken connectors' host and nothing hostile beside it.
const BROKEN_ONLY: Peers = Peers {
    hostile: false,
    framing: true,
};

#[test]
fn every_broken_connector_listens_on_its_port_over_mutual_tls() {
    let dialed = run_networked(
        Seed::new(5),
        Serving::Rows,
        BROKEN_ONLY,
        |_env, net| async move {
            let mut rng = SplitMix64::new(7);
            let placing = Networked::new(
                net,
                &options(&mut rng, Limits::default()),
                &Canary::of("broken-ports"),
            );
            let id = ConnectorId::parse(ID).expect("a valid id");
            let mut dialed = Vec::new();
            for index in 0..catalogue().len() {
                let endpoint = format!("grpcs://{BROKEN}:{}", port(index));
                let reference = ConnectorRef::new(id.clone()).endpoint(endpoint);
                dialed.push(
                    placing
                        .remote
                        .wire(&reference)
                        .await
                        .map(drop)
                        .map_err(|error| error.to_string()),
                );
            }
            dialed
        },
    );
    let failed: Vec<_> = dialed
        .iter()
        .enumerate()
        .filter(|(_, dialed)| dialed.is_err())
        .collect();
    assert!(failed.is_empty(), "{failed:#?}");
    assert_eq!(dialed.len(), 20);
}

// The host accepts a connection only after the attack dials it, so a cap longer than every
// attack's bound ends no connection whose call is still being judged.
#[test]
fn every_attack_ends_before_the_broken_host_caps_its_connection() {
    let cap = ListenLimits::default().send;
    let longest = (0..10_000)
        .map(|seed| {
            let mut rng = SplitMix64::new(seed);
            let connect = options(&mut rng, Limits::default()).deadlines.connect;
            bound(&attacking(&mut rng, connect, seed % 2 == 0))
        })
        .max()
        .unwrap_or_default();
    assert!(
        longest < cap,
        "an attack may take {longest:?}, past the host's cap of {cap:?}"
    );
}

// A connection's dial, handshake and configuration may each take the whole connect deadline
// before a disrupted network ends it.
#[test]
fn an_attack_outlasts_each_setup_step_taking_its_whole_connect_deadline() {
    let mut rng = SplitMix64::new(3);
    let options = attacking(&mut rng, Duration::from_secs(3), false);
    let setup = options.deadlines.connect.saturating_mul(3);
    let after = patience(&options).saturating_mul(4) + options.deadlines.write_ack;
    assert!(
        bound(&options) > setup + after,
        "{:?} leaves no time past a setup of {setup:?} and {after:?} after it",
        bound(&options)
    );
}

/// An error coded `code`.
fn refused(code: &'static str) -> Ended {
    Ended::Refused(ConnectorError::new(ConnectorErrorKind::Internal, "refused").with_code(code))
}

#[test]
fn a_call_met_otherwise_than_its_fault_says_is_a_finding_and_a_lost_one_only_when_healed() {
    let invalid = Expect::Code(INVALID_MESSAGE);
    assert_eq!(judge(invalid, &refused(INVALID_MESSAGE), true), None);
    assert!(judge(invalid, &refused(MALFORMED_FRAME), true).is_some());
    assert!(judge(invalid, &refused(MALFORMED_FRAME), false).is_some());
    assert!(judge(invalid, &refused(CONNECTOR_LOST), true).is_some());
    assert_eq!(judge(invalid, &refused(CONNECTOR_LOST), false), None);
    assert!(judge(invalid, &Ended::Answered, false).is_some());
    assert!(judge(invalid, &Ended::Outlasted(Duration::from_secs(9)), false).is_some());
    assert!(judge(invalid, &Ended::Undialed("cut".to_owned()), true).is_some());
    assert_eq!(
        judge(invalid, &Ended::Undialed("cut".to_owned()), false),
        None
    );
    assert_eq!(judge(Expect::Answered, &Ended::Answered, true), None);
    assert!(
        judge(
            Expect::Answered,
            &Ended::Wrong("answered []".to_owned()),
            false
        )
        .is_some()
    );
    let large = ConnectorError::new(ConnectorErrorKind::Data, "the catalog is too large");
    assert_eq!(judge(Expect::TooLarge, &Ended::Refused(large), true), None);
}

/// The tasks the connectors' hosts on `net` still run, once they run none or a minute passed.
async fn left(net: &Net) -> usize {
    let alive = || -> usize {
        net.served
            .lock()
            .iter()
            .map(|host| host.metrics().num_alive_tasks())
            .sum()
    };
    for _ in 0..60_000 {
        if alive() == 0 {
            return 0;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    alive()
}

/// Attacks every broken connector in turn, judged in full where `strict`, its link cut and held
/// meanwhile where not: what the world found, whether it reached [`Reach::Framing`], and the
/// tasks the connectors' hosts ran a minute on.
fn attacked(strict: bool) -> (Vec<String>, bool, usize, usize) {
    run_networked(
        Seed::new(5),
        Serving::Rows,
        BROKEN_ONLY,
        move |_env, net| async move {
            let mut rng = SplitMix64::new(7);
            let name = if strict {
                "broken-healed"
            } else {
                "broken-cut"
            };
            let registered = World::register(name, &mut rng);
            let options = options(&mut rng, Limits::default());
            let placing = Networked::new(Arc::clone(&net), &options, &Canary::of(name));
            let attacks = async {
                let mut disrupted = 0;
                for index in 0..catalogue().len() {
                    let breaker = Breaker {
                        placing: &placing,
                        world: registered.world(),
                        strict,
                    };
                    let rng = SplitMix64::new(11 + u64::try_from(index).unwrap_or(0));
                    disrupted += usize::from(breaker.attack(index, rng).await);
                }
                disrupted
            };
            let disrupted = if strict {
                attacks.await
            } else {
                let disrupted = tokio::select! {
                    biased;
                    disrupted = attacks => disrupted,
                    never = cut(SplitMix64::new(13), patience(&options)) => match never {},
                };
                turmoil::repair(ENGINE, BROKEN);
                turmoil::release(ENGINE, BROKEN);
                disrupted
            };
            let tasks = left(&net).await;
            let world = registered.world();
            let found = (
                world.violations(),
                world.reached().has(Reach::Framing),
                tasks,
                disrupted,
            );
            drop(registered);
            found
        },
    )
}

#[test]
fn every_broken_connector_is_met_as_its_fault_says_on_a_healed_network() {
    let (violations, reached, tasks, disrupted) = attacked(true);
    assert!(violations.is_empty(), "{violations:#?}");
    assert!(reached, "a healed attack reaches its path");
    assert_eq!(disrupted, 0, "attacks a healed network disrupted");
    assert_eq!(tasks, 0, "tasks the connectors' hosts still run");
}

#[test]
fn every_broken_connector_behind_a_cut_link_is_met_or_lost_within_its_bound() {
    let (violations, _, tasks, disrupted) = attacked(false);
    assert!(violations.is_empty(), "{violations:#?}");
    assert_eq!(tasks, 0, "tasks the connectors' hosts still run");
    assert!(disrupted > 0, "the cut disrupted no attack");
}
