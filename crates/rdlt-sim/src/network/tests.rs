use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{
    ConnectorErrorKind, LoadId, OpenContext, Partition, PartitionId, PipelineId, ReadRequest,
    StreamName, partition_channel,
};

use super::{
    DRAIN, ENGINE, LEAST_PATIENCE, Networked, Peers, Serving, Side, options, patience,
    run_networked,
};
use crate::canary::Canary;
use crate::rng::SplitMix64;
use crate::seed::Seed;
use crate::world::World;

#[test]
fn connectors_on_the_simulated_network_are_placed_there_and_serve_the_engine() {
    let (source, destination) = run_networked(
        Seed::new(1),
        Serving::Rows,
        Peers::HONEST,
        |_env, net| async move {
            let mut rng = SplitMix64::new(7);
            let name = "network-placed";
            let registered = World::register(name, &mut rng);
            let config = Canary::of(name).config();
            let placing = Networked::new(
                net,
                &options(&mut rng, rdlt_wire::Limits::default()),
                &Canary::of(name),
            );
            let source = placing.source(&config, false).await.check().await;
            let destination = placing.destination(&config, false).await.check().await;
            drop(registered);
            (source, destination)
        },
    );
    source.expect("the source serves its check");
    destination.expect("the destination serves its check");
}

#[test]
fn a_disrupted_network_loses_connectors_and_the_healed_one_serves_them_again() {
    let (lost, served) = run_networked(
        Seed::new(1),
        Serving::Rows,
        Peers::HONEST,
        |_env, net| async move {
            let mut rng = SplitMix64::new(7);
            let name = "network-disrupted";
            let registered = World::register(name, &mut rng);
            let config = Canary::of(name).config();
            let placing = Networked::new(
                net,
                &options(&mut rng, rdlt_wire::Limits::default()),
                &Canary::of(name),
            );
            let source = placing.source(&config, false).await;
            // The source is checked again and again while the network is disrupted.
            let checks = async {
                let mut lost = 0;
                for _ in 0..200 {
                    if let Err(error) = source.check().await {
                        assert_eq!(error.kind(), ConnectorErrorKind::Transient, "{error}");
                        lost += 1;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                lost
            };
            let lost = placing.disrupting(SplitMix64::new(11), checks).await;
            let served = placing.source(&config, false).await.check().await;
            drop(registered);
            (lost, served)
        },
    );
    assert!(lost > 0, "no check failed while the network was disrupted");
    served.expect("the healed network serves the source again");
}

#[test]
fn a_call_the_network_held_lands_before_the_disrupted_work_is_over() {
    let (before, after) = run_networked(
        Seed::new(1),
        Serving::Rows,
        Peers::HONEST,
        |_env, net| async move {
            let mut rng = SplitMix64::new(7);
            let name = "network-held";
            let registered = World::register(name, &mut rng);
            let world = registered.world();
            let config = Canary::of(name).config();
            let placing = Networked::new(
                net,
                &options(&mut rng, rdlt_wire::Limits::default()),
                &Canary::of(name),
            );
            let destination = placing.destination(&config, false).await;
            let before = world.store.lock().digest();
            // A session is opened while the network holds what the engine sends, and the work ends,
            // leaving the open to go on, before the network heals.
            let opening = async {
                turmoil::hold(ENGINE, Side::Destination.host());
                let context = OpenContext {
                    pipeline: PipelineId::parse("held").expect("valid pipeline id"),
                    load_id: LoadId::from_parts(std::time::UNIX_EPOCH, 1),
                };
                tokio::spawn(async move { destination.open(&context).await.map(drop) });
                tokio::time::sleep(Duration::from_millis(2)).await;
            };
            placing.disrupting(SplitMix64::new(11), opening).await;
            let after = world.store.lock().digest();
            drop(registered);
            (before, after)
        },
    );
    assert_ne!(before, after, "the session the network held was not opened");
}

#[test]
fn a_stopped_connector_listens_again_while_a_read_its_host_holds_back_drains() {
    let served = run_networked(
        Seed::new(1),
        Serving::Rows,
        Peers::HONEST,
        |_env, net| async move {
            let mut rng = SplitMix64::new(7);
            let name = "network-drained";
            let registered = World::register(name, &mut rng);
            let config = Canary::of(name).config();
            let options = options(&mut rng, rdlt_wire::Limits::default());
            let placing = Networked::new(Arc::clone(&net), &options, &Canary::of(name));
            let source = placing.source(&config, false).await;
            // A read that follows its partition, of whose events the host takes one and no more, as
            // when the engine's budget holds it back: it keeps the stopped source draining for as
            // long as a drain may last, so only a source started beside the drain is placed.
            let (sink, mut feed) = partition_channel(NonZeroUsize::MIN);
            let partition = PartitionId::parse("p0").expect("a valid partition id");
            let request = ReadRequest::new(
                StreamName::new("s0").expect("a valid stream name"),
                Partition::new(partition).unbounded(),
                None,
            )
            .following(true);
            let reading = tokio::spawn(async move { source.read(request, sink).await });
            feed.recv().await.expect("the read sends");
            net.connectors.stop(Side::Source);
            // Long enough for its host to see it stopping.
            tokio::time::sleep(Duration::from_secs(1)).await;
            net.connectors.restart(Side::Source);
            let served = placing.source(&config, false).await.check().await;
            reading.abort();
            drop((feed, registered));
            served
        },
    );
    served.expect("the source started again serves its check");
}

#[test]
fn a_connector_whose_peer_stalls_is_lost_within_the_heartbeat_s_patience() {
    let (checked, waited, patience, violations, judged) = run_networked(
        Seed::new(1),
        Serving::Rows,
        Peers::HONEST,
        |_env, net| async move {
            let mut rng = SplitMix64::new(7);
            let name = "network-stalled";
            let registered = World::register(name, &mut rng);
            let config = Canary::of(name).config();
            let options = options(&mut rng, rdlt_wire::Limits::default());
            let patience = options.heartbeat.saturating_mul(options.missed.get());
            let placing = Networked::new(net, &options, &Canary::of(name));
            let source = placing.source(&config, true).await;
            let started = tokio::time::Instant::now();
            let checked = tokio::time::timeout(Duration::from_secs(3600), source.check())
                .await
                .expect("the check ends within its deadline");
            let waited = started.elapsed();
            drop(source);
            // Long enough for the engine's close to reach the peer.
            tokio::time::sleep(DRAIN).await;
            let violations = placing.stall_violations();
            let judged = placing.stalls_judged();
            drop(registered);
            (checked, waited, patience, violations, judged)
        },
    );
    let error = checked.expect_err("a stalled peer answers no check");
    assert_eq!(error.code(), Some(rdlt_host::CONNECTOR_LOST), "{error}");
    assert!(
        waited <= patience + DRAIN,
        "lost after {waited:?}, beyond the heartbeat's patience of {patience:?}"
    );
    assert!(violations.is_empty(), "{violations:?}");
    assert_eq!(judged, 1, "the stalled connection is judged once it closes");
}

#[test]
fn no_drawn_heartbeat_waits_less_than_the_least_patience_for_echoes() {
    let mut rng = SplitMix64::new(3);
    for _ in 0..10_000 {
        let options = options(&mut rng, rdlt_wire::Limits::default());
        assert!(patience(&options) >= LEAST_PATIENCE, "{options:?}");
        assert!(options.heartbeat < Duration::from_secs(1), "{options:?}");
        assert!((2..=5).contains(&options.missed.get()), "{options:?}");
    }
}

#[test]
fn a_change_world_s_source_is_placed_on_the_network_and_serves_the_engine() {
    let checked = run_networked(
        Seed::new(1),
        Serving::Changes,
        Peers::HONEST,
        |_env, net| async move {
            let (mut rng, mut apart) = (SplitMix64::new(7), SplitMix64::new(8));
            let name = "network-changes";
            let registered = World::register_changes(name, &mut rng, &mut apart);
            let config = Canary::of(name).config();
            let placing = Networked::new(
                net,
                &options(&mut rng, rdlt_wire::Limits::default()),
                &Canary::of(name),
            );
            // Networked checks the connector's id against the reference's, the change source's.
            let checked = placing.source(&config, false).await.check().await;
            drop(registered);
            checked
        },
    );
    checked.expect("the change source serves its check");
}
