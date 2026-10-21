use std::time::Duration;

use super::Spawning;
use crate::canary::Canary;
use crate::network::{Serving, options};
use crate::rng::SplitMix64;
use crate::seed::{Seed, run};
use crate::world::World;

/// A check of `source` until one is answered with something other than a transient failure, at
/// most a hundred, 20 ms apart: the first may still meet a process that ended.
async fn answered(source: &dyn rdlt_connector::Source) -> rdlt_connector::Result<()> {
    for _ in 0..100 {
        match source.check().await {
            Err(error) if error.kind() == rdlt_connector::ConnectorErrorKind::Transient => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            answer => return answer,
        }
    }
    Err(rdlt_connector::ConnectorError::internal(
        "every check failed transiently",
    ))
}

#[test]
fn a_played_source_serves_and_is_played_again_once_it_is_killed() {
    let (first, again, played) = run(Seed::new(1), |_env| async move {
        let mut rng = SplitMix64::new(7);
        let name = "played-killed";
        let registered = World::register(name, &mut rng);
        let canary = Canary::of(name);
        let options = options(&mut rng, rdlt_wire::Limits::default());
        let spawning = Spawning::new(Seed::new(1), &options, &canary, Serving::Rows);
        let source = spawning.source(&canary.config()).await;
        let first = source.check().await;
        spawning.sides[0].kills.kill();
        // The killed process ends before the next call is made.
        tokio::time::sleep(Duration::from_millis(10)).await;
        let again = tokio::time::timeout(Duration::from_secs(60), answered(&*source)).await;
        let played = spawning.sides[0].script.played();
        drop(source);
        drop(registered);
        (first, again, played)
    });
    first.expect("the played source serves its check");
    again
        .expect("bounded")
        .expect("the source played again serves it");
    assert_eq!(played, 2);
}

#[test]
fn a_stalled_source_is_lost_and_the_one_played_next_serves() {
    let (lost, again, played) = run(Seed::new(2), |_env| async move {
        let mut rng = SplitMix64::new(9);
        let name = "played-stalled";
        let registered = World::register(name, &mut rng);
        let canary = Canary::of(name);
        let options = options(&mut rng, rdlt_wire::Limits::default());
        let spawning = Spawning::new(Seed::new(2), &options, &canary, Serving::Rows);
        let source = spawning.source(&canary.config()).await;
        spawning.sides[0].script.stall(true);
        // A stalled process answers nothing: the host loses it once its heartbeats go unanswered.
        let lost = tokio::time::timeout(Duration::from_secs(60), source.check()).await;
        // Held past twice the host's patience, so the host has lost it, whatever ended the check.
        let patience = options.heartbeat.saturating_mul(options.missed.get());
        tokio::time::sleep(patience.saturating_mul(2)).await;
        spawning.sides[0].script.stall(false);
        let again = tokio::time::timeout(Duration::from_secs(60), answered(&*source)).await;
        let played = spawning.sides[0].script.played();
        drop(source);
        drop(registered);
        (lost, again, played)
    });
    let lost = lost
        .expect("bounded")
        .expect_err("a stalled process answers nothing");
    assert_eq!(
        lost.kind(),
        rdlt_connector::ConnectorErrorKind::Transient,
        "{lost}"
    );
    again
        .expect("bounded")
        .expect("the source played next serves");
    assert_eq!(played, 2);
}

#[test]
fn a_played_process_ends_with_the_connector_placed_in_it() {
    let alive = run(Seed::new(3), |_env| async move {
        let mut rng = SplitMix64::new(11);
        let name = "played-ended";
        let registered = World::register(name, &mut rng);
        let canary = Canary::of(name);
        let options = options(&mut rng, rdlt_wire::Limits::default());
        let spawning = Spawning::new(Seed::new(3), &options, &canary, Serving::Rows);
        let source = spawning.source(&canary.config()).await;
        source.check().await.expect("it serves");
        drop(source);
        // Its process lingers 300 ms at most once stopped; its tasks end with it.
        tokio::time::sleep(Duration::from_secs(1)).await;
        let alive = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        drop(registered);
        alive
    });
    assert_eq!(
        alive, 0,
        "a played process outlived the connector placed in it"
    );
}
