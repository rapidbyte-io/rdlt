//! Faults of the network and of the connectors on it: partitions both ways and one way, messages
//! held and released late, and connectors that crash or stop and start again.

use std::convert::Infallible;
use std::time::Duration;

use super::{BROKEN, ENGINE, Net, Side};
use crate::reached::Reach;
use crate::rng::SplitMix64;

/// Disrupts `net` as `rng` draws, until dropped, each fault lasting up to twice `patience`.
///
/// Dropping it leaves faults in place; [`Healing`] heals them.
pub(super) async fn disrupt(net: &Net, mut rng: SplitMix64, patience: Duration) -> Infallible {
    let longest = u64::try_from(patience.as_millis()).unwrap_or(u64::MAX / 2) * 2;
    loop {
        tokio::time::sleep(Duration::from_millis(rng.below(500))).await;
        let side = if rng.chance(500) {
            Side::Source
        } else {
            Side::Destination
        };
        let host = side.host();
        let lasting = Duration::from_millis(rng.below(longest + 1));
        match rng.below(5) {
            0 => {
                net.reached.lock().add(Reach::LinkCut);
                turmoil::partition(ENGINE, host);
                tokio::time::sleep(lasting).await;
                turmoil::repair(ENGINE, host);
            }
            1 => {
                let (from, to) = if rng.chance(500) {
                    (ENGINE, host)
                } else {
                    (host, ENGINE)
                };
                net.reached.lock().add(Reach::LinkCut);
                turmoil::partition_oneway(from, to);
                tokio::time::sleep(lasting).await;
                turmoil::repair_oneway(from, to);
            }
            2 => {
                net.reached.lock().add(Reach::LinkCut);
                turmoil::hold(ENGINE, host);
                tokio::time::sleep(lasting).await;
                turmoil::release(ENGINE, host);
            }
            3 => {
                net.connectors.crash(side);
                tokio::time::sleep(lasting).await;
                net.connectors.restart(side);
            }
            _ => {
                net.connectors.stop(side);
                net.reached.lock().add(Reach::ConnectorStopped);
                tokio::time::sleep(lasting).await;
                net.connectors.restart(side);
            }
        }
    }
}

/// Heals `net` when dropped: every link repaired and released, every connector started again.
pub(super) struct Healing<'a>(pub(super) &'a Net);

impl Drop for Healing<'_> {
    fn drop(&mut self) {
        for side in Side::BOTH {
            turmoil::repair(ENGINE, side.host());
            turmoil::release(ENGINE, side.host());
            self.0.connectors.restart(side);
        }
        if self.0.framing {
            turmoil::repair(ENGINE, BROKEN);
            turmoil::release(ENGINE, BROKEN);
        }
    }
}

/// Cuts and holds the engine's link to the broken connectors' host as `rng` draws, until
/// dropped, each fault lasting up to twice `patience`.
///
/// Dropping it leaves faults in place; [`Healing`] heals them.
pub(super) async fn cut(mut rng: SplitMix64, patience: Duration) -> Infallible {
    let longest = u64::try_from(patience.as_millis()).unwrap_or(u64::MAX / 2) * 2;
    loop {
        tokio::time::sleep(Duration::from_millis(rng.below(500))).await;
        let lasting = Duration::from_millis(rng.below(longest + 1));
        match rng.below(3) {
            0 => {
                turmoil::partition(ENGINE, BROKEN);
                tokio::time::sleep(lasting).await;
                turmoil::repair(ENGINE, BROKEN);
            }
            1 => {
                let (from, to) = if rng.chance(500) {
                    (ENGINE, BROKEN)
                } else {
                    (BROKEN, ENGINE)
                };
                turmoil::partition_oneway(from, to);
                tokio::time::sleep(lasting).await;
                turmoil::repair_oneway(from, to);
            }
            _ => {
                turmoil::hold(ENGINE, BROKEN);
                tokio::time::sleep(lasting).await;
                turmoil::release(ENGINE, BROKEN);
            }
        }
    }
}
