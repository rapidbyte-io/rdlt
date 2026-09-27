//! Faults of the network and of the connectors on it: partitions both ways and one way, messages
//! held and released late, and connectors that crash or stop and start again.

use std::convert::Infallible;
use std::time::Duration;

use super::{ENGINE, Net, Side};
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
                turmoil::partition_oneway(from, to);
                tokio::time::sleep(lasting).await;
                turmoil::repair_oneway(from, to);
            }
            2 => {
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
    }
}
