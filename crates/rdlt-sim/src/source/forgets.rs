//! A source that forgets what it was told: the rows before it are gone, and a read that serves
//! again rows a checkpoint sent, never told committed, is a path the simulation records.

use rdlt_connector::{ConnectorError, ConnectorErrorKind, Result};

use crate::reached::Reach;
use crate::world::World;

/// Admits a read of `key`'s partition, by stream and partition, from `from`: refused before what
/// its source was told, whose rows are gone, and recorded where it serves again rows a checkpoint
/// sent.
pub(super) fn admit(world: &World, key: &(String, String), from: u64) -> Result<()> {
    let forgotten = world.acknowledged.lock().get(key).copied();
    if forgotten.is_some_and(|forgotten| from < forgotten) {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Transient,
            "the rows before the acknowledged offset are gone",
        ));
    }
    let served = world.served.lock().get(key).copied();
    if served.is_some_and(|served| from < served) {
        world.reach(Reach::ServedAgain);
    }
    Ok(())
}

/// Notes `next`, a checkpoint the source sent of `key`'s partition.
pub(super) fn sent(world: &World, key: (String, String), next: u64) {
    let mut served = world.served.lock();
    let furthest = served.entry(key).or_default();
    *furthest = (*furthest).max(next);
}
