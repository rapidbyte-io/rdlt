//! Where a change source stands outside the engine, as a replication slot keeps it.

use std::sync::Arc;

use super::Position;
use crate::kept::{Kept, Registry};

/// One slot: each partition's acknowledged position.
pub(super) type Slot = Kept<Position>;

/// Slots by name, for as long as the process runs.
static SLOTS: Registry<Position> = Registry::new();

/// The slot named `name`, which every change source of this process naming it shares.
pub(super) fn named(name: Option<&str>) -> Arc<Slot> {
    SLOTS.named(name)
}
