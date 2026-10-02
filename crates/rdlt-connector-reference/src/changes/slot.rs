//! Where a change source stands outside the engine, as a replication slot keeps it.

use std::sync::Arc;

use super::Position;
use crate::kept::{Kept, Registry};

/// One slot: each partition's acknowledged position.
pub(super) type Slot = Kept<Position>;

/// Slots by host and name, each for as long as a source holds it.
static SLOTS: Registry<Position> = Registry::new();

/// The slot `host` names `name`, which every change source of this process connected for that
/// host and naming it shares.
pub(super) fn named(host: Option<&str>, name: Option<&str>) -> Arc<Slot> {
    SLOTS.named(host, name)
}

/// The slot kept for `host` in the file at `path`, which every change source of this process
/// connected for that host and naming the file shares.
pub(super) fn at(host: Option<&str>, path: &std::path::Path) -> std::io::Result<Arc<Slot>> {
    SLOTS.at(host, path)
}
