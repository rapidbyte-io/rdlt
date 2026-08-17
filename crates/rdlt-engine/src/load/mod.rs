//! The load stage: the item vocabulary flowing shred → load, the loader that
//! drives one `LoadSession` through ensure → write → commit, and the shared
//! apply/lowering seams recovery replays through.

pub(crate) mod apply;
#[cfg(test)]
mod ipc_fixture;
mod item;
mod loader;
mod lowering;

pub(crate) use item::LoadItem;
pub(crate) use loader::{Loader, Policies, Sink};
