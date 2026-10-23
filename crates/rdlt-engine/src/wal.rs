//! The write-ahead log: what a load writes, sealed and is about to commit, durable before the
//! destination commits it, so a non-replayable source's data survives a crash.

mod counted;
pub(crate) mod frame;
pub(crate) mod load;
mod local;
#[cfg(any(test, feature = "bench"))]
pub(crate) mod memory;
mod positions;
pub(crate) mod scan;
mod store;
pub(crate) mod taken;
mod writer;

#[cfg(test)]
mod tests;

pub(crate) use counted::CountedStore;
pub(crate) use load::{LoadLog, Owner, Sealed};
pub use local::LocalWal;
pub(crate) use local::Refusal;
pub(crate) use positions::{Positions, Recorded};
pub use store::{Chunk, StagedChunk, WalStore};
