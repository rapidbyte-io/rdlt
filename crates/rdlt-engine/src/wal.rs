//! The write-ahead log (spec §15.6): what a load writes, sealed and is about to commit, durable
//! before the destination commits it, so a non-replayable source's data survives a crash.

pub(crate) mod frame;
pub(crate) mod load;
#[cfg(any(test, feature = "bench"))]
pub(crate) mod memory;
mod positions;
pub(crate) mod scan;
mod store;
mod writer;

#[cfg(test)]
mod tests;

pub(crate) use load::{LoadLog, Sealed};
pub(crate) use positions::Positions;
pub use store::{Chunk, Claim, LocalWal, WalStore};
