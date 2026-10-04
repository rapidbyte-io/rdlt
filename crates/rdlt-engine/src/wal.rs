//! The write-ahead log (spec §15.6): what a load writes, sealed and is about to commit, durable
//! before the destination commits it, so a non-replayable source's data survives a crash.

pub(crate) mod frame;
pub(crate) mod load;
mod local;
#[cfg(any(test, feature = "bench"))]
pub(crate) mod memory;
#[cfg(feature = "object-store")]
mod object;
mod positions;
pub(crate) mod scan;
mod store;
pub(crate) mod taken;
mod writer;

#[cfg(test)]
mod tests;

pub(crate) use load::{LoadLog, Owner, Sealed};
pub use local::LocalWal;
pub(crate) use local::Refusal;
#[cfg(feature = "object-store")]
pub(crate) use object::code as object_code;
#[cfg(feature = "object-store")]
pub use object::{ObjectStoreOptions, ObjectStoreWal, StoreRefusal, WalObjects};
pub(crate) use positions::Positions;
pub use store::{Chunk, StagedChunk, WalStore};
