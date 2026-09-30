//! Change streams in the simulation: seeded change workloads, and the source that serves them in
//! phases, a snapshot then its changes.

mod history;
mod source;
mod workload;

pub(crate) use history::Version;
pub(crate) use source::{CHANGES as CHANGES_PHASE, CHANGES_PARTITION};
pub use source::{Position, SimChangeSource, SimChangeSourceConfig};
pub use workload::{ChangeStream, ChangeWorkload, Event};
pub(crate) use workload::{Logged, Merged, ROUNDS};
