//! Change streams in the simulation: seeded change workloads, and the source that serves them in
//! phases, a snapshot then its changes.

mod source;
mod workload;

pub use source::{Position, SimChangeSource, SimChangeSourceConfig};
pub use workload::{ChangeStream, ChangeWorkload, Event};
pub(crate) use workload::{Logged, Merged, ROUNDS};
