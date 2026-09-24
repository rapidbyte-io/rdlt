//! The in-memory source and destination.

mod destination;
mod merge;
mod source;

pub use destination::{MemoryDestination, MemoryDestinationConfig, published, schema};
pub use source::{MemorySource, MemorySourceConfig};
