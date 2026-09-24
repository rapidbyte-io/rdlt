//! The in-memory source and destination.

mod destination;
mod source;

pub use destination::{MemoryDestination, MemoryDestinationConfig, published, schema};
pub use source::{MemorySource, MemorySourceConfig};
