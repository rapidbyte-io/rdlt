//! The in-memory source and destination.

mod destination;
mod source;

pub use destination::{MemoryDestination, MemoryDestinationConfig, published};
pub use source::{MemorySource, MemorySourceConfig};
