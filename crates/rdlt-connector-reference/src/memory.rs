//! The in-memory source and destination.

mod destination;
mod source;

pub use destination::{
    MemoryDestination, MemoryDestinationConfig, forget, published, schema, staged, tables,
};
pub use source::{MemorySource, MemorySourceConfig};
