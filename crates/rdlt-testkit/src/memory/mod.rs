//! In-memory reference connectors — certified by this crate's own
//! conformance suites, and the substrate for crash-injection tests.

mod destination;
mod source;

pub use destination::{MemoryDestination, Row};
pub use source::{MemoryBatch, MemorySource, MemoryStream, SinceLog};
