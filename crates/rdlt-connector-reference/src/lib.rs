//! Reference connectors: small, complete implementations of the rdlt connector contract.
//!
//! - [`MemorySource`] reads rows given inline in its configuration.
//! - [`MemoryDestination`] keeps published tables and pipeline state in process memory.
//! - [`GeneratorSource`] produces seeded, partitioned Arrow data of any size.
//! - [`ChangesSource`] produces seeded change streams: a snapshot in phases, then changes.
//! - [`SqliteDestination`] loads into a SQLite database, through `sqlgen`.
//! - [`FilesSource`] reads JSON lines and Arrow IPC files.
//! - [`FilesDestination`] writes JSON lines or Arrow IPC files and publishes them with manifests.
//!
//! They serve as examples for connector authors and as the engine's test connectors.
//!
//! ```
//! use rdlt_connector::source_factory;
//! use rdlt_connector_reference::GeneratorSource;
//!
//! assert_eq!(source_factory::<GeneratorSource>().spec().id.as_str(), "io.rapidbyte.generator");
//! ```

#![forbid(unsafe_code)]

mod blocking;
pub mod changes;
mod columns;
pub mod files;
mod generator;
mod kept;
pub mod log;
mod memory;
mod merge;
pub mod sqlite;

pub use changes::{ChangedStream, ChangesConfig, ChangesSource};
pub use files::{
    FileFormat, FilesDestination, FilesDestinationConfig, FilesSource, FilesSourceConfig,
};
pub use generator::{GeneratedStream, GeneratorConfig, GeneratorSource};
pub use log::{LogConfig, LogSource, LoggedStream};
pub use memory::{
    MemoryDestination, MemoryDestinationConfig, MemorySource, MemorySourceConfig, published,
    schema, staged, tables,
};
pub use sqlite::{SqliteDestination, SqliteDestinationConfig};
