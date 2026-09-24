//! A source that reads JSON lines and Arrow IPC files, and a destination that writes them and
//! publishes each commit with a manifest.

mod destination;
mod format;
mod io;
mod manifest;
mod session;
mod source;
mod tables;

pub use destination::{FilesDestination, FilesDestinationConfig, published};
pub use format::FileFormat;
pub use session::{FilesSession, FilesWriter};
pub use source::{FilesSource, FilesSourceConfig};
