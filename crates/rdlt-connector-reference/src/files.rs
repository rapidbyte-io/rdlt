//! A source that reads JSON lines and Arrow IPC files, and a destination that writes them and
//! publishes each commit with a manifest.

mod destination;
mod format;
pub(crate) mod io;
mod manifest;
mod session;
mod source;
mod stored;
mod tables;
mod versions;

pub use destination::{FilesDestination, FilesDestinationConfig, published};
pub use format::FileFormat;
pub use session::{FilesSession, FilesWriter};
pub use source::{FilesSource, FilesSourceConfig};
