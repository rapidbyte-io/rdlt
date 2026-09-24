//! A destination that writes JSON lines and Arrow IPC files and publishes each commit with a
//! manifest.

mod destination;
mod format;
mod io;
mod manifest;
mod session;
mod tables;

pub use destination::{FilesDestination, FilesDestinationConfig, published};
pub use format::FileFormat;
pub use session::{FilesSession, FilesWriter};
