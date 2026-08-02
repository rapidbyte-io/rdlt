//! Where files live: the Local | S3 vocabulary and the shared IO
//! primitives both halves speak.

mod kind;
mod options;
mod s3;

pub(crate) use kind::classify_read_error;
pub use kind::{ByteReader, Location};
pub use options::{LocationOptions, S3Options};
