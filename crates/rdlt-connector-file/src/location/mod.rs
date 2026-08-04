//! Where files live: the Local | S3 vocabulary and the shared IO
//! primitives both halves speak.

mod kind;
mod options;
mod s3;

pub(crate) use kind::classify_read_error;
pub(crate) use kind::{ByteReader, Location};
// `DocVersion` is not named here: nothing outside `location/` (kind.rs,
// s3.rs) needs to spell the type, only pass it through — `lease.rs`
// destructures `CreateDoc::Created(_)` and threads `read_doc_versioned`'s
// result straight into `replace_doc_if` without ever naming it. An
// unused `pub(crate) use` fails this crate's own lint gate (see
// `kind.rs`'s `is_stale_version` doc), so it stays un-re-exported until
// a real caller needs the name.
pub(crate) use kind::{CreateDoc, is_stale_version};
pub use options::{LocationOptions, S3Options};
