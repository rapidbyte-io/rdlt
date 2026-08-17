//! The three subcommands: `run` (build, drive the feed, emit the
//! report), `validate` (build through the real gates, run nothing),
//! `schema` (a spawned connector's config JSON Schema).

pub(crate) mod run;
pub(crate) mod schema;
pub(crate) mod validate;
