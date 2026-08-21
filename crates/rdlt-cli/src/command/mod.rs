//! The three subcommands: `run` (build, drive the feed, emit the
//! report), `check` (connectivity, discovery and plan checks, without
//! running), `schema` (a spawned connector's config JSON Schema).

pub(crate) mod check;
pub(crate) mod doctor;
pub(crate) mod reclaim;
pub(crate) mod run;
pub(crate) mod schema;
pub(crate) mod watch;
