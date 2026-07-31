//! # rdlt-connector-postgres-v2 — the PostgreSQL connectors, second generation
//!
//! `source` (binary-COPY→Arrow extraction, reflection, cursor incremental,
//! CDC) and `destination` (binary-COPY writes, staging + transactional
//! commits) share three substrate modules so the two directions cannot drift:
//! `tls` holds the connection-security vocabulary, `session` owns the whole
//! path from a connection string to a prepared live connection, and `types`
//! is the one Postgres type rulebook — every wire decode, text parse, SQL
//! literal, and wire encode dispatches over the same closed [`types`] kind
//! set, so a new kind is a compiler-forced edit in every face.
//!
//! ONE SPELLING PER ITEM: this crate has NO crate-root re-exports — every
//! public item is reached through its module path (`source::…`,
//! `destination::…`, `tls::…`, `fixtures::…`), and that path is the canonical
//! spelling. Types do not repeat their module's noun: the policy is
//! [`tls::Policy`], the source connector is `source::Postgres`, the source
//! configuration is `source::Config`.
//!
//! Modules land bottom-up while the crate is under construction; the tree
//! below fills in as they do.

pub(crate) mod session;
pub mod tls;
pub(crate) mod types;

#[cfg(feature = "fixtures")]
pub mod fixtures;
#[cfg(feature = "source")]
pub mod source;

// Still to land:
//   mod destination;  (feature `destination`)
//   mod testsupport;  (doc-hidden test access across the test-binary boundary)
