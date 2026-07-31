//! # rdlt-connector-postgres — the PostgreSQL connectors, one crate, two directions
//!
//! `source` (binary-COPY→Arrow extraction, reflection, cursor incremental)
//! and `dest` (binary-COPY writes, staging + transactional commits) live as
//! feature-gated modules sharing one home for TLS policy and Postgres type
//! knowledge — the two directions cannot drift.
//!
//! Facade paths are unchanged: `rdlt::postgres` re-exports [`dest`],
//! `rdlt::postgres_source` re-exports [`source`].
//!
//! ## Naming: `Postgres` vs `Pg` prefix
//!
//! One rule for the whole crate. A public type that a downstream crate names —
//! the entry points [`dest::Postgres`], [`source::PostgresSource`],
//! [`source::PostgresConfig`] — spells out `Postgres`, so its origin is
//! unambiguous at the use site. Everything else — internal helpers and impl
//! details that never need cross-crate disambiguation — takes the short `Pg`
//! prefix (`PgDialect`, `PgSession`, `PgTypeInfo`, `PgSourceError`,
//! `PgoutputError`). Shared config vocabulary is not Postgres-specific and
//! carries no prefix at all: it is re-exported under its bare `sqlcore` names
//! (`DestOptions`, `TableOptions`, …), the same spelling every SQL destination
//! uses.

#[cfg(feature = "fixtures")]
pub mod fixtures;
mod pgerror;
pub mod tls;
mod tls_verify;

#[cfg(feature = "dest")]
pub mod dest;
#[cfg(feature = "source")]
pub mod source;
