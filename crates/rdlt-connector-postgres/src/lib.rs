//! # rdlt-connector-postgres — the PostgreSQL connectors, one crate, two directions
//!
//! `source` (binary-COPY→Arrow extraction, reflection, cursor incremental)
//! and `dest` (binary-COPY writes, staging + transactional commits) live as
//! feature-gated modules sharing one home for TLS policy and Postgres type
//! knowledge — the two directions cannot drift (feature 006, amending the
//! 005 one-connector-one-crate convention; research R1).
//!
//! Facade paths are unchanged: `rdlt::postgres` re-exports [`dest`],
//! `rdlt::postgres_source` re-exports [`source`].

pub mod tls;
mod tls_verify;

#[cfg(feature = "dest")]
pub mod dest;
#[cfg(feature = "source")]
pub mod source;
