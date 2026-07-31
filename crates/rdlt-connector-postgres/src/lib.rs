//! # rdlt-connector-postgres — the PostgreSQL connectors, one crate, two directions
//!
//! `source` (binary-COPY→Arrow extraction, reflection, cursor incremental)
//! and `dest` (binary-COPY writes, staging + transactional commits) live as
//! feature-gated modules sharing one home for TLS policy and Postgres type
//! knowledge — the two directions cannot drift. The facade re-exports the
//! whole crate as `rdlt::connector::postgres`.
//!
//! ONE SPELLING PER ITEM: this crate has NO crate-root re-exports — every
//! public item is reached through its module path (`source::…`, `dest::…`,
//! `tls::…`, `fixtures::…`), and that path is the canonical spelling.
//!
//! The primary workflow starts from configuration, and everything up to the
//! database round-trip runs anywhere — parse the pipeline YAML, get back
//! either a validated source or the exact configuration mistake:
//!
//! ```
//! use rdlt_connector_postgres::source::{PostgresConfig, PostgresSource};
//!
//! let config = PostgresConfig::from_yaml(
//!     r#"
//! conn: "postgresql://app@db.internal:5432/app"
//! tables:
//!   - name: orders
//!     cursor:
//!       column: updated_at
//! "#,
//! )
//! .expect("a listed table with a cursor column validates");
//! let _source = PostgresSource::new(config);
//!
//! // Validation is the same gate for every entry point: a conn string whose
//! // sslmode contradicts the tls block is refused at parse time, not at
//! // connect time.
//! let err = PostgresConfig::from_yaml(
//!     "conn: \"postgresql://u:p@localhost/db?sslmode=require\"\ntls:\n  mode: disable\n",
//! )
//! .unwrap_err();
//! assert!(err.to_string().contains("contradicts"));
//! ```
//!
//! Connecting and moving rows needs a database; the container-backed
//! integration suites under `tests/` are the executable reference for that
//! half.
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
//! (`DestinationOptions`, `TableOptions`, …), the same spelling every SQL destination
//! uses.

mod driver_error;
#[cfg(feature = "fixtures")]
pub mod fixtures;
pub mod tls;

#[cfg(feature = "dest")]
pub mod dest;
#[cfg(feature = "source")]
pub mod source;
