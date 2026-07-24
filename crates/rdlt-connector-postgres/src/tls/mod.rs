//! TLS policy for BOTH Postgres connectors.
//!
//! One policy type, one connect path — the source and destination call the
//! same code, so their TLS behavior cannot drift. Modes carry libpq
//! semantics: `require` encrypts WITHOUT validating the certificate (the
//! ecosystem's long-standing meaning — documented loudly), `verify_full` is
//! the production recommendation.
//!
//! The module is split along its four seams: [`policy`] the config-shaped
//! vocabulary + agree-or-error resolution, [`connstring`] the libpq
//! parse gate, [`rustls_config`] the `ClientConfig` construction, and
//! [`connect`] the live connect path + failure classification. The names the
//! rest of the crate (and rdlt-cli's `tls::TlsPolicy` path) depend on are
//! re-exported here so the public surface is unchanged.

mod connect;
mod connstring;
mod policy;
mod rustls_config;

pub use connect::{ConnectError, ConnectResult, TlsFailure, connect};
pub use connstring::{ParsedConn, parse_conn};
pub use policy::{PemSource, TlsConfigError, TlsMode, TlsPolicy};
