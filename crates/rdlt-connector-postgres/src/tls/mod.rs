//! TLS policy for BOTH Postgres connectors.
//!
//! One policy type, one connect path — the source and destination call the
//! same code, so their TLS behavior cannot drift. Modes carry libpq
//! semantics: `require` encrypts WITHOUT validating the certificate (the
//! ecosystem's long-standing meaning — documented loudly), `verify_full` is
//! the production recommendation.
//!
//! The module is split along its five seams (all crate-private): `policy` the
//! config-shaped vocabulary + agree-or-error resolution, `connstring` the libpq
//! parse gate, `rustls_config` the `ClientConfig` construction, `verify` the
//! quarantined weaker-mode certificate verifiers, and `connect` the live
//! connect path + failure classification.
//!
//! Two audiences share the re-export list below. `TlsPolicy`, `TlsMode`,
//! `TlsConfigError` and `PemSource` are embedder API — the YAML `tls:` block
//! and the facade's builder speak them (`rdlt::pipeline_spec` constructs
//! `TlsPolicy` directly). `connect`, `parse_conn` and their result types are
//! public ONLY because this crate's own integration suites (tls_matrix, the
//! conn-string pins) must reach them across the test-binary boundary — they
//! are not a supported API, same posture as the doc-hidden testhook seams.

mod connect;
mod connstring;
mod policy;
mod rustls_config;
mod verify;

pub use policy::{PemSource, TlsConfigError, TlsMode, TlsPolicy};

#[doc(hidden)]
pub use connect::{ConnectError, ConnectResult, TlsFailure, connect};
#[doc(hidden)]
pub use connstring::{ParsedConn, parse_conn};
