//! Where a served connector lives. One type, both transports of the
//! frozen proto (ADR 0001 D3): the private Unix domain socket a spawned
//! connector advertises on its handshake line, and the TCP address a
//! deployer configures for the network binding. `From` conversions let
//! call sites stay in their transport's own vocabulary — pass a
//! `&Path`/`PathBuf` for the socket, a `SocketAddr` for the wire — so
//! one `connect` serves both without a method per transport.
//!
//! THE TRUST MODELS DIFFER, and each variant carries its own: a socket
//! file's owner-only mode is the confidentiality boundary (the same
//! trust a locally spawned CLI plugin inherits); a TCP address is
//! plaintext by design at this layer, and wrapping it in TLS across any
//! boundary an attacker controls is the deployment's job.

use std::path::PathBuf;

/// The endpoint a [`crate::source::Remote`] or
/// [`crate::destination::Remote`] dials.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Endpoint {
    /// The Unix domain socket a spawned connector's handshake line
    /// advertised. Confidentiality rides the socket file's owner-only
    /// mode plus the operator trust any locally spawned child inherits.
    Socket(PathBuf),
    /// A TCP address from deployment configuration. Plaintext at this
    /// layer by design; TLS across an untrusted boundary is the
    /// provider layer's to wrap on both ends.
    Address(std::net::SocketAddr),
}

impl From<&PathBuf> for Endpoint {
    fn from(path: &PathBuf) -> Self {
        Endpoint::Socket(path.clone())
    }
}

impl From<PathBuf> for Endpoint {
    fn from(path: PathBuf) -> Self {
        Endpoint::Socket(path)
    }
}

impl From<&std::path::Path> for Endpoint {
    fn from(path: &std::path::Path) -> Self {
        Endpoint::Socket(path.to_path_buf())
    }
}

impl From<&str> for Endpoint {
    fn from(path: &str) -> Self {
        Endpoint::Socket(PathBuf::from(path))
    }
}

impl From<std::net::SocketAddr> for Endpoint {
    fn from(address: std::net::SocketAddr) -> Self {
        Endpoint::Address(address)
    }
}
