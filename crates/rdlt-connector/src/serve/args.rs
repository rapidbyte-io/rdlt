//! A connector binary's arguments: the socket its host passed, or an address to listen on for
//! hosts over mutual TLS.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;

use rdlt_wire::tls::{Accepted, Hosts, Identity};

/// Why a connector binary could not serve.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub(crate) struct Failure(String);

impl From<String> for Failure {
    fn from(message: String) -> Self {
        Self(message)
    }
}

impl From<&str> for Failure {
    fn from(message: &str) -> Self {
        Self(message.to_owned())
    }
}

/// What a connector binary was asked to do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Args {
    /// Serve the socket the host passed at this file descriptor: `--rdlt-fd N`.
    Inherited {
        /// The file descriptor.
        fd: i32,
    },
    /// Listen for hosts, over mutual TLS: `--listen <address>`, with `--tls-cert <path>`,
    /// `--tls-key <path>`, `--tls-client-ca <path>`, a `--tls-allow-host <name>` for each host
    /// accepted, and `--tls-client-crl <path>` where revocation is checked.
    Listen(Listen),
}

/// Where a connector listens for hosts, and the TLS it requires of them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Listen {
    /// The address, whose port may be 0 for any free one.
    pub(crate) address: SocketAddr,
    /// The certificate and key the connector presents.
    pub(crate) identity: Identity,
    /// The hosts accepted: their CA bundle, their names, and their revocation lists.
    pub(crate) accepted: Accepted,
}

/// Every argument a connector binary takes.
const NAMES: [&str; 7] = [
    "rdlt-fd",
    "listen",
    "tls-cert",
    "tls-key",
    "tls-client-ca",
    HOST,
    "tls-client-crl",
];

/// The argument given once for each host accepted.
const HOST: &str = "tls-allow-host";

/// The binary's arguments: each `--name value` or `--name=value`, once, but for the hosts
/// accepted.
pub(crate) fn parse(mut args: impl Iterator<Item = String>) -> Result<Args, Failure> {
    let mut given = BTreeMap::new();
    let mut hosts = Vec::new();
    while let Some(arg) = args.next() {
        let Some((name, inline)) = arg
            .strip_prefix("--")
            .map(|flag| {
                flag.split_once('=')
                    .map_or((flag, None), |(name, value)| (name, Some(value)))
            })
            .filter(|(name, _)| NAMES.contains(name))
        else {
            return Err(format!("unknown argument `{arg}`").into());
        };
        let value = inline.map(str::to_owned).or_else(|| args.next());
        let value = value.ok_or_else(|| format!("`--{name}` needs a value"))?;
        if name == HOST {
            hosts.push(value);
        } else if given.insert(name.to_owned(), value).is_some() {
            return Err(format!("`--{name}` is given twice").into());
        }
    }
    let mut take = |name: &str| given.remove(name);
    let (fd, listen) = (take("rdlt-fd"), take("listen"));
    let tls = (take("tls-cert"), take("tls-key"), take("tls-client-ca"));
    let crl = take("tls-client-crl");
    match (fd, listen) {
        (Some(fd), None) => {
            if tls != (None, None, None) || crl.is_some() || !hosts.is_empty() {
                return Err("the TLS options go with `--listen`, not `--rdlt-fd`".into());
            }
            let fd = fd
                .parse()
                .map_err(|_| format!("`--rdlt-fd {fd}` is not a file descriptor"))?;
            Ok(Args::Inherited { fd })
        }
        (None, Some(address)) => {
            let address = address
                .parse()
                .map_err(|_| format!("`--listen {address}` is not an address and port"))?;
            let (Some(cert), Some(key), Some(client_ca)) = tls else {
                return Err(
                    "`--listen` needs `--tls-cert`, `--tls-key` and `--tls-client-ca`: \
                            hosts connect over mutual TLS only"
                        .into(),
                );
            };
            let hosts = Hosts::new(hosts).map_err(|_| {
                "`--listen` needs a `--tls-allow-host` naming each host it accepts: a connector \
                 accepts the hosts named to it, not every certificate of its CA"
            })?;
            Ok(Args::Listen(Listen {
                address,
                identity: Identity {
                    cert: cert.into(),
                    key: key.into(),
                },
                accepted: Accepted {
                    ca: client_ca.into(),
                    hosts,
                    crl: crl.map(PathBuf::from),
                },
            }))
        }
        (Some(_), Some(_)) => Err("`--rdlt-fd` and `--listen` exclude each other".into()),
        (None, None) => Err(
            "a connector binary serves its host's socket (`--rdlt-fd`), or listens for hosts \
             (`--listen`)"
                .into(),
        ),
    }
}
