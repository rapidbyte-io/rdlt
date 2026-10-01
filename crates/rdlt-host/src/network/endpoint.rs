//! A connector's endpoint: `grpcs://host:port`, and nothing else.

#[cfg(test)]
mod tests;

use std::fmt;

use rustls::pki_types::ServerName;

/// Where a connector listens: a host name or IP address, and a port.
///
/// It is shown as `host:port`, an IPv6 address in brackets: what errors name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    host: String,
    port: u16,
}

/// What is wrong with an endpoint.
///
/// None of these repeats the endpoint, or any part of it: what makes an endpoint wrong may be a
/// credential written into it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum EndpointError {
    /// It does not begin with `grpcs://`.
    #[error("the endpoint is not a `grpcs://` one")]
    Scheme,
    /// It has a part before an `@`: a user name, or a password.
    #[error("the endpoint carries credentials, which a host presents in its certificate")]
    Credentials,
    /// It has a path.
    #[error("the endpoint has a path")]
    Path,
    /// It has a query.
    #[error("the endpoint has a query")]
    Query,
    /// It has a fragment.
    #[error("the endpoint has a fragment")]
    Fragment,
    /// It has no port, or one that is not a number from 1 to 65535 written in digits alone.
    #[error("the endpoint has no valid port")]
    Port,
    /// Its host is empty, or is neither a DNS name nor an IP address, an IPv6 one in brackets.
    #[error("the endpoint's host is neither a DNS name nor an IP address, IPv6 in brackets")]
    Host,
}

impl Endpoint {
    /// The endpoint `grpcs://host:port`; anything else, credentials, a path, a query or a
    /// fragment included, is refused.
    ///
    /// # Errors
    ///
    /// The [`EndpointError`] saying what is wrong with `endpoint`, without repeating it.
    pub fn parse(endpoint: &str) -> Result<Self, EndpointError> {
        let rest = endpoint
            .strip_prefix("grpcs://")
            .ok_or(EndpointError::Scheme)?;
        // As a URL reads: the fragment ends it, the query precedes that, the path that.
        if rest.contains('#') {
            return Err(EndpointError::Fragment);
        }
        if rest.contains('?') {
            return Err(EndpointError::Query);
        }
        let authority = rest.strip_suffix('/').unwrap_or(rest);
        if authority.contains('/') {
            return Err(EndpointError::Path);
        }
        if authority.contains('@') {
            return Err(EndpointError::Credentials);
        }
        let (host, port) = authority.rsplit_once(':').ok_or(EndpointError::Port)?;
        // An IPv6 address is in brackets, and nothing else is.
        let host = match host.strip_prefix('[') {
            Some(inner) => {
                let inner = inner.strip_suffix(']').ok_or(EndpointError::Host)?;
                inner
                    .parse::<std::net::Ipv6Addr>()
                    .map_err(|_| EndpointError::Host)?;
                inner
            }
            None if host.contains(':') => return Err(EndpointError::Host),
            None => {
                ServerName::try_from(host).map_err(|_| EndpointError::Host)?;
                host
            }
        };
        // Digits alone, as a port is written: the integer parser would take a sign, and a
        // leading zero is no port anyone names.
        let written = !port.starts_with('0') && port.bytes().all(|digit| digit.is_ascii_digit());
        let port = port
            .parse()
            .ok()
            .filter(|_| written)
            .ok_or(EndpointError::Port)?;
        Ok(Self {
            host: host.to_owned(),
            port,
        })
    }

    /// The host name or IP address, an IPv6 address without its brackets.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The port.
    pub fn port(&self) -> u16 {
        self.port
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(formatter, "[{}]:{}", self.host, self.port)
        } else {
            write!(formatter, "{}:{}", self.host, self.port)
        }
    }
}
