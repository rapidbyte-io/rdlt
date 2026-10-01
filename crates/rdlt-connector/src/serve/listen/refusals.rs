//! What a listening connector reports: the host of each session it serves, and, at most once an
//! interval, how many connections it refused and why.
//!
//! A peer that has not authenticated decides how many connections are refused, so no line is
//! written for one of them.

#[cfg(test)]
mod tests;

use std::fmt;
use std::io::Write as _;
use std::sync::Arc;
use std::time::Duration;

/// Where a listening connector writes its lines.
#[derive(Clone)]
pub struct Log(Arc<dyn Fn(&str) + Send + Sync>);

impl Log {
    /// Writes each line with `write`.
    pub fn new(write: impl Fn(&str) + Send + Sync + 'static) -> Self {
        Self(Arc::new(write))
    }

    /// Writes each line to standard error, which whoever runs the connector keeps.
    pub fn stderr() -> Self {
        Self::new(|line| {
            let mut stderr = std::io::stderr().lock();
            writeln!(stderr, "{line}").ok();
        })
    }

    pub(super) fn line(&self, line: &str) {
        (self.0)(line);
    }
}

impl fmt::Debug for Log {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Log").finish_non_exhaustive()
    }
}

/// Why a connection was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Refused {
    /// Accepting it failed.
    Accept,
    /// It had not authenticated, and was closed for a newer connection.
    Displaced,
    /// Its TLS handshake failed: no certificate, or one that is not accepted.
    Handshake,
    /// Its certificate is revoked.
    Revoked,
    /// No current revocation list says whether its certificate is revoked: the connector has
    /// none of its issuer, or one past its next update.
    Unlisted,
    /// It did not complete its TLS handshake in time.
    Slow,
    /// Its host holds as many connections as one host may.
    HostFull,
    /// As many connections as may were waiting for a session.
    QueueFull,
    /// It waited for a session as long as it may.
    Waited,
    /// Serving its session failed in its transport.
    Transport,
}

impl Refused {
    /// Why a TLS handshake that failed with `error` was refused.
    ///
    /// Revocation is told apart: a connector whose lists have gone stale refuses every host, and
    /// its operator must be able to see why.
    pub(super) fn handshake(error: &std::io::Error) -> Self {
        use tokio_rustls::rustls::{CertificateError, Error};
        let tls = error.get_ref().and_then(|error| error.downcast_ref());
        match tls {
            Some(Error::InvalidCertificate(CertificateError::Revoked)) => Self::Revoked,
            Some(Error::InvalidCertificate(
                CertificateError::UnknownRevocationStatus
                | CertificateError::ExpiredRevocationList
                | CertificateError::ExpiredRevocationListContext { .. },
            )) => Self::Unlisted,
            _ => Self::Handshake,
        }
    }
}

/// How many connections were refused since the last report, by why.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Refusals {
    accept: u64,
    displaced: u64,
    handshake: u64,
    revoked: u64,
    unlisted: u64,
    slow: u64,
    host_full: u64,
    queue_full: u64,
    waited: u64,
    transport: u64,
}

impl Refusals {
    pub(super) fn count(&mut self, why: Refused) {
        let counter = match why {
            Refused::Accept => &mut self.accept,
            Refused::Displaced => &mut self.displaced,
            Refused::Handshake => &mut self.handshake,
            Refused::Revoked => &mut self.revoked,
            Refused::Unlisted => &mut self.unlisted,
            Refused::Slow => &mut self.slow,
            Refused::HostFull => &mut self.host_full,
            Refused::QueueFull => &mut self.queue_full,
            Refused::Waited => &mut self.waited,
            Refused::Transport => &mut self.transport,
        };
        *counter = counter.saturating_add(1);
    }

    /// The line that reports these refusals over the last `span`, and forgets them; none where
    /// nothing was refused.
    pub(super) fn report(&mut self, span: Duration) -> Option<String> {
        use fmt::Write as _;
        let counted = [
            (self.accept, "could not be accepted"),
            (self.displaced, "closed unauthenticated for a newer one"),
            (self.handshake, "failed their TLS handshake"),
            (self.revoked, "presented a revoked certificate"),
            (
                self.unlisted,
                "presented a certificate no current revocation list covers",
            ),
            (self.slow, "did not complete their TLS handshake in time"),
            (self.host_full, "over their host's sessions"),
            (self.queue_full, "over the connections that may wait"),
            (self.waited, "waited too long for a session"),
            (self.transport, "failed while served"),
        ];
        let total = counted
            .iter()
            .fold(0_u64, |total, (count, _)| total.saturating_add(*count));
        if total == 0 {
            return None;
        }
        let mut line = format!("refused {total} connections in the last {span:?}:");
        let mut separator = " ";
        for (count, why) in counted.iter().filter(|(count, _)| *count > 0) {
            write!(line, "{separator}{count} {why}").ok();
            separator = ", ";
        }
        *self = Self::default();
        Some(line)
    }
}
