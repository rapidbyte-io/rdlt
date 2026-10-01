//! The limits of a connector listening for hosts: how many connections it holds at each stage,
//! for how long, and the file descriptors those need.

#[cfg(test)]
mod tests;

use std::time::Duration;

/// How many connections a listening connector holds, and for how long.
///
/// A connection is unauthenticated until its TLS handshake has shown a certificate naming an
/// accepted host. Then it waits for a session, or is served one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ListenLimits {
    /// Connections: how many are unauthenticated at once.
    ///
    /// A further connection closes one of them, drawn at random, so peers that never authenticate
    /// hold no more than these.
    pub unauthenticated: usize,
    /// How long a connection has to complete its TLS handshake, and then again to send HTTP/2's
    /// preface, before it is closed.
    pub handshake: Duration,
    /// Sessions: how many are served at once, each on a connection of its own.
    pub sessions: usize,
    /// Connections: how many one host holds at once, served or waiting; a further is closed.
    ///
    /// The default is for one host, which may hold every session: [`ListenLimits::shared`] gives
    /// each of several hosts its share.
    pub host_sessions: usize,
    /// Connections: how many wait for a session at once; a further is closed.
    pub waiting: usize,
    /// How long a connection waits for a session before it is closed.
    pub wait: Duration,
    /// File descriptors: how many each session may take, its socket and the files the connector
    /// opens for it.
    pub session_descriptors: usize,
    /// File descriptors: how many the connector keeps for itself, its listening socket, standard
    /// streams and runtime among them.
    pub own_descriptors: usize,
    /// How often refused connections are reported: one line for each such span in which any was
    /// refused, however many were.
    pub report_every: Duration,
}

impl Default for ListenLimits {
    fn default() -> Self {
        Self {
            unauthenticated: 64,
            handshake: Duration::from_secs(5),
            sessions: 256,
            host_sessions: 256,
            waiting: 64,
            wait: Duration::from_secs(10),
            session_descriptors: 4,
            own_descriptors: 64,
            report_every: Duration::from_secs(10),
        }
    }
}

/// A process may open fewer file descriptors than a listening connector's limits need.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error(
    "this process may open {limit} file descriptors, and listening needs {needed}: raise the \
     limit, or serve fewer sessions"
)]
pub struct TooFewDescriptors {
    /// File descriptors: how many the process may open.
    pub limit: u64,
    /// File descriptors: how many these limits need.
    pub needed: u64,
}

/// The hosts named to a connector could between them leave one of them no session.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error(
    "{hosts} hosts are named, and each may hold {host_sessions} of {sessions} sessions: the others \
     could leave a host none; serve more sessions, or let a host hold fewer"
)]
pub struct UnfairSessions {
    /// Hosts: how many are named.
    pub hosts: usize,
    /// Sessions: how many are served at once.
    pub sessions: usize,
    /// Connections: how many one host may hold.
    pub host_sessions: usize,
}

impl ListenLimits {
    /// These limits for `hosts` named hosts, serving `sessions` sessions where given, of which
    /// one host holds `host_sessions` where given and an equal share otherwise.
    ///
    /// # Errors
    ///
    /// [`UnfairSessions`] where a host may hold none, or where the other hosts, each holding all
    /// it may, would leave a host no session.
    pub fn shared(
        self,
        hosts: usize,
        sessions: Option<usize>,
        host_sessions: Option<usize>,
    ) -> Result<Self, UnfairSessions> {
        let hosts = hosts.max(1);
        let sessions = sessions.unwrap_or(self.sessions);
        let host_sessions = host_sessions.unwrap_or(sessions / hosts);
        let others = (hosts - 1).saturating_mul(host_sessions);
        if host_sessions == 0 || others >= sessions {
            return Err(UnfairSessions {
                hosts,
                sessions,
                host_sessions,
            });
        }
        Ok(Self {
            sessions,
            host_sessions,
            ..self
        })
    }

    /// File descriptors: how many a connector listening within these limits may come to hold.
    ///
    /// Every unauthenticated connection, a connection just accepted that closes one of them, every
    /// waiting connection, each session's share, and the connector's own. Unauthenticated peers
    /// hold the first two terms at most, so they cannot take a descriptor a session needs.
    pub fn descriptors(&self) -> u64 {
        let wide = |count: usize| u64::try_from(count).unwrap_or(u64::MAX);
        wide(self.unauthenticated)
            .saturating_add(1)
            .saturating_add(wide(self.waiting))
            .saturating_add(wide(self.sessions).saturating_mul(wide(self.session_descriptors)))
            .saturating_add(wide(self.own_descriptors))
    }

    /// Checks that a process that may open `limit` file descriptors can listen within these
    /// limits.
    ///
    /// # Errors
    ///
    /// [`TooFewDescriptors`] when [`descriptors`](Self::descriptors) exceeds `limit`.
    pub fn admit_descriptors(&self, limit: u64) -> Result<(), TooFewDescriptors> {
        let needed = self.descriptors();
        if needed > limit {
            return Err(TooFewDescriptors { limit, needed });
        }
        Ok(())
    }
}
