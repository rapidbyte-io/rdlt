//! The signals that end a host, heard so that it stops what it spawned before it exits.

/// Hears this process being interrupted or asked to terminate (`SIGINT`, `SIGTERM`).
///
/// A host that spawns connectors owns their process groups, which a terminal's Ctrl-C does not
/// reach: it listens before it spawns, awaits [`heard`](Self::heard) beside its work, and once
/// that answers calls [`stop_spawned`](super::stop_spawned) and exits. From the moment it
/// listens, neither signal ends the process by itself.
#[derive(Debug)]
pub struct Interrupts {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

impl Interrupts {
    /// Starts listening, within a runtime.
    ///
    /// # Errors
    ///
    /// The error of installing the signals' handlers.
    pub fn listen() -> std::io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }

    /// Waits for either signal, and answers the exit status a process so ended has: 130 for an
    /// interrupt, 143 for a termination.
    pub async fn heard(&mut self) -> i32 {
        tokio::select! {
            biased;
            _ = self.interrupt.recv() => 130,
            _ = self.terminate.recv() => 143,
        }
    }
}
