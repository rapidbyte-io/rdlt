//! The signals that end a host, heard so that it stops what it spawned before it exits.

use tokio::signal::unix::{Signal, SignalKind, signal};

/// Hears this process being asked to end: interrupted, asked to terminate, hung up on, or
/// asked to quit (`SIGINT`, `SIGTERM`, `SIGHUP`, `SIGQUIT`).
///
/// A host that spawns connectors owns their process groups, which a terminal's Ctrl-C, and
/// its hanging up, do not reach: it listens before it spawns, awaits [`heard`](Self::heard)
/// beside its work, and once that answers calls [`stop_spawned`](super::stop_spawned) and
/// exits. From the moment it listens, none of the four signals ends the process by itself,
/// for as long as the process lives: a signal that comes once nothing awaits is not acted on.
#[derive(Debug)]
pub struct Interrupts {
    /// Each signal heard, with the exit status of a process it ended.
    signals: [(Signal, i32); 4],
}

impl Interrupts {
    /// Starts listening, within a runtime.
    ///
    /// # Errors
    ///
    /// The error of installing the signals' handlers.
    pub fn listen() -> std::io::Result<Self> {
        Ok(Self {
            signals: [
                (signal(SignalKind::interrupt())?, 130),
                (signal(SignalKind::terminate())?, 143),
                (signal(SignalKind::hangup())?, 129),
                (signal(SignalKind::quit())?, 131),
            ],
        })
    }

    /// Waits for any of the signals, and answers the exit status a process so ended has: 130
    /// for an interrupt, 143 for a termination, 129 for a hangup, 131 for a quit.
    ///
    /// A signal that came since the last answer is answered at once.
    pub async fn heard(&mut self) -> i32 {
        let [interrupt, terminate, hangup, quit] = &mut self.signals;
        tokio::select! {
            biased;
            _ = interrupt.0.recv() => interrupt.1,
            _ = terminate.0.recv() => terminate.1,
            _ = hangup.0.recv() => hangup.1,
            _ = quit.0.recv() => quit.1,
        }
    }
}
