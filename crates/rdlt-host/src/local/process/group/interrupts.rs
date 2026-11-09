//! The signals that end a host, heard so that it stops what it spawned before it exits.

#[cfg(test)]
mod tests;

use tokio::signal::unix::{Signal, SignalKind, signal};

/// Hears this process being asked to end: interrupted, asked to terminate, hung up on, or
/// asked to quit (`SIGINT`, `SIGTERM`, `SIGHUP`, `SIGQUIT`).
///
/// A host that spawns connectors owns their process groups, which a terminal's Ctrl-C, and
/// its hanging up, do not reach: it listens before it spawns, awaits [`heard`](Self::heard)
/// beside its work, and once that answers calls [`stop_spawned`](super::stop_spawned) and
/// exits. From the moment it listens, none of the four signals ends the process by itself,
/// for as long as the process lives: a signal that comes once nothing awaits is not acted on.
///
/// A signal this process was started with ignored, as `nohup` starts it with hangups, stays
/// ignored on Linux, where `/proc` says so: whoever started it asked that, and it is left to go
/// on. macOS has no `/proc`, and there it is heard and handled as if it were not ignored.
#[derive(Debug)]
pub struct Interrupts {
    /// Each signal heard, unless it is left ignored, with the exit status of a process it ended.
    signals: [(Option<Signal>, i32); 4],
}

impl Interrupts {
    /// Starts listening, within a runtime.
    ///
    /// # Errors
    ///
    /// The error of installing the signals' handlers.
    pub fn listen() -> std::io::Result<Self> {
        let ignored =
            std::fs::read_to_string("/proc/self/status").map_or(0, |status| ignored_in(&status));
        let hear = |kind: SignalKind| -> std::io::Result<Option<Signal>> {
            if among(kind, ignored) {
                return Ok(None);
            }
            signal(kind).map(Some)
        };
        Ok(Self {
            signals: [
                (hear(SignalKind::interrupt())?, 130),
                (hear(SignalKind::terminate())?, 143),
                (hear(SignalKind::hangup())?, 129),
                (hear(SignalKind::quit())?, 131),
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
            () = received(&mut interrupt.0) => interrupt.1,
            () = received(&mut terminate.0) => terminate.1,
            () = received(&mut hangup.0) => hangup.1,
            () = received(&mut quit.0) => quit.1,
        }
    }
}

/// Waits for `signal`, forever where it is left ignored.
async fn received(signal: &mut Option<Signal>) {
    match signal {
        Some(signal) => {
            signal.recv().await;
        }
        None => std::future::pending().await,
    }
}

/// Whether `kind` is among the signals of `mask`, with a bit `n - 1` set for signal `n`.
fn among(kind: SignalKind, mask: u64) -> bool {
    mask & (1_u64 << (kind.as_raw_value() - 1)) != 0
}

/// The mask of signals ignored in `status`, what `/proc/self/status` holds: its `SigIgn` line, a
/// hexadecimal mask of bit `n - 1` for signal `n`.
fn ignored_in(status: &str) -> u64 {
    status
        .lines()
        .find_map(|line| line.strip_prefix("SigIgn:"))
        .and_then(|mask| u64::from_str_radix(mask.trim(), 16).ok())
        .unwrap_or(0)
}
