//! The HTTP/2 settings both ends of a control connection set on what they receive, which bound
//! what a peer sends on each call that this end has not taken.

#[cfg(test)]
mod tests;

use crate::limits::{MAX_CALLS, STREAM_WINDOW, TRANSPORT_FRAME_BYTES};

/// The HTTP/2 settings each end of a connection sets on what it receives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Transport {
    /// Bytes: each call's window, what a peer may send on it that this end has not taken.
    pub stream_window: u32,
    /// Bytes: the connection's window, which holds every call's and one more, so a call that
    /// keeps to its window never holds another, the heartbeat among them.
    pub connection_window: u32,
    /// Bytes: the most one transport frame carries.
    pub max_frame: u32,
}

impl Default for Transport {
    /// The settings both ends set: a stream window of [`STREAM_WINDOW`], whatever the limits, so a
    /// peer parks at most that much a call.
    fn default() -> Self {
        let calls = u64::from(MAX_CALLS) + 1;
        let connection_window = calls.saturating_mul(STREAM_WINDOW);
        let window = |bytes: u64| u32::try_from(bytes).unwrap_or(u32::MAX);
        Self {
            stream_window: window(STREAM_WINDOW),
            connection_window: window(connection_window),
            max_frame: TRANSPORT_FRAME_BYTES,
        }
    }
}
