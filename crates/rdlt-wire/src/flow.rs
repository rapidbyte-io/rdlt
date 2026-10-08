//! Flow control of a data-plane call: the credit a receiver grants, the credit a sender spends,
//! and the HTTP/2 windows and frame size both ends set on their transport.
//!
//! Credit paces a sender that keeps to it. HTTP/2's windows bound one that does not: a receiver's
//! transport refuses a frame beyond a call's window, so what a peer sends that its receiver has
//! not taken is at most a stream window for each open call.

#[cfg(test)]
mod tests;

use crate::limits::{CREDIT_FLOOR, Class, Limits, MAX_CALLS, TRANSPORT_FRAME_BYTES};

/// A receiver's credit: what it grants a call's sender, a window of bytes that grows to two of
/// the largest frames it has taken, between its floor and its own data wire bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Granting {
    floor: u64,
    limit: u64,
    window: u64,
}

impl Granting {
    /// A receiver whose window starts at `floor`, at least a byte and at most a frame of
    /// `limits`' data wire bound.
    pub fn new(floor: u64, limits: &Limits) -> Self {
        let limit = u64::try_from(limits.decoding(Class::Data)).unwrap_or(u64::MAX);
        let floor = floor.clamp(1, limit.max(1));
        Self {
            floor,
            limit,
            window: floor,
        }
    }

    /// The credit the call opens with: the floor.
    pub fn opening(&self) -> u64 {
        self.floor
    }

    /// Bytes: the window of credit the sender has once every frame is taken.
    pub fn window(&self) -> u64 {
        self.window
    }

    /// The credit a frame of `bytes` returns once taken: its bytes, and whatever the window
    /// grows by to hold two such frames.
    pub fn taken(&mut self, bytes: u64) -> u64 {
        let two = bytes.min(self.limit).saturating_mul(2);
        let grown = two.clamp(self.floor, self.limit.max(self.floor));
        let growth = grown.saturating_sub(self.window);
        self.window += growth;
        bytes.saturating_add(growth)
    }
}

/// A sender's credit: what its receiver granted less what its frames spent.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Spending {
    credit: i64,
}

impl Spending {
    /// Whether a frame may go: while any credit is left, whatever the frame's size.
    pub fn may_send(&self) -> bool {
        self.credit > 0
    }

    /// Bytes: the credit left, below zero by what the last frame spent beyond it.
    pub fn credit(&self) -> i64 {
        self.credit
    }

    /// Spends a frame of `bytes`.
    pub fn spend(&mut self, bytes: u64) {
        let bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
        self.credit = self.credit.saturating_sub(bytes);
    }

    /// Takes a grant of `bytes`.
    ///
    /// # Errors
    ///
    /// [`ZeroGrant`] for a grant of no bytes.
    pub fn grant(&mut self, bytes: u64) -> Result<(), ZeroGrant> {
        if bytes == 0 {
            return Err(ZeroGrant);
        }
        let bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
        self.credit = self.credit.saturating_add(bytes);
        Ok(())
    }
}

/// A grant of no bytes, which grants nothing and only keeps a sender waiting.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("a credit of no bytes grants nothing")]
pub struct ZeroGrant;

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
    /// The settings both ends set: a stream window of the credit floor, whatever the limits, so a
    /// sender within it is never held by the transport and a peer that ignores its credit parks
    /// at most the floor a call.
    fn default() -> Self {
        let stream_window = CREDIT_FLOOR;
        let calls = u64::from(MAX_CALLS) + 1;
        let connection_window = calls.saturating_mul(stream_window);
        let window = |bytes: u64| u32::try_from(bytes).unwrap_or(u32::MAX);
        Self {
            stream_window: window(stream_window),
            connection_window: window(connection_window),
            max_frame: TRANSPORT_FRAME_BYTES,
        }
    }
}
