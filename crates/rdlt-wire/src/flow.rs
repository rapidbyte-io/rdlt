//! Flow control of a data-plane call: the credit a receiver grants, the credit a sender spends,
//! and the HTTP/2 windows and frame size both ends set on their transport.

#[cfg(test)]
mod tests;

use crate::limits::Limits;

/// A receiver's credit: what it grants a call's sender, a window of bytes that grows to two of
/// the largest frames it has taken, between its floor and its own data wire bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Granting {
    floor: u64,
    limit: u64,
    window: u64,
}

impl Granting {
    /// A receiver whose window starts at `floor`, within `limits`.
    pub fn new(floor: u64, limits: &Limits) -> Self {
        let _ = (floor, limits);
        todo!()
    }

    /// The credit the call opens with: the floor.
    pub fn opening(&self) -> u64 {
        todo!()
    }

    /// Bytes: the window of credit the sender has once every frame is taken.
    pub fn window(&self) -> u64 {
        todo!()
    }

    /// The credit a frame of `bytes` returns once taken.
    pub fn taken(&mut self, bytes: u64) -> u64 {
        let _ = bytes;
        todo!()
    }
}

/// A sender's credit: what its receiver granted less what its frames spent.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Spending {
    credit: i64,
}

impl Spending {
    /// Whether a frame may go.
    pub fn may_send(&self) -> bool {
        todo!()
    }

    /// Bytes: the credit left.
    pub fn credit(&self) -> i64 {
        todo!()
    }

    /// Spends a frame of `bytes`.
    pub fn spend(&mut self, bytes: u64) {
        let _ = bytes;
        todo!()
    }

    /// Takes a grant of `bytes`.
    ///
    /// # Errors
    ///
    /// [`ZeroGrant`] for a grant of no bytes.
    pub fn grant(&mut self, bytes: u64) -> Result<(), ZeroGrant> {
        let _ = bytes;
        todo!()
    }
}

/// A grant of no bytes, which grants nothing and only keeps a sender waiting.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("a credit of no bytes grants nothing")]
pub struct ZeroGrant;

/// The HTTP/2 settings each end of a connection sets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Transport {
    /// Bytes: each call's window.
    pub stream_window: u32,
    /// Bytes: the connection's window.
    pub connection_window: u32,
    /// Bytes: the most one transport frame carries.
    pub max_frame: u32,
}

impl Transport {
    /// The settings of an end that receives within `limits`.
    pub fn of(limits: &Limits) -> Self {
        let _ = limits;
        todo!()
    }
}
