//! Fetch message for retrieving rows from a cursor
//!
//! This module implements the fetch message used to retrieve additional
//! rows from an already-executed query cursor.

use bytes::Bytes;

use crate::buffer::WriteBuffer;
use crate::capabilities::Capabilities;
use crate::constants::{FetchOrientation, FunctionCode, MessageType};
use crate::error::Result;

/// Fetch message to retrieve rows from a cursor
#[derive(Debug)]
pub struct FetchMessage {
    /// Cursor ID to fetch from
    cursor_id: u16,
    /// Number of rows to fetch
    num_rows: u32,
    /// Fetch orientation for scrollable cursors
    orientation: Option<FetchOrientation>,
    /// Fetch offset/position for scrollable cursors
    offset: i64,
}

impl FetchMessage {
    /// Create a new fetch message
    pub fn new(cursor_id: u16, num_rows: u32) -> Self {
        Self {
            cursor_id,
            num_rows,
            orientation: None,
            offset: 0,
        }
    }

    /// Create a new scrollable fetch message
    pub fn new_scrollable(
        cursor_id: u16,
        num_rows: u32,
        orientation: FetchOrientation,
        offset: i64,
    ) -> Self {
        Self {
            cursor_id,
            num_rows,
            orientation: Some(orientation),
            offset,
        }
    }

    /// Build the fetch request MESSAGE BODY.
    ///
    /// rdlt patch (032): three defects fixed against the published
    /// 0.1.7 — the sequence number was hardcoded 0 while every other
    /// message advances it; the ub8 token required at TTC field
    /// version >= 18 (Oracle 23ai) was missing, so modern servers
    /// misparsed the body from its third byte; and the packet header
    /// was written here with a small-SDU 2-byte length even when the
    /// connection negotiated large SDU. Framing now belongs to the
    /// connection's own sender (as it does for execute and lob_op),
    /// so this returns the BODY and the caller frames it.
    pub fn build_request(&self, caps: &Capabilities, sequence_number: u8) -> Result<Bytes> {
        let mut buf = WriteBuffer::new();

        // Write message header
        buf.write_u8(MessageType::Function as u8)?;
        buf.write_u8(FunctionCode::Fetch as u8)?;
        buf.write_u8(sequence_number)?;

        // Token number (required for TTC field version >= 18, i.e. Oracle 23ai)
        if caps.ttc_field_version >= 18 {
            buf.write_ub8(0)?;
        }

        // Write fetch body
        buf.write_ub4(self.cursor_id as u32)?;
        buf.write_ub4(self.num_rows)?;

        // Write scrollable cursor fields if present
        if let Some(orientation) = self.orientation {
            buf.write_ub4(orientation as u32)?; // Fetch orientation
            buf.write_ub4(self.offset as u32)?; // Fetch position (for absolute/relative)
        }

        Ok(buf.freeze())
    }

    /// Get the cursor ID
    pub fn cursor_id(&self) -> u16 {
        self.cursor_id
    }

    /// Get the number of rows to fetch
    pub fn num_rows(&self) -> u32 {
        self.num_rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fetch_message_creation() {
        let msg = FetchMessage::new(1, 100);
        assert_eq!(msg.cursor_id(), 1);
        assert_eq!(msg.num_rows(), 100);
    }

    // rdlt patch (032): `test_fetch_message_builds_packet` REMOVED.
    // It called `build_request(&caps)` and asserted a packet header,
    // but the patched `build_request` takes a sequence number and
    // returns the BODY — framing moved to the caller. The test could
    // not compile, so `cargo test -p oracle-rs` was broken for
    // anyone who ran it directly (the workspace never builds this
    // crate's tests; it enters via [patch.crates-io]).
}
