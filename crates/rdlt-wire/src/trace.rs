//! The W3C trace context a host sends in each call's metadata: `traceparent` of version 00,
//! read only where it is exactly that, and ignored otherwise.

#[cfg(test)]
mod tests;

use std::fmt;

use crate::limits::TRACEPARENT_BYTES;

/// The metadata key a call's trace context travels under.
pub const TRACEPARENT: &str = "traceparent";

/// A trace's id, the calling span's id and the trace's flags.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TraceParent {
    trace_id: [u8; 16],
    parent_id: [u8; 8],
    flags: u8,
}

impl TraceParent {
    /// The context of trace `trace_id`, called from span `parent_id`; none where either id is
    /// all zero, which W3C reserves as invalid.
    pub fn new(trace_id: [u8; 16], parent_id: [u8; 8], flags: u8) -> Option<Self> {
        let set = |bytes: &[u8]| bytes.iter().any(|byte| *byte != 0);
        (set(&trace_id) && set(&parent_id)).then_some(Self {
            trace_id,
            parent_id,
            flags,
        })
    }

    /// The context `text` holds: `00-<32 hex>-<16 hex>-<2 hex>`, lower case, exactly
    /// [`TRACEPARENT_BYTES`]; none for anything else.
    pub fn parse(text: &[u8]) -> Option<Self> {
        if text.len() != TRACEPARENT_BYTES
            || &text[..3] != b"00-"
            || text[35] != b'-'
            || text[52] != b'-'
        {
            return None;
        }
        let mut trace_id = [0; 16];
        let mut parent_id = [0; 8];
        let mut flags = [0; 1];
        hex(&text[3..35], &mut trace_id)?;
        hex(&text[36..52], &mut parent_id)?;
        hex(&text[53..55], &mut flags)?;
        Self::new(trace_id, parent_id, flags[0])
    }

    /// The trace's id.
    pub fn trace_id(&self) -> [u8; 16] {
        self.trace_id
    }

    /// The calling span's id.
    pub fn parent_id(&self) -> [u8; 8] {
        self.parent_id
    }

    /// The trace's flags; bit 0 says it is sampled.
    pub fn flags(&self) -> u8 {
        self.flags
    }
}

/// Fills `into` from the lower-case hex `text`, two digits a byte.
fn hex(text: &[u8], into: &mut [u8]) -> Option<()> {
    let digit = |byte: u8| match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    };
    let (pairs, _) = text.as_chunks::<2>();
    for (byte, pair) in into.iter_mut().zip(pairs) {
        *byte = digit(pair[0])? * 16 + digit(pair[1])?;
    }
    Some(())
}

impl fmt::Display for TraceParent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("00-")?;
        self.trace_id
            .iter()
            .try_for_each(|byte| write!(formatter, "{byte:02x}"))?;
        formatter.write_str("-")?;
        self.parent_id
            .iter()
            .try_for_each(|byte| write!(formatter, "{byte:02x}"))?;
        write!(formatter, "-{:02x}", self.flags)
    }
}
