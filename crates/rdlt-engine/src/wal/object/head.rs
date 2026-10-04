//! A chunk's head: the object its name holds, which is the chunk itself, or a reference to the body
//! a longer chunk was uploaded as in parts.

use std::collections::BTreeMap;

use bytes::{BufMut as _, Bytes, BytesMut};
use parking_lot::Mutex;
use rdlt_connector::{LoadId, PipelineId};

use crate::limits::OBJECT_HEADS;
use crate::wal::Chunk;

/// What a reference begins with; a chunk begins with its log's preamble, `rdltwal\0`.
const MAGIC: &[u8; 8] = b"rdltref\0";

/// The format of a reference.
const FORMAT: u16 = 1;

/// Bytes: what a reference takes: its magic, format, body's token and length, and a CRC32C of
/// the rest.
pub(super) const REFERENCE: usize = 8 + 2 + 16 + 8 + 4;

/// What a chunk's head is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    /// The chunk itself.
    Whole,
    /// A reference to the body the chunk was uploaded as.
    Parts(Reference),
}

/// A chunk uploaded in parts: the token naming its body, and its length.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Reference {
    pub(super) token: u128,
    pub(super) len: u64,
}

impl Reference {
    /// The reference as a head holds it.
    pub(super) fn encode(self) -> Bytes {
        let mut out = BytesMut::with_capacity(REFERENCE);
        out.put_slice(MAGIC);
        out.put_u16_le(FORMAT);
        out.put_u128_le(self.token);
        out.put_u64_le(self.len);
        out.put_u32_le(crc32c::crc32c(&out));
        out.freeze()
    }

    /// The reference `bytes` hold, where they are one whole and intact.
    pub(super) fn decode(bytes: &[u8]) -> Option<Self> {
        let (body, crc) = bytes.split_at_checked(REFERENCE - 4)?;
        let crc: [u8; 4] = crc.try_into().ok()?;
        let rest = body.strip_prefix(MAGIC)?;
        if u32::from_le_bytes(crc) != crc32c::crc32c(body) || rest.get(..2)? != FORMAT.to_le_bytes()
        {
            return None;
        }
        let token = u128::from_le_bytes(rest.get(2..18)?.try_into().ok()?);
        let len = u64::from_le_bytes(rest.get(18..26)?.try_into().ok()?);
        Some(Self { token, len })
    }
}

/// The kinds of chunks a store looked at, at most [`OBJECT_HEADS`]: a head never changes once
/// published, so its kind is asked once.
#[derive(Debug, Default)]
pub(super) struct Heads {
    kinds: Mutex<BTreeMap<(PipelineId, Chunk), Kind>>,
}

impl Heads {
    /// The kind of `chunk` of `pipeline`'s log, where it is known.
    pub(super) fn kind(&self, pipeline: &PipelineId, chunk: Chunk) -> Option<Kind> {
        self.kinds.lock().get(&(pipeline.clone(), chunk)).copied()
    }

    /// Notes `chunk`'s kind, forgetting another's where it holds as many as it may.
    pub(super) fn note(&self, pipeline: &PipelineId, chunk: Chunk, kind: Kind) {
        let mut kinds = self.kinds.lock();
        if kinds.len() >= OBJECT_HEADS {
            kinds.pop_first();
        }
        kinds.insert((pipeline.clone(), chunk), kind);
    }

    /// Forgets `chunk`, deleted.
    pub(super) fn forget(&self, pipeline: &PipelineId, chunk: Chunk) {
        self.kinds.lock().remove(&(pipeline.clone(), chunk));
    }

    /// Forgets every chunk of `load`'s log of `pipeline`, removed.
    pub(super) fn forget_log(&self, pipeline: &PipelineId, load: LoadId) {
        self.kinds
            .lock()
            .retain(|(owner, chunk), _| owner != pipeline || chunk.load != load);
    }
}
