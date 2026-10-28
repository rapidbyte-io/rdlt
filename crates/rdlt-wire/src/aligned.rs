//! Bytes held from a first byte at a multiple of [`ALIGNMENT`], where Arrow reads every type in
//! place.

#[cfg(test)]
mod tests;

use std::io::Read as _;

use bytes::Bytes;

/// Bytes: the alignment a frame's allocation starts at and its buffers lie at, every Arrow type's
/// or more.
pub const ALIGNMENT: usize = 64;

/// Whether `bytes` start at a multiple of [`ALIGNMENT`].
pub fn is_aligned(bytes: &[u8]) -> bool {
    bytes.as_ptr().addr().is_multiple_of(ALIGNMENT)
}

/// Bytes from `pointer` to the next multiple of [`ALIGNMENT`].
fn skip(pointer: *const u8) -> usize {
    (ALIGNMENT - pointer.addr() % ALIGNMENT) % ALIGNMENT
}

/// Bytes gathered from a first byte at a multiple of [`ALIGNMENT`], in an allocation of the room
/// asked for and the alignment beside it.
#[derive(Debug, Default)]
pub struct Aligned {
    /// The allocation: `skip` bytes before the first held, then those held.
    bytes: Vec<u8>,
    skip: usize,
    /// Bytes it holds before it grows.
    room: usize,
}

/// What an [`Aligned`] held, frozen.
#[derive(Clone, Debug)]
pub struct Frozen {
    /// The bytes held.
    pub bytes: Bytes,
    /// The allocation they lie in, from its first byte to its last, zeroed where nothing was
    /// held: what holding `bytes` keeps alive.
    pub allocation: Bytes,
}

impl Aligned {
    /// Room for `room` bytes, none held.
    pub fn with_room(room: usize) -> Self {
        let mut aligned = Self::default();
        aligned.grow(room);
        aligned
    }

    /// `len` zero bytes, to be read into.
    pub fn zeroed(len: usize) -> Self {
        let mut aligned = Self::with_room(len);
        aligned.bytes.resize(aligned.skip + len, 0);
        aligned
    }

    /// `bytes` copied into an allocation of their own.
    pub fn copied(bytes: &[u8]) -> Frozen {
        let mut aligned = Self::with_room(bytes.len());
        aligned.extend(bytes);
        aligned.freeze()
    }

    /// Bytes held.
    pub fn len(&self) -> usize {
        self.bytes.len() - self.skip
    }

    /// Whether it holds nothing.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Bytes it may hold before it grows.
    pub fn room(&self) -> usize {
        self.room
    }

    /// The bytes held.
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[self.skip..]
    }

    /// The bytes held, to be written.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.bytes[self.skip..]
    }

    /// Grows its room to `room` bytes, keeping what it holds from an aligned first byte; a room
    /// it has already is kept.
    pub fn grow(&mut self, room: usize) {
        if room <= self.room {
            return;
        }
        let held = self.len();
        // The allocation may start anywhere: the alignment's room beside it finds an aligned byte.
        let wanted = (ALIGNMENT - 1 + room).saturating_sub(self.bytes.len());
        self.bytes.reserve_exact(wanted);
        let skip = skip(self.bytes.as_ptr());
        if skip != self.skip {
            // What it holds moves to the new allocation's aligned byte, within its room.
            self.bytes.resize(skip.max(self.skip) + held, 0);
            self.bytes.copy_within(self.skip..self.skip + held, skip);
            self.bytes.truncate(skip + held);
        }
        self.skip = skip;
        self.room = room;
    }

    /// Appends `data`, growing its room to hold it exactly where it must.
    pub fn extend(&mut self, data: &[u8]) {
        let len = self.len().saturating_add(data.len());
        self.grow(len);
        self.bytes.extend_from_slice(data);
    }

    /// Appends at most `len` bytes of `reader`, all of them unless it ends first, growing its
    /// room to hold them; nothing is written twice, as zeroed room to read into would be.
    ///
    /// # Errors
    ///
    /// The error reading failed with.
    pub fn read_from(&mut self, reader: impl std::io::Read, len: u64) -> std::io::Result<()> {
        let wanted = usize::try_from(len).unwrap_or(usize::MAX);
        self.grow(self.len().saturating_add(wanted));
        // Room for all it may read is reserved, so the allocation stays where it is aligned.
        reader.take(len).read_to_end(&mut self.bytes)?;
        Ok(())
    }

    /// Keeps the first `len` bytes held.
    pub fn truncate(&mut self, len: usize) {
        self.bytes.truncate(self.skip.saturating_add(len));
    }

    /// What it held, and the allocation it lies in, zeroed to its end.
    pub fn freeze(mut self) -> Frozen {
        let (skip, len) = (self.skip, self.len());
        let capacity = self.bytes.capacity();
        self.bytes.resize(capacity, 0);
        let allocation = Bytes::from(self.bytes);
        Frozen {
            bytes: allocation.slice(skip..skip + len),
            allocation,
        }
    }
}
