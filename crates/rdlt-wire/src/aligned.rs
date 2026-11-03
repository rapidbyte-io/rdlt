//! Bytes held from a first byte at a multiple of [`ALIGNMENT`], where Arrow reads every type in
//! place.

#[cfg(test)]
mod tests;

use std::io::Read as _;
use std::mem;

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

/// An allocation nothing holds any more, kept for a later message to arrive in.
#[derive(Debug)]
pub struct Spare(Vec<u8>);

impl Spare {
    /// Bytes: what it takes.
    pub fn bytes(&self) -> usize {
        self.0.capacity()
    }

    /// Whether a message needing `room` bytes may arrive in it: one needing no more than its room
    /// and at least half of it, so taking it frees no more than half.
    pub fn fits(&self, room: usize) -> bool {
        let held = self.0.capacity().saturating_sub(ALIGNMENT - 1);
        room <= held && held / 2 <= room
    }
}

/// An allocation frozen, handed to its home once the last of what holds it drops.
struct Returning<F: FnOnce(Spare)> {
    bytes: Vec<u8>,
    home: Option<F>,
}

impl<F: FnOnce(Spare)> AsRef<[u8]> for Returning<F> {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl<F: FnOnce(Spare)> Drop for Returning<F> {
    fn drop(&mut self) {
        if let Some(home) = self.home.take() {
            home(Spare(mem::take(&mut self.bytes)));
        }
    }
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

    /// Room for `room` bytes in `spare`'s allocation, none held: the allocation shrunk to what
    /// [`Aligned::with_room`] takes where it takes more, and grown where it takes less.
    pub fn in_spare(spare: Spare, room: usize) -> Self {
        if room == 0 {
            return Self::default();
        }
        let mut bytes = spare.0;
        bytes.clear();
        bytes.shrink_to(ALIGNMENT - 1 + room);
        let mut aligned = Self {
            bytes,
            skip: 0,
            room: 0,
        };
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
    pub fn freeze(self) -> Frozen {
        let (skip, len, bytes) = self.zeroed_to_end();
        Frozen::of(Bytes::from(bytes), skip, len)
    }

    /// What it held, frozen as [`Aligned::freeze`] freezes it, its allocation handed to `home`
    /// once nothing holds any of it.
    pub fn freeze_returning(self, home: impl FnOnce(Spare) + Send + 'static) -> Frozen {
        let (skip, len, bytes) = self.zeroed_to_end();
        let returning = Returning {
            bytes,
            home: Some(home),
        };
        Frozen::of(Bytes::from_owner(returning), skip, len)
    }

    /// Where what it holds starts, how much it holds, and its allocation zeroed past that.
    fn zeroed_to_end(mut self) -> (usize, usize, Vec<u8>) {
        let (skip, len) = (self.skip, self.len());
        let capacity = self.bytes.capacity();
        self.bytes.resize(capacity, 0);
        (skip, len, self.bytes)
    }
}

impl Frozen {
    /// The `len` bytes at `skip` of `allocation`, and the allocation.
    fn of(allocation: Bytes, skip: usize, len: usize) -> Self {
        Self {
            bytes: allocation.slice(skip..skip + len),
            allocation,
        }
    }
}
