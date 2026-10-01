//! A reader held to a limit while it is read.

use std::io;

use super::Limit;

/// A reader that refuses the first byte beyond a limit, so a file that grows while it is read
/// fails its read rather than being read without end or cut short.
#[derive(Debug)]
pub(crate) struct Limited<R> {
    inner: R,
    limit: Limit,
    read: u64,
}

impl<R> Limited<R> {
    /// `inner`, of which at most `limit` is read.
    pub(crate) fn new(inner: R, limit: Limit) -> Self {
        Self {
            inner,
            limit,
            read: 0,
        }
    }
}

impl<R: io::Read> io::Read for Limited<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.read = self
            .read
            .saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
        // The size refused is the first beyond the limit, however far the read went.
        let within = self.read.min(self.limit.bytes.saturating_add(1));
        self.limit.admit(within)?;
        Ok(read)
    }
}
