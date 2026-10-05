//! Publishing the chunk staged: its end, naming what of the log a replay still needs, then the
//! chunk written whole, then the deletion of every chunk it leaves unneeded.

use std::sync::atomic::Ordering;

use super::super::frame::{End, Frame};
use super::super::store::Chunk;
use super::Log;
use super::chunk::Written;
use crate::crash::crash_point;
use crate::error::Error;

impl Log {
    /// Ends the chunk staged with what a replay needs, publishes it, moves on to the next, and
    /// deletes every earlier chunk the end did not name: the bytes those held.
    pub(super) async fn publish(&mut self) -> Result<u64, Error> {
        let live: Vec<u64> = self
            .written
            .iter()
            .filter(|(number, _)| **number < self.chunk)
            .filter(|(_, written)| self.needed(written))
            .map(|(number, _)| *number)
            .collect();
        let received = live
            .iter()
            .flat_map(|number| &self.written[number].commits)
            .filter(|seq| !self.pending.contains_key(seq))
            .copied()
            .collect();
        let end = Frame::End(End {
            live: live.clone(),
            received,
        });
        self.append(end.encode()?).await?;
        let staged = self
            .staged
            .take()
            .ok_or_else(|| Error::internal("a chunk was published that was never staged"))?;
        crash_point!("engine.wal.sync.before");
        staged.publish().await.map_err(|error| self.lost(error))?;
        crash_point!("engine.wal.sync.after");
        self.chunk += 1;
        let freed = self.forget(&live).await?;
        self.note_room();
        Ok(freed)
    }

    /// Deletes every chunk before the chunk published last that `live` does not name; every
    /// receipt is recorded now, or its commit's chunk gone: the bytes those held.
    async fn forget(&mut self, live: &[u64]) -> Result<u64, Error> {
        let gone: Vec<u64> = self
            .written
            .keys()
            .filter(|number| **number < self.chunk - 1 && !live.contains(number))
            .copied()
            .collect();
        let mut freed = 0_u64;
        for number in gone {
            let chunk = Chunk {
                load: self.owner.load,
                number,
            };
            self.store
                .remove(&self.owner.pipeline, chunk)
                .await
                .map_err(Error::from_wal)?;
            if let Some(written) = self.forgotten(number) {
                self.shared.release(written.len);
                freed = freed.saturating_add(written.len);
            }
            crash_point!("engine.wal.remove");
        }
        self.unrecorded.clear();
        self.note_oldest();
        self.shared.room.notify_waiters();
        Ok(freed)
    }

    /// Whether a replay needs `written`, a published chunk: a segment in it is neither settled
    /// nor carried to a later chunk, or a commit in it waits for its receipt.
    pub(super) fn needed(&self, written: &Written) -> bool {
        let open = !written.carried
            && written
                .segments
                .iter()
                .any(|segment| !self.settled.contains(*segment));
        open || written
            .commits
            .iter()
            .any(|seq| self.pending.contains_key(seq))
    }

    /// Closes the log: a closing chunk published, then the log deleted where no commit waits
    /// for a receipt.
    pub(super) async fn close(&mut self) -> Result<(), Error> {
        self.failure()?;
        // A log of no chunk holds nothing to close: it goes as it is.
        if !self.written.is_empty() {
            self.append(Frame::Closed.encode()?).await?;
            crash_point!("engine.wal.close.before");
            self.publish().await?;
            crash_point!("engine.wal.close.after");
        }
        if !self.pending.is_empty() {
            return Ok(());
        }
        self.written.clear();
        self.store
            .remove_log(&self.owner.pipeline, self.owner.load)
            .await
            .map_err(Error::from_wal)?;
        self.shared.held.store(0, Ordering::Relaxed);
        self.shared.room.notify_waiters();
        crash_point!("engine.wal.removed");
        Ok(())
    }
}
