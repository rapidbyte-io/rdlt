//! Publishing the chunk staged: its end, naming what of the log a replay still needs, then the
//! chunk written whole; the chunks it leaves unneeded are deleted before any later frame.

use std::sync::atomic::Ordering;

use super::super::frame::{End, Frame};
use super::super::store::Chunk;
use super::Log;
use super::chunk::Written;
use crate::crash::crash_point;
use crate::error::Error;

impl Log {
    /// Ends the chunk staged with what a replay needs, publishes it, moves on to the next, and
    /// notes every earlier chunk the end did not name as no longer needed.
    pub(super) async fn publish(&mut self) -> Result<(), Error> {
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
        crash_point!("engine.wal.publish.before");
        staged.publish().await.map_err(|error| self.lost(error))?;
        crash_point!("engine.wal.publish.after");
        self.chunk += 1;
        self.doom(&live);
        self.note_room();
        Ok(())
    }

    /// Notes every chunk before the chunk published last that `live` does not name as no longer
    /// needed: its bookkeeping goes now, its bytes and its room once it is deleted.
    fn doom(&mut self, live: &[u64]) {
        let gone: Vec<u64> = self
            .written
            .keys()
            .filter(|number| **number < self.chunk - 1 && !live.contains(number))
            .copied()
            .collect();
        for number in gone {
            if let Some(written) = self.forgotten(number) {
                self.doomed.push((number, written.len));
            }
        }
        self.unrecorded.clear();
        self.note_oldest();
    }

    /// Deletes the chunks no replay needs, each before any later frame is written, releasing
    /// its room once it is gone.
    pub(super) async fn remove_unneeded(&mut self) -> Result<(), Error> {
        for (number, len) in std::mem::take(&mut self.doomed) {
            let chunk = Chunk {
                load: self.owner.load,
                number,
            };
            self.store
                .remove(&self.owner.pipeline, chunk)
                .await
                .map_err(Error::from_wal)?;
            self.shared.release(len);
            crash_point!("engine.wal.remove");
        }
        self.shared.room.notify_waiters();
        Ok(())
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
            self.remove_unneeded().await?;
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
