//! Publishing the chunk staged: its end, naming what of the log a replay still needs, then the
//! chunk written whole, then the deletion of every chunk it leaves unneeded.

use std::sync::atomic::Ordering;

use super::super::frame::{End, Frame};
use super::{Chunk, Log, Written};
use crate::crash::crash_point;
use crate::error::Error;

impl Log {
    /// Ends the chunk staged with what a replay needs, publishes it, moves on to the next, and
    /// deletes every earlier chunk the end did not name.
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
        crash_point!("engine.wal.sync.before");
        staged.publish().await.map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                Error::wal_fenced(self.owner.load)
            } else {
                Error::from_wal(error)
            }
        })?;
        crash_point!("engine.wal.sync.after");
        self.chunk += 1;
        let gone: Vec<u64> = self
            .written
            .keys()
            .filter(|number| **number < self.chunk - 1 && !live.contains(number))
            .copied()
            .collect();
        for number in gone {
            let chunk = Chunk {
                load: self.owner.load,
                number,
            };
            self.store
                .remove(&self.owner.pipeline, chunk)
                .await
                .map_err(Error::from_wal)?;
            if let Some(written) = self.written.remove(&number) {
                self.shared.held.fetch_sub(written.len, Ordering::Relaxed);
            }
            crash_point!("engine.wal.remove");
        }
        self.settled.forget_unwritten(&self.written);
        Ok(())
    }

    /// Whether a replay needs `written`, a published chunk: a segment in it is neither settled
    /// nor carried to a later chunk, or a commit in it waits for its receipt.
    fn needed(&self, written: &Written) -> bool {
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
        if self.written.is_empty() {
            return Ok(());
        }
        self.append(Frame::Closed.encode()?).await?;
        crash_point!("engine.wal.close.before");
        self.publish().await?;
        crash_point!("engine.wal.close.after");
        if !self.pending.is_empty() {
            return Ok(());
        }
        self.written.clear();
        self.store
            .remove_log(&self.owner.pipeline, self.owner.load)
            .await
            .map_err(Error::from_wal)?;
        self.shared.held.store(0, Ordering::Relaxed);
        crash_point!("engine.wal.removed");
        Ok(())
    }
}
