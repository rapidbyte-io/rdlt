//! The barrier a commit raises: every reading on-demand partition asked to checkpoint first.

use super::{Coordinator, cancelled};
use crate::error::Error;

impl Coordinator {
    /// Asks every reading on-demand partition to checkpoint, and waits until each has answered,
    /// ended, or `barrier_wait` has passed.
    pub(super) async fn raise_barrier(&mut self) -> Result<(), Error> {
        self.barrier += 1;
        let barrier = self.barrier;
        self.parts.barrier.send_replace(barrier);
        let mut deadline = self.parts.env.sleep(self.parts.barrier_wait);
        // Kept as partitions start, answer and end, so the wait looks at no partition twice.
        self.owing = (0..self.parts.partitions.len())
            .filter(|index| self.parts.partitions[*index].owes(barrier))
            .collect();
        while !self.owing.is_empty() {
            tokio::select! {
                biased;
                () = self.parts.cancel.cancelled() => return Err(cancelled()),
                // Once the wait is over, the commit takes whatever is sealed, and those that did
                // not answer are not asked again until they write more.
                () = &mut deadline => {
                    for partition in std::mem::take(&mut self.owing) {
                        self.due.unanswered(partition);
                    }
                }
                // An answer that finds no room for its cursor waits for this commit: the commit
                // takes whatever is sealed now, and frees the room. Those still owing may be
                // that answer, so they are asked again.
                () = self.parts.budget.cursor_waits() => break,
                progress = self.parts.progress.recv() => self.observe(progress.ok_or_else(cancelled)?),
            }
        }
        Ok(())
    }
}
