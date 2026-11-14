//! A source whose reads run a window of rows ahead of the commits of the load that reads them,
//! until the load has started the last commit a kill point counts: every point then finds rows
//! still to write, however slowly its destination commits.

#[cfg(test)]
mod tests;

use std::num::NonZeroUsize;
use std::sync::Arc;

use rdlt_connector::{
    BoxFuture, Catalog, Cursor, PartitionFeed, PartitionId, PartitionPlan, PartitionSink, Push,
    ReadRequest, Requested, Result, Source, SourceEvent, StreamName, StreamState,
    partition_channel,
};
use tokio::sync::watch;

use super::COMMIT_ROWS;

/// Rows a read forwards of its partition after a commit starts, before it holds its next push
/// until the next starts: at least what makes a commit due, so the rows of a holding read, which
/// the commit in flight did not take, always make the next one due.
const WINDOW: u64 = 2 * COMMIT_ROWS;

/// Where a paced load stands: the commits it has started while its reads are paced, and its reads.
///
/// The reads are counted, relaying and holding, so a test can tell when every read holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Pace {
    /// The commits started; none once the reads are paced no more.
    started: Option<u64>,
    /// The reads relaying.
    relaying: usize,
    /// The reads holding a push since the latest commit started.
    holding: usize,
}

impl Default for Pace {
    /// No commit started, and no read.
    fn default() -> Self {
        Self {
            started: Some(0),
            relaying: 0,
            holding: 0,
        }
    }
}

impl Pace {
    /// Notes the `commit`th commit started, which releases every held push, pacing the reads
    /// only while `commit` is before `last`.
    pub(super) fn start(&mut self, commit: u64, last: u64) {
        self.started = (commit < last).then_some(commit);
        self.holding = 0;
    }

    /// Whether the reads can send nothing more until a commit starts: each holds a push.
    #[cfg(test)]
    pub(super) fn stalled(&self) -> bool {
        self.holding == self.relaying
    }

    /// Counts a push held since the `window`th commit started, where that is the latest.
    fn held(&mut self, window: u64) {
        if self.started == Some(window) {
            self.holding += 1;
        }
    }

    /// Counts a push held since the `window`th commit started released, where that is the
    /// latest: a later start released it already.
    fn released(&mut self, window: u64) {
        if self.started == Some(window) {
            self.holding -= 1;
        }
    }
}

/// `source`, each read of which holds a push once it has forwarded a [`WINDOW`] of rows since it
/// started or the latest commit did, until the next commit starts, as `pace` tells.
///
/// A read holds only a push that follows a checkpoint, so a barrier it owes while it holds is
/// answered by sending that checkpoint again; a read whose pushes follow no checkpoint is never
/// held.
pub(super) struct Paced {
    source: Arc<dyn Source>,
    pace: Arc<watch::Sender<Pace>>,
}

impl Paced {
    pub(super) fn new(source: Arc<dyn Source>, pace: Arc<watch::Sender<Pace>>) -> Self {
        Self { source, pace }
    }
}

impl Source for Paced {
    fn check(&self) -> BoxFuture<'_, Result<()>> {
        self.source.check()
    }

    fn discover(&self) -> BoxFuture<'_, Result<Catalog>> {
        self.source.discover()
    }

    fn plan<'a>(
        &'a self,
        stream: &'a StreamName,
        state: &'a StreamState,
    ) -> BoxFuture<'a, Result<PartitionPlan>> {
        self.source.plan(stream, state)
    }

    fn read(&self, request: ReadRequest, sink: PartitionSink) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let (inner, feed) = partition_channel(NonZeroUsize::MIN);
            let relay = Relay::new(feed, sink, &self.pace);
            let (read, relayed) = tokio::join!(self.source.read(request, inner), relay.run());
            read.and(relayed)
        })
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, Result<()>> {
        self.source.committed(stream, cursors)
    }

    fn max_calls(&self) -> Option<NonZeroUsize> {
        self.source.max_calls()
    }
}

/// A push held until the next commit starts.
struct Held {
    push: SourceEvent,
    /// The commits started when it was held, so a test can tell when every read holds.
    window: u64,
}

/// One read's events, relayed from the source's end of its channel to the engine's.
struct Relay<'a> {
    feed: PartitionFeed,
    sink: PartitionSink,
    pace: &'a watch::Sender<Pace>,
    started: watch::Receiver<Pace>,
    /// The commits started when the window last opened; none once the read is paced no more.
    window: Option<u64>,
    /// Rows forwarded since the window last opened.
    rows: u64,
    /// The cursor of the event forwarded last, where it is a checkpoint: where a held push waits.
    at: Option<Cursor>,
    held: Option<Held>,
    /// The newest barrier the engine asked for that was passed on or answered.
    forwarded: u64,
}

impl<'a> Relay<'a> {
    /// The relay of `feed` to `sink`, counted in `pace` until it drops, so a test can tell when
    /// every read holds.
    fn new(feed: PartitionFeed, sink: PartitionSink, pace: &'a watch::Sender<Pace>) -> Self {
        pace.send_modify(|pace| pace.relaying += 1);
        let mut started = pace.subscribe();
        let window = started.borrow_and_update().started;
        Self {
            feed,
            sink,
            pace,
            started,
            window,
            rows: 0,
            at: None,
            held: None,
            forwarded: 0,
        }
    }

    /// Relays until the source's read has sent everything, sending nothing more once the engine
    /// stops the read.
    async fn run(mut self) -> Result<()> {
        let mut stopping = false;
        loop {
            if self.holding().is_none()
                && let Some(held) = self.release()
            {
                self.relay(held.push).await?;
            }
            tokio::select! {
                biased;
                // The engine's requests win: a stop ends the read, and a barrier a holding read
                // owes is answered at once.
                requested = self.sink.requested(self.forwarded), if !stopping => match requested {
                    Requested::Stop => {
                        stopping = true;
                        self.feed.stop();
                        self.release();
                    }
                    Requested::Checkpoint(barrier) => self.barrier(barrier).await?,
                },
                changed = self.started.changed(), if self.window.is_some() => {
                    let started = match changed {
                        Ok(()) => self.started.borrow_and_update().started,
                        Err(_) => None,
                    };
                    if started != self.window {
                        self.window = started;
                        self.rows = 0;
                    }
                }
                event = self.feed.recv(), if self.held.is_none() => match event {
                    None => return Ok(()),
                    Some(_) if stopping => {}
                    Some(event) => self.relay(event).await?,
                },
            }
        }
    }

    /// The window the read holds its next push in: its own, while it is paced, the event it
    /// forwarded last is a checkpoint, and it forwarded a window since the window opened.
    fn holding(&self) -> Option<u64> {
        self.window
            .filter(|_| self.at.is_some() && self.rows >= WINDOW)
    }

    /// Forwards `event`, or holds it where it is a push the read holds.
    async fn relay(&mut self, event: SourceEvent) -> Result<()> {
        if let SourceEvent::Push(_) = &event
            && let Some(window) = self.holding()
        {
            return self.hold(event, window).await;
        }
        match &event {
            SourceEvent::Push(push) => {
                self.rows = self.rows.saturating_add(rows(push));
                self.at = None;
            }
            SourceEvent::Checkpoint { cursor, .. } => self.at = Some(cursor.clone()),
            SourceEvent::Replan | SourceEvent::Behind { .. } => {}
        }
        self.sink.send(event).await
    }

    /// Holds `push` in `window`, answering a barrier the engine is waiting for.
    async fn hold(&mut self, push: SourceEvent, window: u64) -> Result<()> {
        self.pace.send_modify(|pace| pace.held(window));
        self.held = Some(Held { push, window });
        match self.sink.pending_barrier() {
            Some(barrier) => self.answer(barrier).await,
            None => Ok(()),
        }
    }

    /// The push held, no longer held.
    fn release(&mut self) -> Option<Held> {
        let held = self.held.take()?;
        self.pace.send_modify(|pace| pace.released(held.window));
        Some(held)
    }

    /// Passes `barrier` on to the source, or answers it where the read holds a push.
    async fn barrier(&mut self, barrier: u64) -> Result<()> {
        self.forwarded = barrier;
        if self.held.is_some() {
            self.answer(barrier).await
        } else {
            self.feed.request_checkpoint(barrier);
            Ok(())
        }
    }

    /// Answers `barrier` where the held push waits: at the checkpoint forwarded last.
    async fn answer(&mut self, barrier: u64) -> Result<()> {
        let Some(cursor) = self.at.clone() else {
            return Ok(());
        };
        let checkpoint = SourceEvent::Checkpoint {
            cursor,
            answers: Some(barrier),
        };
        self.sink.send(checkpoint).await
    }
}

impl Drop for Relay<'_> {
    /// Counts the read gone, so a test can tell when every read holds.
    fn drop(&mut self) {
        self.release();
        self.pace.send_modify(|pace| pace.relaying -= 1);
    }
}

/// The rows `push` holds, where it says without decoding: a JSON push says none.
fn rows(push: &Push) -> u64 {
    match push {
        Push::Arrow(batch) | Push::Changes(batch) => {
            u64::try_from(batch.num_rows()).unwrap_or(u64::MAX)
        }
        Push::Json(_) => 0,
    }
}
