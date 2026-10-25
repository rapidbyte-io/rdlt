//! What a run's report says of where its time went, as the passthrough and served benches print
//! it beside criterion's times.

#[cfg(test)]
mod tests;

use std::fmt;
use std::num::NonZeroUsize;
use std::time::Duration;

use crate::report::{Commits, Counters, Flushes};

/// A run's counters as shares of its wall time and counts a batch.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Readings {
    /// Commits by what made each due.
    pub commits: Commits,
    /// Requests for lowering waiting for room, their time summed, over the run's.
    pub work: f64,
    /// Pushes waiting for room, their time summed, over the run's.
    pub intake: f64,
    /// Frames of served reads waiting for room to be decoded, their time summed, over the run's.
    pub decode: f64,
    /// The compute pool's threads' time running no job, over theirs.
    pub idle: f64,
    /// Jobs' waits to start and ways back to their callers, summed, over the run's time.
    pub hops: f64,
    /// Lanes that wrote at least once.
    pub lanes: usize,
    /// The lanes' flushes of their writers, by what asked for each.
    pub flushes: Flushes,
    /// The budget's signals a batch.
    pub signals: f64,
}

/// `part` over `whole`, or nothing of nothing.
fn share(part: f64, whole: f64) -> f64 {
    if whole > 0.0 { part / whole } else { 0.0 }
}

impl Readings {
    /// The readings of a run of `batches` batches that took `elapsed`, counted `counters` and
    /// had a compute pool of `threads` threads.
    #[expect(
        clippy::cast_precision_loss,
        reason = "counts and times far below 2^52"
    )]
    pub fn of(elapsed: Duration, counters: &Counters, batches: u64, threads: NonZeroUsize) -> Self {
        let run = elapsed.as_secs_f64();
        let pool = &counters.pool;
        let busy = share(pool.running.as_secs_f64(), threads.get() as f64 * run);
        Self {
            commits: counters.commits,
            work: share(counters.waits.work.time.as_secs_f64(), run),
            intake: share(counters.waits.intake.time.as_secs_f64(), run),
            decode: share(counters.waits.decode.time.as_secs_f64(), run),
            idle: if run > 0.0 {
                (1.0 - busy).max(0.0)
            } else {
                0.0
            },
            hops: share((pool.queued + pool.returned).as_secs_f64(), run),
            lanes: counters
                .lanes
                .iter()
                .filter(|lane| !lane.writing.is_zero())
                .count(),
            flushes: counters
                .lanes
                .iter()
                .fold(Flushes::default(), |mut all, lane| {
                    all.add(&lane.flushes);
                    all
                }),
            signals: share(counters.signals as f64, batches as f64),
        }
    }
}

impl fmt::Display for Readings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (c, flushes) = (&self.commits, &self.flushes);
        let commits = c.interval + c.size + c.cursors + c.end;
        let writer_flushes = flushes.pressure + flushes.commit + flushes.retired;
        write!(
            f,
            "commits {commits} (interval {}, size {}, cursors {}, end {}), work waits \
             {:.1} % of the run, intake waits {:.1} %, decode waits {:.1} %, pool idle {:.1} %, \
             pool queue and way back {:.1} %, lanes written {}, writer flushes {writer_flushes} \
             (pressure {}, commit {}, retired {}), signals {:.1} a batch",
            c.interval,
            c.size,
            c.cursors,
            c.end,
            self.work * 100.0,
            self.intake * 100.0,
            self.decode * 100.0,
            self.idle * 100.0,
            self.hops * 100.0,
            self.lanes,
            flushes.pressure,
            flushes.commit,
            flushes.retired,
            self.signals,
        )
    }
}
