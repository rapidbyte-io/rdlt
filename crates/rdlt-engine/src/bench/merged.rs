//! A merge of hot keys: orders whose ids a Zipf distribution draws, merged by id and normalized
//! into their table and child tables, pushed a flush at a time through the engine into the IPC
//! sink, in this process or served from another.

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::fmt;
use std::num::NonZeroU64;
use std::sync::Arc;

use bytes::Bytes;
use rdlt_connector::{ConnectContext, Destination, Source};

use super::corpora::orders_identified;
use super::runner::{Runner, stream};
use super::{Mix, Replayed, Sinking, Zipf, replay, sink_factory, staged};
use crate::{ComputePoolError, Cores, Nested, Report, SchemaSettings, WriteMode};

/// Orders whose ids a Zipf distribution draws, and an engine to merge them by id.
#[derive(Debug)]
pub struct Merged {
    runner: Runner,
    pushes: Vec<Bytes>,
    keys: u64,
}

/// What a merge's commits staged: its stream's table's rows, its child tables' rows, the
/// distinct keys of the rows its pushes held, and the commits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Staging {
    /// Rows staged in the stream's table.
    pub roots: u64,
    /// Rows staged in its child tables.
    pub children: u64,
    /// Distinct keys pushed.
    pub keys: u64,
    /// Commits made.
    pub commits: u64,
}

impl fmt::Display for Staging {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        #[expect(clippy::cast_precision_loss, reason = "counts far below 2^52")]
        let each = |count: u64| count as f64 / self.commits.max(1) as f64;
        write!(
            f,
            "staged root rows {:.0}, staged child rows {:.0} and distinct keys {:.0} a commit",
            each(self.roots),
            each(self.children),
            each(self.keys)
        )
    }
}

impl Merged {
    /// Rows each run merges.
    pub const ROWS: u64 = 200_000;
    /// Rows a push holds, each push a flush.
    pub const PUSH_ROWS: NonZeroU64 = NonZeroU64::new(50_000).expect("not zero");
    /// Keys the ids are drawn from.
    pub const KEYS: NonZeroU64 = NonZeroU64::new(100_000).expect("not zero");
    /// The skew of the ids' Zipf distribution.
    pub const THETA: f64 = 0.99;

    /// The first `rows` orders, their ids drawn, and a runtime of `cores`' workers and an engine
    /// within `cores` to merge them, committing once at the end.
    ///
    /// # Errors
    ///
    /// When the compute pool's threads do not start.
    ///
    /// # Panics
    ///
    /// Panics where the runtime does not start.
    pub fn try_new(cores: Cores, rows: u64) -> Result<Self, ComputePoolError> {
        let stream = stream()
            .write(WriteMode::Merge)
            .key(["id"])
            .schema(SchemaSettings::new().nested(Nested::normalize()));
        let (pushes, keys) = orders(rows);
        Ok(Self {
            runner: Runner::try_new(cores, "merged", stream, None)?,
            pushes,
            keys,
        })
    }

    /// The pushes of the first `rows` orders, their ids drawn: what a run of that many rows
    /// replays, made the same in any process.
    pub fn pushes(rows: u64) -> Vec<Bytes> {
        orders(rows).0
    }

    /// The distinct ids of the rows a run pushes.
    pub fn keys(&self) -> u64 {
        self.keys
    }

    /// The bytes of JSON each run pushes.
    pub fn bytes(&self) -> u64 {
        self.pushes.iter().map(|push| push.len() as u64).sum()
    }

    /// Runs `future` on the merge's runtime, where connectors served from this process run.
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.runner.block_on(future)
    }

    /// Runs the engine from a source replaying the pushes into the IPC sink, which tallies what
    /// it stages; its report, and what its commits staged.
    ///
    /// # Panics
    ///
    /// Panics where the run fails or commits more than once.
    pub fn run(&self) -> (Report, Staging) {
        let (source, destination) = self.block_on(async {
            let source = replay("merged", Replayed::Json(self.pushes.clone())).await;
            let config = Sinking::Ipc.tallied("merged");
            let sink = sink_factory().connect(config, ConnectContext::new()).await;
            let destination: Arc<dyn Destination> = Arc::from(sink.expect("the sink connects"));
            (source, destination)
        });
        staged("merged");
        let report = self.load(source, destination);
        let tallied = staged("merged");
        let roots = tallied.rows.get("events").copied().unwrap_or(0);
        let staging = Staging {
            roots,
            children: tallied.rows.values().sum::<u64>() - roots,
            keys: self.keys,
            commits: tallied.commits,
        };
        (report, staging)
    }

    /// Runs the engine from `source`, which pushes the orders, into `destination`; its report.
    ///
    /// # Panics
    ///
    /// Panics where the run fails or commits more than once.
    pub fn load(&self, source: Arc<dyn Source>, destination: Arc<dyn Destination>) -> Report {
        let report = self.runner.load(source, destination);
        assert_eq!(report.commits, 1, "a merge commits once, at its end");
        report
    }
}

/// The seed of the orders' ids.
const SEED: u64 = 13;

/// The first `rows` orders, [`Merged::PUSH_ROWS`] a push, their ids drawn from [`Merged::KEYS`]
/// keys by a Zipf distribution of [`Merged::THETA`]; and how many distinct ids they hold.
fn orders(rows: u64) -> (Vec<Bytes>, u64) {
    let zipf = Zipf::new(Merged::KEYS, Merged::THETA);
    let mut mix = Mix::new(SEED);
    let mut keys = BTreeSet::new();
    let pushes = orders_identified(rows, Merged::PUSH_ROWS, || {
        let key = zipf.draw(&mut mix);
        keys.insert(key);
        key
    });
    (
        pushes,
        u64::try_from(keys.len()).expect("a count fits in 64 bits"),
    )
}
