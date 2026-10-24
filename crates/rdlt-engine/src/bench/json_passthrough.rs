//! JSON through the engine: a corpus's pushes of about the default coalescing target, a
//! checkpoint after each, read from one partition or from many, shredded and loaded into a sink
//! that discards them.

#[cfg(test)]
mod tests;

use std::num::{NonZeroU64, NonZeroUsize};

use bytes::Bytes;

use super::replay::connect_replay;
use super::runner::{Runner, streams};
use super::{
    Corpus, PUSH_BYTES, Passthrough, Replayed, Sinking, register, sink, split_replay_config,
};
use crate::{ComputePoolError, Cores, EngineConfig, Report};

/// One run's shape: the corpus it pushes and the partitions it reads them from, each an even
/// share of the pushes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JsonCell {
    /// The corpus pushed.
    pub corpus: Corpus,
    /// Partitions of its stream.
    pub partitions: NonZeroUsize,
}

impl JsonCell {
    /// Pushes a run makes: one for each partition of the most a cell reads from.
    pub const PUSHES: usize = 16;

    /// Every cell: nested rows and rows of 200 columns, each from one partition and from
    /// sixteen.
    pub fn all() -> Vec<Self> {
        let partitions = [NonZeroUsize::MIN, NonZeroUsize::new(16).expect("not zero")];
        [Corpus::Nested, Corpus::Wide(200)]
            .into_iter()
            .flat_map(|corpus| partitions.map(|partitions| Self { corpus, partitions }))
            .collect()
    }

    /// The cell's name in a benchmark's: `<corpus>/<partitions>p`.
    pub fn name(self) -> String {
        format!("{}/{}p", self.corpus.name(), self.partitions)
    }

    /// The pushes a timed run makes: [`JsonCell::PUSHES`] of the corpus's rows, of whole lines
    /// of [`PUSH_BYTES`] or a little more, the last of what is left.
    pub fn pushes(self) -> Vec<Bytes> {
        self.corpus.pushes(Self::PUSHES * PUSH_BYTES)
    }

    /// The pushes a run as a test makes: two of a hundred rows for each partition.
    pub fn tested(self) -> Vec<Bytes> {
        let pushes = u64::try_from(2 * self.partitions.get()).expect("pushes fit in 64 bits");
        self.corpus
            .rows(100 * pushes, NonZeroU64::new(100).expect("not zero"))
    }
}

/// A cell's JSON pushes, and an engine within the default budget to load them.
#[derive(Debug)]
pub struct JsonPassthrough {
    runner: Runner,
    cell: JsonCell,
    pushes: Vec<Bytes>,
    rows: u64,
}

impl JsonPassthrough {
    /// The JSON `pushes` read as `cell` reads them, and a runtime of `cores`' workers and an
    /// engine within `cores` and the default budget to load them, committing once at the end.
    ///
    /// # Errors
    ///
    /// When the compute pool's threads do not start.
    ///
    /// # Panics
    ///
    /// Panics where the runtime does not start.
    pub fn try_new(
        cores: Cores,
        cell: JsonCell,
        pushes: Vec<Bytes>,
    ) -> Result<Self, ComputePoolError> {
        let memory = EngineConfig::default().memory().get();
        let runner = Runner::within(cores, "json", streams(NonZeroUsize::MIN), None, memory)?;
        let rows = pushes
            .iter()
            .map(|push| memchr::memchr_iter(b'\n', push).count() as u64)
            .sum();
        Ok(Self {
            runner,
            cell,
            pushes,
            rows,
        })
    }

    /// The rows each run loads: the pushes' lines.
    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// The bytes of JSON each run pushes.
    pub fn bytes(&self) -> u64 {
        self.pushes.iter().map(|push| push.len() as u64).sum()
    }

    /// Runs the engine from a source replaying the pushes into a sink that discards them,
    /// through as many writers at once as the passthrough workload's; its report.
    ///
    /// # Panics
    ///
    /// Panics where the run fails or loads other rows than the pushes hold.
    pub fn run(&self) -> Report {
        let (source, destination) = self.runner.block_on(async {
            register("json", Replayed::Json(self.pushes.clone()));
            let config = split_replay_config("json", self.cell.partitions, NonZeroUsize::MIN);
            let source = connect_replay(config).await;
            (source, sink(Sinking::Discard, Passthrough::WRITERS).await)
        });
        let report = self.runner.load(source, destination);
        assert_eq!(report.rows, self.rows);
        report
    }
}
