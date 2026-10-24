//! The normalized write path: keyless nested JSON pushed a flush at a time through the engine,
//! each flush shredded into units, normalized into its table and child tables, and lowered into a
//! destination that discards what it stages; read from one partition, or from many through as
//! many writers as the passthrough workload's.

#[cfg(test)]
mod tests;

use std::num::{NonZeroU64, NonZeroUsize};

use bytes::Bytes;

use super::replay::connect_replay;
use super::runner::{Runner, stream};
use super::{
    CHUNK_BYTES, Corpus, Passthrough, Replayed, Sinking, null_sink, register, sink,
    split_replay_config,
};
use crate::{ComputePoolError, Cores, Nested, Report, SchemaSettings};

/// The rows of the orders corpus pushed `per_push` a push, normalized through the engine.
#[derive(Debug)]
pub struct Normalized {
    runner: Runner,
    pushes: Vec<Bytes>,
    roots: u64,
    partitions: NonZeroUsize,
}

impl Normalized {
    /// Rows of the stream each run normalizes into three tables.
    pub const ROOTS: u64 = 100_000;
    /// Rows a push holds in each case: a chunk's worth, so each flush is one unit, and pushes of
    /// several chunks.
    pub const PUSH_ROWS: [NonZeroU64; 3] = [
        NonZeroU64::new(2_000).expect("not zero"),
        NonZeroU64::new(10_000).expect("not zero"),
        NonZeroU64::new(50_000).expect("not zero"),
    ];
    /// Rows of the stream each run read from several partitions normalizes.
    pub const SPLIT_ROOTS: u64 = 200_000;
    /// Rows a push holds in the runs read from several partitions.
    pub const SPLIT_PUSH_ROWS: NonZeroU64 = NonZeroU64::new(10_000).expect("not zero");
    /// The partitions of the runs read from several.
    pub const PARTITIONS: [NonZeroUsize; 2] = [
        NonZeroUsize::new(4).expect("not zero"),
        NonZeroUsize::new(16).expect("not zero"),
    ];
    /// Bytes: the budgets the runs read from several partitions load within, the default and
    /// four times it.
    pub const BUDGETS: [u64; 2] = [256 << 20, 1 << 30];

    /// The first `roots` rows of the orders corpus in pushes of `per_push` rows, a checkpoint
    /// after each so each push is one flush, and a runtime of `cores`' workers and an engine
    /// within `cores` to load them.
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
        roots: u64,
        per_push: NonZeroU64,
    ) -> Result<Self, ComputePoolError> {
        Self::within(cores, roots, per_push, Runner::MEMORY)
    }

    /// As [`Normalized::try_new`], the engine within a budget of `memory` bytes.
    ///
    /// # Errors
    ///
    /// When the compute pool's threads do not start.
    ///
    /// # Panics
    ///
    /// Panics where the runtime does not start.
    pub fn within(
        cores: Cores,
        roots: u64,
        per_push: NonZeroU64,
        memory: u64,
    ) -> Result<Self, ComputePoolError> {
        let stream = stream().schema(SchemaSettings::new().nested(Nested::normalize()));
        Ok(Self {
            runner: Runner::within(cores, "normalized", vec![stream], None, memory)?,
            pushes: Corpus::Orders.rows(roots, per_push),
            roots,
            partitions: NonZeroUsize::MIN,
        })
    }

    /// The load, its pushes read from `partitions` partitions, each an even share in order, into
    /// a sink of as many writers as the passthrough workload's.
    #[must_use]
    pub fn partitioned(self, partitions: NonZeroUsize) -> Self {
        Self { partitions, ..self }
    }

    /// The partitions the pushes are read from.
    pub fn partitions(&self) -> NonZeroUsize {
        self.partitions
    }

    /// The bytes of JSON each run loads.
    ///
    /// # Panics
    ///
    /// Panics where the count does not fit in 64 bits, which happens on no platform Rust supports.
    pub fn bytes(&self) -> u64 {
        let bytes: usize = self.pushes.iter().map(Bytes::len).sum();
        u64::try_from(bytes).expect("a length fits in 64 bits")
    }

    /// The bytes of the largest push.
    ///
    /// # Panics
    ///
    /// Panics where the count does not fit in 64 bits, which happens on no platform Rust supports.
    pub fn largest_push(&self) -> u64 {
        let bytes = self.pushes.iter().map(Bytes::len).max().unwrap_or(0);
        u64::try_from(bytes).expect("a length fits in 64 bits")
    }

    /// The rows each run loads into the three tables.
    pub fn rows(&self) -> u64 {
        Self::loaded(self.roots)
    }

    /// The rows loading the first `roots` rows of the orders corpus makes in the three tables:
    /// each root, its items and their tags.
    pub fn loaded(roots: u64) -> u64 {
        // Item counts cycle 0, 1, 2, 3 over the roots' ids; each item has two tags.
        let items = roots / 4 * 6 + (0..roots % 4).sum::<u64>();
        roots + 3 * items
    }

    /// The fewest and the most units a flush is cut into: one per chunk of JSON.
    ///
    /// # Panics
    ///
    /// Panics where the corpus does not cut, which it always does.
    pub fn units(&self) -> (usize, usize) {
        let units = self.pushes.iter().map(|push| {
            crate::shred::chunked(std::slice::from_ref(push), CHUNK_BYTES)
                .expect("the corpus cuts into chunks")
        });
        units.fold((usize::MAX, 0), |(fewest, most), units| {
            (fewest.min(units), most.max(units))
        })
    }

    /// Runs the engine from a source replaying the pushes into a sink that discards them; the
    /// rows it reports.
    ///
    /// # Panics
    ///
    /// Panics where the run fails or loads other rows than [`Normalized::rows`].
    pub fn run(&self) -> u64 {
        self.report().rows
    }

    /// Runs the engine as [`Normalized::run`] does; its report.
    ///
    /// # Panics
    ///
    /// Panics where the run fails or loads other rows than [`Normalized::rows`].
    pub fn report(&self) -> Report {
        let (source, destination) = self.runner.block_on(async {
            register("normalized", Replayed::Json(self.pushes.clone()));
            let config = split_replay_config("normalized", self.partitions, NonZeroUsize::MIN);
            let source = connect_replay(config).await;
            let destination = if self.partitions == NonZeroUsize::MIN {
                null_sink().await
            } else {
                sink(Sinking::Discard, Passthrough::WRITERS).await
            };
            (source, destination)
        });
        let report = self.runner.load(source, destination);
        assert_eq!(report.rows, self.rows());
        report
    }
}
