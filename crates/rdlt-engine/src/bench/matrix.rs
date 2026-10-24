//! The passthrough matrix: the same rows moved in batches of three sizes, from one to sixteen
//! partitions of one or four streams, committed at the end or every sixteenth of the rows, the
//! small batches also checkpointed only every so many pushes.

#[cfg(test)]
mod tests;

use std::num::{NonZeroU64, NonZeroUsize};

use serde_json::Value;

use super::replay::checkpointing;
use super::{Sinking, split_replay_config};

/// When a cell's run commits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cadence {
    /// At no threshold: only the end of the read, or the budget, makes a commit due.
    End,
    /// Every sixteenth of the run's rows.
    Sixteenths,
}

/// One run's shape: its batches, the partitions and streams they are read from, how often its
/// source checkpoints, when it commits and what its sink does with each batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cell {
    /// Rows a batch.
    pub rows: u32,
    /// Batches a run.
    pub batches: u32,
    /// Partitions of each stream, each an even share of the batches.
    pub partitions: NonZeroUsize,
    /// Streams the batches are read from.
    pub streams: NonZeroUsize,
    /// Pushes of a partition between its source's checkpoints; the last push of each partition
    /// is checkpointed whatever its place.
    pub checkpoints: NonZeroUsize,
    /// When the run commits.
    pub commit: Cadence,
    /// What the sink does with each batch.
    pub sinking: Sinking,
}

/// `count` as a count that cannot be zero, one where it is.
pub(super) const fn nonzero(count: usize) -> NonZeroUsize {
    match NonZeroUsize::new(count) {
        Some(count) => count,
        None => NonZeroUsize::MIN,
    }
}

impl Cell {
    /// Rows every cell of the matrix moves a run.
    pub const RUN_ROWS: u32 = 5_120_000;
    /// Rows a batch, by cell.
    pub const ROWS: [u32; 3] = [1_000, 8_000, 80_000];
    /// Partitions of each stream, by cell.
    pub const PARTITIONS: [NonZeroUsize; 3] = [nonzero(1), nonzero(4), nonzero(16)];
    /// Streams, by cell.
    pub const STREAMS: [NonZeroUsize; 2] = [nonzero(1), nonzero(4)];
    /// Pushes between checkpoints of the cells whose small batches a checkpoint does not end one
    /// by one: about as many as fill a sixteenth of the default coalescing target, and as many
    /// as fill all of it.
    pub const CHECKPOINTS: [NonZeroUsize; 2] = [nonzero(6), nonzero(97)];
    /// The cell measured against a bare loop in pairs: the 64 largest batches from one
    /// partition of one stream, committed at the end, into the IPC sink.
    pub const PAIRED: Self = Self::of(80_000, nonzero(1), nonzero(1), Cadence::End, Sinking::Ipc);

    /// The cell of [`Cell::RUN_ROWS`] rows in batches of `rows`, read from `partitions` of each
    /// of `streams`, committed at `commit`, its sink doing `sinking`.
    pub const fn of(
        rows: u32,
        partitions: NonZeroUsize,
        streams: NonZeroUsize,
        commit: Cadence,
        sinking: Sinking,
    ) -> Self {
        Self {
            rows,
            batches: Self::RUN_ROWS / rows,
            partitions,
            streams,
            checkpoints: NonZeroUsize::MIN,
            commit,
            sinking,
        }
    }

    /// The cell, its source checkpointing each partition after every `every`-th push and after
    /// its last.
    #[must_use]
    pub const fn checkpointed(self, every: NonZeroUsize) -> Self {
        Self {
            checkpoints: every,
            ..self
        }
    }

    /// The cells timed unless the whole matrix is asked for: the paired cell, each axis moved
    /// alone from it, the small batches at sixteen partitions and into the null sink, and the
    /// small batches of one and of sixteen partitions checkpointed every so many pushes.
    pub fn chosen() -> Vec<Self> {
        let (one, four, sixteen) = (nonzero(1), nonzero(4), nonzero(16));
        let (end, sixteenths) = (Cadence::End, Cadence::Sixteenths);
        let (ipc, null) = (Sinking::Ipc, Sinking::Discard);
        let mut cells = vec![
            Self::PAIRED,
            Self::of(8_000, one, one, end, ipc),
            Self::of(1_000, one, one, end, ipc),
            Self::of(80_000, four, one, end, ipc),
            Self::of(80_000, sixteen, one, end, ipc),
            Self::of(80_000, one, four, end, ipc),
            Self::of(80_000, one, one, sixteenths, ipc),
            Self::of(8_000, sixteen, one, end, ipc),
            Self::of(1_000, sixteen, one, end, ipc),
            Self::of(1_000, sixteen, one, sixteenths, ipc),
            Self::of(1_000, one, one, end, null),
            Self::of(8_000, one, one, end, null),
        ];
        cells.extend(Self::checkpointed_cells());
        cells
    }

    /// The small batches of one and of sixteen partitions into the IPC sink, committed at the
    /// end, checkpointed every so many pushes of [`Cell::CHECKPOINTS`].
    fn checkpointed_cells() -> impl Iterator<Item = Self> {
        [nonzero(1), nonzero(16)]
            .into_iter()
            .flat_map(|partitions| {
                let cell = Self::of(1_000, partitions, nonzero(1), Cadence::End, Sinking::Ipc);
                Self::CHECKPOINTS.map(|every| cell.checkpointed(every))
            })
    }

    /// Every cell: each size of batch from each count of partitions of each count of streams at
    /// each cadence into the IPC sink, then the small batches of one partition into the null
    /// sink, then the small batches checkpointed every so many pushes.
    pub fn full() -> Vec<Self> {
        let mut cells = Vec::new();
        for rows in Self::ROWS {
            for partitions in Self::PARTITIONS {
                for streams in Self::STREAMS {
                    for commit in [Cadence::End, Cadence::Sixteenths] {
                        cells.push(Self::of(rows, partitions, streams, commit, Sinking::Ipc));
                    }
                }
            }
        }
        let one = NonZeroUsize::MIN;
        cells.extend(
            [1_000, 8_000].map(|rows| Self::of(rows, one, one, Cadence::End, Sinking::Discard)),
        );
        cells.extend(Self::checkpointed_cells());
        cells
    }

    /// The cells a bench run with `args` times, and the group they are timed in: every cell, as
    /// `full`, where an argument after the program's path names `full`, as an exact benchmark's
    /// name does; the chosen ones, as `matrix`, otherwise.
    pub fn asked(args: &[String]) -> (&'static str, Vec<Self>) {
        if args.iter().skip(1).any(|arg| arg.contains("full")) {
            ("full", Self::full())
        } else {
            ("matrix", Self::chosen())
        }
    }

    /// The cell's shape as a test runs it: two batches of at most a thousand rows for each
    /// partition of each stream.
    #[must_use]
    pub fn tested(self) -> Self {
        let units = self.partitions.get().saturating_mul(self.streams.get());
        Self {
            rows: self.rows.min(1_000),
            batches: u32::try_from(units.saturating_mul(2)).unwrap_or(u32::MAX),
            ..self
        }
    }

    /// Rows a commit waits for: none at the end, a sixteenth of the run's rows otherwise.
    pub fn commit_rows(self) -> Option<NonZeroU64> {
        match self.commit {
            Cadence::End => None,
            Cadence::Sixteenths => {
                let rows = u64::from(self.rows) * u64::from(self.batches) / 16;
                NonZeroU64::new(rows.max(1))
            }
        }
    }

    /// The configuration of a source replaying the cell's batches registered as `name`.
    pub fn replay_config(self, name: &str) -> Value {
        checkpointing(
            split_replay_config(name, self.partitions, self.streams),
            self.checkpoints,
        )
    }

    /// The cell's name in a benchmark's: `<rows>x<batches>/<partitions>p/<streams>s/<cadence>`,
    /// then `/null` for the null sink, then `/k<pushes>` where checkpoints are that many pushes
    /// apart.
    pub fn name(self) -> String {
        let commit = match self.commit {
            Cadence::End => "end",
            Cadence::Sixteenths => "16th",
        };
        let sink = match self.sinking {
            Sinking::Ipc => "",
            Sinking::Discard => "/null",
            Sinking::Refuse => "/refuse",
        };
        let checkpoints = match self.checkpoints.get() {
            1 => String::new(),
            every => format!("/k{every}"),
        };
        format!(
            "{}x{}/{}p/{}s/{commit}{sink}{checkpoints}",
            self.rows, self.batches, self.partitions, self.streams
        )
    }
}
