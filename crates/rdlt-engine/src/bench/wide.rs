//! Wide tables: rows of thousands of columns, pushed as Arrow batches or as JSON, loaded through
//! the engine into a destination that discards them, so each flush's work on its schema and
//! columns shows.

#[cfg(test)]
mod tests;

use std::num::{NonZeroU32, NonZeroU64};

use super::runner::{Runner, stream};
use super::{Corpus, PUSH_BYTES, Replayed, logical_bytes, null_sink, replay};
use crate::fixtures::events;
use crate::{ComputePoolError, Cores};

/// The form a wide table's rows are pushed in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Form {
    /// As Arrow batches of the ten mixed kinds of column, by turns.
    Arrow,
    /// As JSON records of integers and short strings, by turns.
    Json,
}

/// Rows of a table of many columns, and an engine to load them.
#[derive(Debug)]
pub struct Wide {
    runner: Runner,
    replayed: Replayed,
    rows: u64,
    bytes: u64,
}

impl Wide {
    /// The tables' widths.
    pub const COLUMNS: [u16; 2] = [1_000, 5_000];
    /// Pushes of each run, each of about the default coalescing target.
    pub const PUSHES: u32 = 8;

    /// `pushes` pushes of rows of `columns` columns pushed in `form`, each of about the default
    /// coalescing target, and a runtime of `cores`' workers and an engine within `cores` to load
    /// them.
    ///
    /// # Errors
    ///
    /// When the compute pool's threads do not start.
    ///
    /// # Panics
    ///
    /// Panics where the runtime does not start or `columns` is zero.
    pub fn try_new(
        cores: Cores,
        form: Form,
        columns: u16,
        pushes: u32,
    ) -> Result<Self, ComputePoolError> {
        let per_push = per_push(form, columns);
        let replayed = match form {
            Form::Arrow => Replayed::Batches(
                (0..pushes)
                    .map(|push| {
                        let first = i64::from(push) * i64::from(per_push);
                        events(first, per_push, usize::from(columns))
                    })
                    .collect(),
            ),
            Form::Json => {
                let rows = u64::from(per_push) * u64::from(pushes);
                let per_push = NonZeroU64::from(NonZeroU32::new(per_push).expect("a row a push"));
                Replayed::Json(Corpus::Wide(columns).rows(rows, per_push))
            }
        };
        let bytes = match &replayed {
            Replayed::Batches(batches) => logical_bytes(batches),
            Replayed::Json(pushes) => pushes.iter().map(|push| push.len() as u64).sum(),
        };
        Ok(Self {
            runner: Runner::try_new(cores, "wide", stream(), None)?,
            replayed,
            rows: Self::rows_of(form, columns, pushes),
            bytes,
        })
    }

    /// The bytes each run pushes: Arrow's logical bytes, or JSON's text.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// The rows each run loads.
    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// The rows a run of `pushes` pushes of rows of `columns` columns in `form` loads, known
    /// before the rows are made.
    pub fn rows_of(form: Form, columns: u16, pushes: u32) -> u64 {
        u64::from(per_push(form, columns)) * u64::from(pushes)
    }

    /// Runs the engine from a source replaying the pushes into a sink that discards them; the
    /// rows it reports.
    ///
    /// # Panics
    ///
    /// Panics where the run fails or loads other rows than the pushes hold.
    pub fn run(&self) -> u64 {
        let replayed = self.replayed.clone();
        let (source, destination) = self
            .runner
            .block_on(async { (replay("wide", replayed).await, null_sink().await) });
        let report = self.runner.load(source, destination);
        assert_eq!(report.rows, self.rows);
        report.rows
    }
}

/// Rows of `columns` columns pushed in `form` each push holds: as many as fill
/// [`PUSH_BYTES`], one at least.
fn per_push(form: Form, columns: u16) -> u32 {
    let row = match form {
        Form::Arrow => logical_bytes(&[events(0, 1, usize::from(columns))]),
        Form::Json => {
            let pushes = Corpus::Wide(columns).rows(1, NonZeroU64::MIN);
            pushes.iter().map(|push| push.len() as u64).sum()
        }
    };
    let rows = (PUSH_BYTES as u64 / row.max(1)).max(1);
    u32::try_from(rows).unwrap_or(u32::MAX)
}
