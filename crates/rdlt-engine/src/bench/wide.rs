//! Wide tables: rows of thousands of columns, pushed as Arrow batches or as JSON, loaded through
//! the engine into a destination that discards them, so each flush's work on its schema and
//! columns shows; tables kept to some of their columns by excluding the others; and sparse
//! tables, whose rows each hold a few of their many columns.

#[cfg(test)]
mod tests;

use std::num::{NonZeroU32, NonZeroU64};
use std::ops::Range;
use std::sync::Arc;

use rdlt_connector::{Destination, Source};

use super::corpora::wide_lines;
use super::runner::{Runner, stream};
use super::{Corpus, Mix, PUSH_BYTES, Replayed, logical_bytes, null_sink, replay};
use crate::fixtures::{events, events_in};
use crate::{ComputePoolError, Cores, Report, StreamPlan};

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
    /// Columns of a sparse table.
    pub const SPARSE: u16 = 5_000;
    /// Columns each row of a sparse table holds, and the columns a kept table keeps.
    pub const KEPT: u16 = 100;
    /// Pushes of a run of a sparse table: each of its 50 windows of columns twice.
    pub const SPARSE_PUSHES: u32 = 100;
    /// Pushes of a run of a kept table.
    pub const KEPT_PUSHES: u32 = Self::PUSHES + 1;

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
        let replayed = Self::replayed(form, columns, pushes);
        let rows = Self::rows_of(form, columns, pushes);
        Self::loading(cores, stream(), replayed, rows)
    }

    /// The pushes [`Wide::try_new`] loads, made the same in any process.
    ///
    /// # Panics
    ///
    /// Panics where `columns` is zero.
    pub fn replayed(form: Form, columns: u16, pushes: u32) -> Replayed {
        let per_push = per_push(form, columns);
        match form {
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
        }
    }

    /// `pushes` Arrow pushes of a table of `columns` columns, as many rows a push as a sparse
    /// table's, the stream keeping the first `kept` of them by excluding the rest, which the source
    /// pushes; and an engine within `cores` to load them.
    ///
    /// # Errors
    ///
    /// When the compute pool's threads do not start.
    ///
    /// # Panics
    ///
    /// Panics where the runtime does not start, or `kept` is zero or more than `columns`.
    pub fn keeping(
        cores: Cores,
        columns: u16,
        kept: u16,
        pushes: u32,
    ) -> Result<Self, ComputePoolError> {
        assert!(
            kept > 0 && kept <= columns,
            "a table keeps from one to all of its {columns} columns, not {kept}"
        );
        let per_push = per_push(Form::Arrow, Self::SPARSE);
        let batches: Vec<_> = (0..pushes)
            .map(|push| {
                let first = i64::from(push) * i64::from(per_push);
                events(first, per_push, usize::from(columns))
            })
            .collect();
        let excluded: Vec<String> = batches.first().map_or_else(Vec::new, |batch| {
            let schema = batch.schema();
            let fields = schema.fields().iter().skip(usize::from(kept));
            fields.map(|field| field.name().clone()).collect()
        });
        let stream = stream().exclude(excluded.iter().map(String::as_str));
        let rows = u64::from(per_push) * u64::from(pushes);
        Self::loading(cores, stream, Replayed::Batches(batches), rows)
    }

    /// `pushes` pushes in `form` of a table of [`Wide::SPARSE`] columns whose rows each hold
    /// [`Wide::KEPT`] of them, as many rows a push as an Arrow push of every column holds: every
    /// row of a push holds the same window of columns, and the pushes take the windows in turn;
    /// and an engine within `cores` to load them.
    ///
    /// # Errors
    ///
    /// When the compute pool's threads do not start.
    ///
    /// # Panics
    ///
    /// Panics where the runtime does not start.
    pub fn sparse(cores: Cores, form: Form, pushes: u32) -> Result<Self, ComputePoolError> {
        let per_push = per_push(Form::Arrow, Self::SPARSE);
        let replayed = match form {
            Form::Arrow => Replayed::Batches(
                (0..pushes)
                    .map(|push| {
                        let first = i64::from(push) * i64::from(per_push);
                        let ids: Vec<i64> = (first..first + i64::from(per_push)).collect();
                        let window = window(push);
                        events_in(&ids, usize::from(window.start)..usize::from(window.end))
                    })
                    .collect(),
            ),
            Form::Json => {
                let mut mix = Mix::new(SPARSE_SEED);
                Replayed::Json(
                    (0..pushes)
                        .map(|push| wide_lines(&window(push), per_push, &mut mix))
                        .collect(),
                )
            }
        };
        let rows = u64::from(per_push) * u64::from(pushes);
        Self::loading(cores, stream(), replayed, rows)
    }

    /// `replayed`, `rows` rows of `stream`, and an engine within `cores` to load them.
    fn loading(
        cores: Cores,
        stream: StreamPlan,
        replayed: Replayed,
        rows: u64,
    ) -> Result<Self, ComputePoolError> {
        let bytes = match &replayed {
            Replayed::Batches(batches) => logical_bytes(batches),
            Replayed::Json(pushes) | Replayed::Rows(pushes) => {
                pushes.iter().map(|push| push.len() as u64).sum()
            }
        };
        Ok(Self {
            runner: Runner::try_new(cores, "wide", stream, None)?,
            replayed,
            rows,
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
    /// Panics where the run fails, loads other rows than the pushes hold, or discards a value.
    pub fn run(&self) -> u64 {
        let replayed = self.replayed.clone();
        let (source, destination) = self
            .runner
            .block_on(async { (replay("wide", replayed).await, null_sink().await) });
        self.load(source, destination).rows
    }

    /// Runs the engine from `source`, which pushes the pushes, into `destination`; its report.
    ///
    /// # Panics
    ///
    /// Panics where the run fails, loads other rows than the pushes hold, or discards a value.
    pub fn load(&self, source: Arc<dyn Source>, destination: Arc<dyn Destination>) -> Report {
        let report = self.runner.load(source, destination);
        assert_eq!(report.rows, self.rows);
        let discarded: u64 = report
            .streams
            .values()
            .map(|stream| stream.discarded_values)
            .sum();
        assert_eq!(discarded, 0, "no stream discards a value");
        report
    }

    /// Runs `future` on the workload's runtime, where connectors served from this process run.
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.runner.block_on(future)
    }
}

/// The seed of a sparse table's JSON values.
const SPARSE_SEED: u64 = 11;

/// The window of a sparse table's columns the rows of push `push` hold: the windows of
/// [`Wide::KEPT`] columns from the first, in turn.
fn window(push: u32) -> Range<u16> {
    let windows = u32::from(Wide::SPARSE / Wide::KEPT);
    let first = u16::try_from(push % windows).expect("a window's place fits") * Wide::KEPT;
    first..first + Wide::KEPT
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
