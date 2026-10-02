//! A unit cut into the pieces the engine lowers within its budget: a few bytes of encoded
//! columns may decode to far more, and a column may be stored in a wider type than it arrives
//! in, so a unit is lowered a piece of its rows at a time, each measured as its table stores it.

#[cfg(test)]
mod tests;

use arrow_array::RecordBatch;
use rdlt_connector::cost::{Measure, Rendering, Stored};

use super::Held;

/// A row that alone takes more to lower than what is asked of the budget for it may.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct RowTooLarge {
    /// Bytes: what lowering the row takes, as far as it was measured.
    pub(super) expanded: u64,
    /// Bytes: the most a row may take.
    pub(super) limit: u64,
}

/// How a unit's batches are measured: as their table stores them, where its plan is known.
#[derive(Clone, Debug)]
pub(super) struct Lowered {
    pub(super) rendering: Rendering,
    /// How the table stores each column of the unit's batches; empty before its plan is found.
    pub(super) stored: Vec<Option<Stored>>,
    /// Bytes: what each row takes beside its columns: the columns of the table the unit holds
    /// nothing in, and the metadata lowering adds.
    pub(super) row: u64,
    /// Bytes: what each item a list names takes beside itself, where items become rows.
    pub(super) item: u64,
    /// Bytes: the most lowering one piece holds, but for a row that alone takes more.
    pub(super) max: u64,
    /// Bytes: the most one row may take, beyond which it is refused.
    pub(super) limit: u64,
}

/// Rows of a unit to lower together, and what lowering them holds at most.
#[derive(Debug)]
pub(super) struct Piece {
    /// The rows, as slices of the unit's batches, in order.
    pub(super) parts: Vec<RecordBatch>,
    pub(super) rows: usize,
    /// Bytes: what lowering the rows holds at once, as they were measured.
    pub(super) bytes: u64,
}

/// A run of rows of one batch, cut and measured.
struct Run {
    rows: usize,
    bytes: u64,
}

/// A unit's batches being cut into pieces, in order.
pub(super) struct Pieces {
    lowered: Lowered,
    parts: Vec<RecordBatch>,
    /// The batch being cut, the row its next run starts at, and its measures: against a piece's
    /// bytes, and against the budget for a row beyond a piece.
    part: usize,
    row: usize,
    measures: Option<(Measure, Measure)>,
    /// The run cut last that the piece before it had no room for.
    cut: Option<Run>,
}

impl Pieces {
    /// `parts`, a unit's batches of one schema, to be cut as `lowered` measures them.
    pub(super) fn new(parts: Vec<RecordBatch>, lowered: Lowered) -> Self {
        Self {
            lowered,
            parts,
            part: 0,
            row: 0,
            measures: None,
            cut: None,
        }
    }

    /// No batches: what stands in for a unit's while they are cut elsewhere.
    pub(super) fn none() -> Self {
        Self::new(
            Vec::new(),
            Lowered {
                rendering: Rendering::text(),
                stored: Vec::new(),
                row: 0,
                item: 0,
                max: 0,
                limit: 0,
            },
        )
    }

    /// Whether every row has been cut into a piece.
    pub(super) fn done(&self) -> bool {
        self.part >= self.parts.len()
    }

    /// The next piece: the rows that follow, as many as lowering holds within a piece's bytes,
    /// or one row where a row alone takes more; nothing once every row was cut.
    ///
    /// Pieces keep the rows' order, and a batch within a piece's bytes is not cut.
    ///
    /// # Errors
    ///
    /// A [`RowTooLarge`] for a row that alone takes more than the budget.
    pub(super) fn next(&mut self) -> Result<Option<Piece>, RowTooLarge> {
        let mut piece = Piece {
            parts: Vec::new(),
            rows: 0,
            bytes: 0,
        };
        while !self.done() {
            let rows = self.parts[self.part].num_rows();
            if self.row >= rows {
                (self.part, self.row, self.measures) = (self.part + 1, 0, None);
                continue;
            }
            let run = match self.cut.take() {
                Some(run) => run,
                None => self.run()?,
            };
            if piece.rows > 0 && piece.bytes.saturating_add(run.bytes) > self.lowered.max {
                self.cut = Some(run);
                break;
            }
            piece
                .parts
                .push(self.parts[self.part].slice(self.row, run.rows));
            piece.rows += run.rows;
            piece.bytes = piece.bytes.saturating_add(run.bytes);
            self.row += run.rows;
        }
        Ok((piece.rows > 0).then_some(piece))
    }

    /// The next run of the batch being cut, from its row `self.row`.
    fn run(&mut self) -> Result<Run, RowTooLarge> {
        let lowered = &self.lowered;
        let batch = &self.parts[self.part];
        let (pieces, alone) = self.measures.get_or_insert_with(|| {
            let measure = |limit| {
                let stored = lowered.stored.clone();
                let measure = lowered
                    .rendering
                    .lowering(batch, stored, lowered.row, limit);
                measure.with_items(lowered.item)
            };
            (measure(lowered.max), measure(lowered.limit))
        });
        let cut = pieces.piece(self.row);
        let (rows, mut bytes) = (cut.end - self.row, cut.bytes);
        if bytes > lowered.max {
            // One row beyond a piece: measured against what a row may take, each value it names
            // once.
            bytes = alone.expanded(self.row..cut.end);
            if bytes > lowered.limit {
                return Err(RowTooLarge {
                    expanded: bytes,
                    limit: lowered.limit,
                });
            }
        }
        Ok(Run { rows, bytes })
    }
}

/// `parts`, a unit `held` holds, cut into pieces as `lowered` measures them, before its tables
/// are known: each piece's batches, in order, what holds it, and what it was measured to take.
///
/// The unit's permits stay with its last piece, so they hold until all of it is written.
///
/// # Errors
///
/// A [`RowTooLarge`] for a row that alone takes more than a row may.
pub(super) fn sliced(
    parts: Vec<RecordBatch>,
    held: Held,
    lowered: Lowered,
) -> Result<Vec<(Vec<RecordBatch>, Held, u64)>, RowTooLarge> {
    let mut pieces = Pieces::new(parts, lowered);
    let mut cut = Vec::new();
    while let Some(piece) = pieces.next()? {
        cut.push((piece.parts, held.piece(), piece.bytes));
    }
    if let Some((_, last, _)) = cut.last_mut() {
        *last = held;
    }
    Ok(cut)
}
