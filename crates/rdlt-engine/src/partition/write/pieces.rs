//! A unit cut into the pieces the engine lowers within its budget: a few bytes of encoded
//! columns may decode to far more, and a column may be stored in a wider type than it arrives
//! in, so a unit is lowered a piece of its rows at a time, each measured as its table stores it.

#[cfg(test)]
mod tests;

use std::collections::VecDeque;
use std::sync::Arc;

use arrow_array::RecordBatch;
use parking_lot::Mutex;
use rdlt_connector::cost::{Allocations, Measure, Rendering, Stored};

use super::Held;
use super::held::Share;

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
    /// Bytes: what lowering a piece takes whatever its rows: the arrays of the batch it becomes.
    pub(super) piece: u64,
    /// Bytes: what each part of a piece takes whatever its rows, the arrays of its slice of a
    /// batch; once more where a piece has several, for the batch they are joined into.
    pub(super) part: u64,
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
    /// Bytes: what lowering the rows holds at once, as they were measured, and what the piece
    /// and its parts take whatever their rows.
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
    /// The batch being cut, the row its next run starts at, and its measures: against a piece's
    /// bytes, and against the budget for a row beyond a piece.
    part: usize,
    row: usize,
    measures: Option<(Measure, Measure)>,
    /// The run cut last that the piece before it had no room for.
    cut: Option<Run>,
    /// Pieces cut ahead, in order, before any of them is lowered.
    ready: VecDeque<Piece>,
    /// The unit's batches, which what holds the unit keeps too: the allocations it counts each
    /// piece's growth beyond stay alive until its last piece goes.
    parts: Vec<RecordBatch>,
    /// What holds the unit, which each of its pieces shares: last, so the batches and the pieces
    /// cut ahead of them go before it.
    unit: Share,
}

impl Pieces {
    /// `unit`, batches of one schema, to be cut as `lowered` measures them.
    pub(super) fn new(unit: Held, lowered: Lowered) -> Self {
        Self {
            lowered,
            part: 0,
            row: 0,
            measures: None,
            cut: None,
            ready: VecDeque::new(),
            parts: unit.batches.clone(),
            unit: Arc::new(Mutex::new(unit)),
        }
    }

    /// No batches: what stands in for a unit's while they are cut elsewhere.
    pub(super) fn none() -> Self {
        Self::new(
            Held::default(),
            Lowered {
                rendering: Rendering::text(),
                stored: Vec::new(),
                row: 0,
                item: 0,
                piece: 0,
                part: 0,
                max: 0,
                limit: 0,
            },
        )
    }

    /// Whether every row has been cut into a piece and every piece cut ahead taken.
    pub(super) fn done(&self) -> bool {
        self.ready.is_empty() && self.cut_all()
    }

    /// Whether every row has been cut into a piece.
    fn cut_all(&self) -> bool {
        self.part >= self.parts.len()
    }

    /// The allocations the unit's batches keep alive, which its pieces share.
    pub(super) fn allocations(&self) -> Arc<Allocations> {
        Arc::clone(&self.unit.lock().allocations)
    }

    /// Holds `unit`, the same batches, in place of what held them.
    pub(super) fn held_by(&mut self, unit: Held) {
        self.parts.clone_from(&unit.batches);
        self.unit = Arc::new(Mutex::new(unit));
    }

    /// A share of what holds the unit, for one of its pieces: the unit goes once the last share
    /// does.
    pub(super) fn share(&self) -> Share {
        Arc::clone(&self.unit)
    }

    /// Cuts every piece that is left ahead, to be taken with [`Pieces::ready`], returning the
    /// bytes the largest of them takes to lower.
    ///
    /// # Errors
    ///
    /// A [`RowTooLarge`] for a row that alone takes more than the budget.
    pub(super) fn cut_ahead(&mut self) -> Result<u64, RowTooLarge> {
        while let Some(piece) = self.next()? {
            self.ready.push_back(piece);
        }
        Ok(self.ready.iter().map(|piece| piece.bytes).fold(0, u64::max))
    }

    /// The next piece cut ahead, where one is left.
    pub(super) fn ready(&mut self) -> Option<Piece> {
        self.ready.pop_front()
    }

    /// The next piece: the rows that follow, as many as lowering holds within a piece's bytes,
    /// or one row where a row alone takes more; nothing once every row was cut.
    ///
    /// Pieces keep the rows' order, and a batch within a piece's bytes is not cut. Pieces cut
    /// ahead are not among them.
    ///
    /// # Errors
    ///
    /// A [`RowTooLarge`] for a row that alone takes more than the budget.
    pub(super) fn next(&mut self) -> Result<Option<Piece>, RowTooLarge> {
        let mut piece = Piece {
            parts: Vec::new(),
            rows: 0,
            bytes: self.lowered.piece,
        };
        while !self.cut_all() {
            let rows = self.parts[self.part].num_rows();
            if self.row >= rows {
                (self.part, self.row, self.measures) = (self.part + 1, 0, None);
                continue;
            }
            let run = match self.cut.take() {
                Some(run) => run,
                None => self.run()?,
            };
            // A second part joins the first into a batch of their own.
            let parts = if piece.parts.len() == 1 { 2 } else { 1 };
            let bytes = run
                .bytes
                .saturating_add(self.lowered.part.saturating_mul(parts));
            if piece.rows > 0 && piece.bytes.saturating_add(bytes) > self.lowered.max {
                self.cut = Some(run);
                break;
            }
            piece
                .parts
                .push(self.parts[self.part].slice(self.row, run.rows));
            piece.rows += run.rows;
            piece.bytes = piece.bytes.saturating_add(bytes);
            self.row += run.rows;
        }
        Ok((piece.rows > 0).then_some(piece))
    }

    /// Whether cutting the batches left reads their values, or measures them by their rows and
    /// offsets alone: the unit's batches are of one schema, so the batch being cut says.
    pub(super) fn reads_values(&mut self) -> bool {
        self.measures()
            .is_some_and(|(pieces, _)| pieces.reads_values())
    }

    /// The measures of the batch being cut: against what a piece's rows may take, and against
    /// what a row's may; none once every batch is cut.
    fn measures(&mut self) -> Option<&mut (Measure, Measure)> {
        let lowered = &self.lowered;
        let (max, limit) = self.rows_within();
        let batch = self.parts.get(self.part)?;
        Some(self.measures.get_or_insert_with(|| {
            let measure = |limit| {
                let stored = lowered.stored.clone();
                let measure = lowered
                    .rendering
                    .lowering(batch, stored, lowered.row, limit);
                measure.with_items(lowered.item)
            };
            (measure(max), measure(limit))
        }))
    }

    /// Bytes: what a piece of one part takes whatever its rows.
    fn beside(&self) -> u64 {
        self.lowered.piece.saturating_add(self.lowered.part)
    }

    /// Bytes: the most the rows of a piece of one part may take, and a row's alone, beside what
    /// the piece takes whatever its rows.
    fn rows_within(&self) -> (u64, u64) {
        let beside = self.beside();
        (
            self.lowered.max.saturating_sub(beside),
            self.lowered.limit.saturating_sub(beside),
        )
    }

    /// The next run of the batch being cut, from its row `self.row`.
    fn run(&mut self) -> Result<Run, RowTooLarge> {
        let row = self.row;
        let ((max, limit), beside) = (self.rows_within(), self.beside());
        let (pieces, alone) = self
            .measures()
            .expect("a run is cut only from a batch left to cut");
        let cut = pieces.piece(row);
        let (rows, mut bytes) = (cut.end - row, cut.bytes);
        if bytes > max {
            // One row beyond a piece: measured against what a row may take, each value it names
            // once.
            bytes = alone.expanded(row..cut.end);
            if bytes > limit {
                return Err(RowTooLarge {
                    expanded: bytes.saturating_add(beside),
                    limit: self.lowered.limit,
                });
            }
        }
        Ok(Run { rows, bytes })
    }
}

/// `unit` cut into pieces as `lowered` measures them, before its tables are known: what holds
/// each piece's batches, in order, each with a share of what holds the unit, so its permits hold
/// until all of it is written.
///
/// # Errors
///
/// A [`RowTooLarge`] for a row that alone takes more than a row may.
pub(super) fn sliced(unit: Held, lowered: Lowered) -> Result<Vec<Held>, RowTooLarge> {
    let mut pieces = Pieces::new(unit, lowered);
    let mut cut = Vec::new();
    while let Some(piece) = pieces.next()? {
        let mut held = pieces.unit.lock().piece(piece.parts);
        held.permits.push(Box::new(pieces.share()));
        cut.push(held);
    }
    Ok(cut)
}
