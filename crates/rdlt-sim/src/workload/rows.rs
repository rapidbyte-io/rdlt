//! A stream's rows: drawn for each phase, read from where a phase's read starts, and cut into the
//! batches and checkpoint groups a read sends.

use std::ops::Range;

use rdlt_connector::{Checkpointing, ReadMode};
use rdlt_testkit::draw::mix;
use rdlt_testkit::drawn::Scalar;

use super::{PHASES, Relaxed, Row, SimStream, values};

impl SimStream {
    /// The rows of `partition` the source reads in `phase`: every row of a full read, and an
    /// incremental read's rows new in `phase`, none where a read finished it in the phase before.
    pub fn read(&self, partition: usize, phase: usize) -> &[Row] {
        let rows = self.rows(partition, phase);
        &rows[self.start(partition, phase).min(rows.len())..]
    }

    /// The offset `partition`'s read starts from in `phase`: where the last phase's ended, for an
    /// incremental read, however the last phase read it, and else 0.
    fn start(&self, partition: usize, phase: usize) -> usize {
        match (self.reads[phase], phase.checked_sub(1)) {
            (ReadMode::Incremental, Some(last)) => {
                usize::try_from(self.partitions[partition][last]).unwrap_or(usize::MAX)
            }
            _ => 0,
        }
    }

    /// The offsets of the rows each batch of `partition` in `phase` holds, in order: a read
    /// resumes only at a checkpoint, and checkpoints fall between batches, so every run sends the
    /// same batches.
    pub fn batches(&self, partition: usize, phase: usize) -> Vec<Range<usize>> {
        let end = self.rows(partition, phase).len();
        let size = usize::try_from(self.batch_rows).unwrap_or(1);
        (self.start(partition, phase)..end)
            .step_by(size)
            .map(|start| start..(start + size).min(end))
            .collect()
    }

    /// How many rows of `partition` a read in `phase` serves once `arrived` of them have arrived:
    /// whole checkpoint groups (whole batches where the stream checkpoints on demand), so every
    /// read sends the batches and checkpoints [`span`](Self::span) models, and every row once
    /// they all have.
    pub fn servable(&self, partition: usize, phase: usize, arrived: usize) -> usize {
        let rows = self.rows(partition, phase).len();
        if arrived >= rows {
            return rows;
        }
        let batches = match self.checkpointing {
            Checkpointing::Natural => self.checkpoint_every,
            Checkpointing::OnDemand => 1,
        };
        let group = usize::try_from(self.batch_rows.saturating_mul(batches)).unwrap_or(usize::MAX);
        let start = self.start(partition, phase);
        if arrived <= start {
            return start.min(arrived);
        }
        start + (arrived - start) / group * group
    }

    /// The offsets of the rows whose pushes the engine may shred together with the push holding
    /// row `offset` of `partition` in `phase`: those between the checkpoints around it, as the
    /// engine gathers pushes only up to a checkpoint, and where the stream checkpoints on demand,
    /// wherever the engine asks, every row the phase reads.
    pub fn span(&self, partition: usize, phase: usize, offset: usize) -> Range<usize> {
        let batches = self.batches(partition, phase);
        let every = match self.checkpointing {
            Checkpointing::Natural => usize::try_from(self.checkpoint_every).unwrap_or(1),
            Checkpointing::OnDemand => batches.len().max(1),
        };
        let at = batches
            .iter()
            .position(|batch| batch.contains(&offset))
            .unwrap_or(0);
        let group = &batches[at / every * every..batches.len().min((at / every + 1) * every)];
        match (group.first(), group.last()) {
            (Some(first), Some(last)) => first.start..last.end,
            _ => offset..offset,
        }
    }

    /// The phase that first delivers row `offset` of `partition`: a phase read incrementally keeps
    /// the rows the phase before held, and a phase read in full delivers every row anew.
    fn delivered(&self, partition: usize, offset: u64, phase: usize) -> usize {
        let mut delivered = phase;
        while delivered > 0
            && self.reads[delivered] == ReadMode::Incremental
            && offset < self.partitions[partition][delivered - 1]
        {
            delivered -= 1;
        }
        delivered
    }

    /// Fits each partition's rows to how the stream's reads end: a bounded partition that a
    /// first phase's read ending on rows finishes does not grow into an incremental next phase, as
    /// it is done; and where an incremental first phase leaves the rows after its last checkpoint
    /// to the next read, each partition holding rows holds two or more whole checkpoint groups, so
    /// the rows left are one whole group, which the next phase reads again in the same batches.
    pub(super) fn fit_partitions(&mut self) {
        let incremental = self.reads[1] == ReadMode::Incremental;
        let finished = !self.final_checkpoint && !self.unbounded && incremental;
        let tailed = self.tails() && self.reads[0] == ReadMode::Incremental;
        let group = self.batch_rows * self.checkpoint_every;
        for rows in self.partitions.iter_mut().filter(|rows| rows[0] > 0) {
            if finished {
                rows[1] = rows[0];
            }
            if tailed {
                let fitted = rows[0].div_ceil(group).max(2) * group;
                if incremental {
                    rows[1] += fitted - rows[0];
                }
                rows[0] = fitted;
            }
        }
    }

    /// Whether its incremental reads of a partition that never ends, where they end on rows,
    /// leave the rows after their last checkpoint to the next read: it checkpoints naturally, so
    /// where that checkpoint falls is the workload's to say.
    pub fn tails(&self) -> bool {
        !self.final_checkpoint && self.unbounded && self.checkpointing == Checkpointing::Natural
    }

    /// Where `partition`'s read in `phase` starts, as the operator left the stream: at its
    /// beginning in the first phase, in a phase read in full and after a reset with its tables;
    /// else where the phase before sealed it.
    pub(crate) fn begins(&self, partition: usize, phase: usize, relaxed: Relaxed) -> usize {
        match phase.checked_sub(1) {
            Some(last) if self.reads[phase] != ReadMode::Full && relaxed.reset != Some(phase) => {
                self.sealed(partition, last, relaxed)
            }
            _ => 0,
        }
    }

    /// The offset up to which `partition`'s rows of `phase` land once the phase converged: all of
    /// them, but where an incremental read of a partition that never ends leaves the rows after
    /// its last checkpoint to the next read, and the operator had not mended the source by then.
    ///
    /// A read that seals none of its rows is refused, and the source mended.
    pub(crate) fn sealed(&self, partition: usize, phase: usize, relaxed: Relaxed) -> usize {
        let end = self.rows(partition, phase).len();
        let mended = relaxed.mended.is_some_and(|at| at <= phase);
        if !self.tails() || mended || self.reads[phase] != ReadMode::Incremental {
            return end;
        }
        let begins = self.begins(partition, phase, relaxed);
        let size = usize::try_from(self.batch_rows).unwrap_or(1);
        let every = usize::try_from(self.checkpoint_every).unwrap_or(1);
        let batches = end.saturating_sub(begins).div_ceil(size);
        if batches <= every {
            return end;
        }
        begins + (batches - 1) / every * every * size
    }

    /// The rows of `partition` that land in `phase`, as the operator left the stream: every row
    /// of a phase read in full, at each of its completions, and those an incremental read seals.
    pub(crate) fn landing(&self, partition: usize, phase: usize, relaxed: Relaxed) -> &[Row] {
        let rows = self.rows(partition, phase);
        match self.reads[phase] {
            ReadMode::Full => rows,
            _ => {
                &rows
                    [self.begins(partition, phase, relaxed)..self.sealed(partition, phase, relaxed)]
            }
        }
    }

    /// The rows `partition` holds in each phase; a row delivered in an earlier phase is that
    /// phase's row again.
    pub(super) fn draw_rows(&self, salt: u64, partition: usize) -> [Vec<Row>; PHASES] {
        let mut phases: [Vec<Row>; PHASES] = Default::default();
        for phase in 0..PHASES {
            let count = self.partitions[partition][phase];
            let rows = (0..count)
                .map(|offset| {
                    let delivered = self.delivered(partition, offset, phase);
                    let position = usize::try_from(offset).unwrap_or(usize::MAX);
                    match phases[delivered].get(position) {
                        Some(row) if delivered < phase => row.clone(),
                        _ => self.row(salt, partition, offset, delivered),
                    }
                })
                .collect();
            phases[phase] = rows;
        }
        phases
    }

    /// Row `offset` of `partition` as `delivered` delivers it.
    fn row(&self, salt: u64, partition: usize, offset: u64, delivered: usize) -> Row {
        let index = i64::try_from(partition).unwrap_or(i64::MAX);
        let position = i64::try_from(offset).unwrap_or(i64::MAX);
        let id = index * 1_000_000 + position;
        // Incremental rows never change; a full read sees new values in each phase.
        let version = delivered as u64 + 1;
        let value = mix(salt ^ mix(id.unsigned_abs() ^ (version << 48)));
        let first = if self.shared_keys { 0 } else { index * 16 };
        let key = (self.keys > 0).then(|| first + i64::try_from(offset % self.keys).unwrap_or(0));
        let tag = (self.keys > 0 && self.composite).then(|| format!("t{}", offset % 3));
        let extras = self
            .drift
            .iter()
            .enumerate()
            .map(|(column, drift)| {
                let shape = drift.shapes[partition][delivered].as_ref()?;
                let seed = mix(value ^ (column as u64 + 1));
                let keep = |value: &Scalar| match (self.json, self.named_floats) {
                    (false, _) => values::convergent(value, &shape.logical),
                    (true, named) => named || values::finite(value),
                };
                Some(values::drawn(shape, seed, keep))
            })
            .collect();
        Row {
            id,
            partition: index,
            offset: position,
            value: i64::from_ne_bytes(value.to_ne_bytes()),
            key,
            tag,
            extras,
            delivered,
        }
    }

    /// The rows `partition` holds in `phase`, in order.
    pub fn rows(&self, partition: usize, phase: usize) -> &[Row] {
        &self.rows[partition][phase]
    }

    /// Every row of the stream in `phase`.
    pub fn all_rows(&self, phase: usize) -> Vec<Row> {
        (0..self.partitions.len())
            .flat_map(|partition| self.rows(partition, phase).iter().cloned())
            .collect()
    }
}
