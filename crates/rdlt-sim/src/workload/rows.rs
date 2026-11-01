//! A stream's rows: drawn for each phase, read from where a phase's read starts, and cut into the
//! batches and checkpoint groups a read sends.

use std::ops::Range;

use rdlt_connector::{Checkpointing, ReadMode};
use rdlt_testkit::draw::mix;
use rdlt_testkit::drawn::Scalar;

use super::{PHASES, Row, SimStream, values};

impl SimStream {
    /// The rows of `partition` the source reads in `phase`: every row of a full read, and an
    /// incremental read's rows new in `phase`.
    pub fn read(&self, partition: usize, phase: usize) -> &[Row] {
        let rows = self.rows(partition, phase);
        &rows[self.start(partition, phase).min(rows.len())..]
    }

    /// The offset `partition`'s read starts from in `phase`: where the last phase's ended, for an
    /// incremental read, and else 0.
    fn start(&self, partition: usize, phase: usize) -> usize {
        match (self.read, phase.checked_sub(1)) {
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

    /// The phase that first delivers row `offset` of `partition`: incremental rows keep the phase
    /// they first appeared in, full reads deliver every row again in each phase.
    fn delivered(&self, partition: usize, offset: u64, phase: usize) -> usize {
        if self.read != ReadMode::Incremental {
            return phase;
        }
        (0..=phase)
            .find(|earlier| offset < self.partitions[partition][*earlier])
            .unwrap_or(phase)
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
