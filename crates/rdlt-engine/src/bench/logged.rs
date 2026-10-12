//! The passthrough workload logged ahead: batches of about a mebibyte written through a
//! write-ahead log into the Arrow IPC destination, committed every so many batches or once.

#[cfg(test)]
mod tests;

use std::num::NonZeroU64;
use std::sync::Arc;

use arrow_array::RecordBatch;

use super::runner::{Runner, stream};
use super::{Passthrough, Replayed, ipc_sink, replay};
use crate::wal::frame::{self, Frame};
use crate::{ComputePoolError, Cores, Report, WalStore};

/// Batches logged ahead into a store as the engine moves them, and an engine to move them.
#[derive(Debug)]
pub struct Logged {
    runner: Runner,
    batches: Vec<RecordBatch>,
}

impl Logged {
    /// Rows a batch holds: about a mebibyte of the passthrough workload's columns.
    pub const ROWS: u32 = 12_000;
    /// Batches a run moves: about 256 MiB.
    pub const BATCHES: u32 = 256;

    /// `batches` batches of `rows` rows logged into `store`, committed every `every` batches or
    /// once at the end, on a runtime of `cores`' workers and an engine within `cores`.
    ///
    /// # Errors
    ///
    /// When the compute pool's threads do not start.
    pub fn try_new(
        cores: Cores,
        store: Arc<dyn WalStore>,
        batches: u32,
        rows: u32,
        every: Option<NonZeroU64>,
    ) -> Result<Self, ComputePoolError> {
        let commit = every.and_then(|every| NonZeroU64::new(every.get() * u64::from(rows)));
        Ok(Self {
            runner: Runner::logging(cores, "logged", stream(), commit, store)?,
            batches: (0..batches)
                .map(|index| Passthrough::batch(i64::from(index) * i64::from(rows), rows))
                .collect(),
        })
    }

    /// Runs the engine from a source replaying the batches into the IPC sink; its report.
    ///
    /// # Panics
    ///
    /// Panics where the run fails.
    pub fn run(&self) -> Report {
        let replayed = Replayed::Batches(self.batches.clone());
        let source = self.runner.block_on(replay("logged", replayed));
        self.runner.load(source, self.runner.block_on(ipc_sink()))
    }

    /// Bytes: the batch frames of every batch a run logs, as one frame each.
    ///
    /// # Panics
    ///
    /// Panics where a frame does not encode, which a valid batch always does.
    pub fn frame_bytes(&self) -> u64 {
        self.batches
            .iter()
            .map(|batch| {
                let frame = Frame::Batch(frame::Batch {
                    segment: rdlt_connector::SegmentId(1),
                    table: 0,
                    ordinal: 0,
                    batch: batch.clone(),
                });
                frame.encode().expect("the frame encodes").len() as u64
            })
            .sum()
    }
}
