//! A partition as the coordinator sees it: where its read stands, and how far it has gone.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use rdlt_connector::{Cursor, PartitionId, PartitionState};
use tokio_util::sync::CancellationToken;

use crate::status::Place;

/// A partition as the coordinator sees it.
#[derive(Debug)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "each says one thing of the partition's read: how it checkpoints, whether it holds a \
              place, and how far it has gone"
)]
pub(crate) struct PartitionRun {
    pub(crate) stream: usize,
    pub(crate) id: PartitionId,
    /// Whether the partition checkpoints when asked.
    pub(crate) on_demand: bool,
    /// Whether its read follows an unbounded partition, holding a place among the read slots.
    pub(super) followed: bool,
    pub(super) started: bool,
    pub(super) ended: bool,
    pub(super) answered: u64,
    /// Stops this partition alone.
    pub(super) stop: CancellationToken,
    /// Where the partition stood when its read started, then the position of its last commit
    /// that landed: a commit that records it there again moves it nowhere.
    pub(super) stands: Option<PartitionState>,
    /// When the partition last told the coordinator anything, for the run's status.
    pub(super) stamp: Arc<AtomicU64>,
}

impl PartitionRun {
    pub(crate) fn new(
        stream: usize,
        id: PartitionId,
        on_demand: bool,
        stop: CancellationToken,
    ) -> Self {
        Self {
            stream,
            id,
            on_demand,
            followed: false,
            started: false,
            ended: false,
            answered: 0,
            stop,
            stands: None,
            stamp: Arc::new(AtomicU64::new(Place::WAITING)),
        }
    }

    /// The partition, whose read starts from `cursor`, where the destination holds it.
    pub(crate) fn starting(mut self, cursor: Option<&Cursor>) -> Self {
        self.stands = cursor.cloned().map(PartitionState::Cursor);
        self
    }

    /// The partition, whose read follows it and holds a place among the read slots where
    /// `followed`.
    pub(crate) fn following(mut self, followed: bool) -> Self {
        self.followed = followed;
        self
    }

    /// Whether barrier `barrier` is waiting for this partition.
    pub(super) fn owes(&self, barrier: u64) -> bool {
        self.on_demand && self.started && !self.ended && self.answered < barrier
    }
}
