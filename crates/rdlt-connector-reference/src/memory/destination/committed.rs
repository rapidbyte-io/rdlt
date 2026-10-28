//! What a commit leaves in its pipeline's store besides its rows: its state and its receipt.

use rdlt_connector::{CommitMeta, Fence, Receipt, StateChange};

use super::PipelineStore;

impl PipelineStore {
    /// The pipeline's fence row: its epoch and the receipt of its last commit that landed.
    pub(super) fn fence(&self) -> Fence {
        Fence {
            epoch: self.epoch,
            landed: self.landed.clone(),
        }
    }

    /// Applies `meta`'s state changes and keeps `receipt` as the last commit's that landed.
    pub(super) fn committed(&mut self, meta: &CommitMeta, receipt: &Receipt) {
        for change in &meta.state_delta {
            match change {
                StateChange::Put(record) => {
                    self.state.insert(record.key.clone(), record.clone());
                }
                StateChange::Delete(key) => {
                    self.state.remove(key);
                }
            }
        }
        self.landed = Some(receipt.clone());
    }
}
