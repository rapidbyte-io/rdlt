//! What a commit leaves in its pipeline's store besides its rows: its state and its receipt.

use rdlt_connector::{CommitMeta, Receipt, StateChange};

use super::PipelineStore;

impl PipelineStore {
    /// Applies `meta`'s state changes and keeps `receipt`, forgetting the receipts of the
    /// commits before the horizon `meta` declares, which the engine never repeats.
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
        if let Some(horizon) = &meta.horizon {
            self.receipts.retain(|key, _| horizon.keeps(key.0, key.1));
        }
        self.receipts
            .insert((meta.load_id, meta.commit_seq), receipt.clone());
    }
}
