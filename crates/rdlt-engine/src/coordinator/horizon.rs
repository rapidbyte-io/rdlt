//! The oldest commit a replay may still repeat, which each commit declares so its destination
//! may forget the receipts of the commits before it.

#[cfg(test)]
mod tests;

use rdlt_connector::{CommitSeq, Horizon, LoadId};

use super::Coordinator;
use crate::error::Error;

impl Coordinator {
    /// The oldest commit any replay may repeat as this commit lands, as [`earliest`] finds it
    /// among the loads of the pipeline with a log.
    ///
    /// A load whose log is listed after this attempt's session opened, and not before, opened a
    /// session after it, so this commit fails as fenced: no commit the listing missed landed
    /// where a replay could repeat it against a receipt this commit lets go.
    ///
    /// # Errors
    ///
    /// The log store's failure to list the pipeline's logs.
    pub(super) async fn horizon(&self) -> Result<Horizon, Error> {
        let oldest = self
            .parts
            .wal
            .as_ref()
            .and_then(crate::wal::LoadLog::oldest);
        let loads = match self.parts.env.wal() {
            Some(store) => store
                .loads(&self.parts.pipeline)
                .await
                .map_err(Error::from_wal)?,
            None => Vec::new(),
        };
        Ok(earliest(
            self.parts.load_id,
            oldest.unwrap_or(self.seq),
            &loads,
        ))
    }
}

/// The oldest commit a replay may repeat: the first of every load in `loads` but `load`, whose
/// logs a replay may take over, and `oldest` of `load`'s own, the oldest its log holds or the
/// commit being made.
pub(super) fn earliest(load: LoadId, oldest: CommitSeq, loads: &[LoadId]) -> Horizon {
    let own = Horizon {
        load_id: load,
        commit_seq: oldest,
    };
    loads
        .iter()
        .filter(|other| **other != load)
        .map(|other| Horizon {
            load_id: *other,
            commit_seq: CommitSeq::FIRST,
        })
        .fold(own, Horizon::min)
}
