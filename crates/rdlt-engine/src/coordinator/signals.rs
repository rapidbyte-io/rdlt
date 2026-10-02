//! What a source tells the coordinator beside its data: that a stream's partitions changed, how
//! far its reads are behind, and where it read again after its retention dropped their place.

use std::collections::{BTreeMap, BTreeSet};

use rdlt_connector::PartitionId;

use super::Coordinator;
use crate::error::Error;

impl Coordinator {
    /// Notes that `partition`'s source said its stream's partitions changed.
    pub(super) fn signalled(&mut self, partition: usize) {
        let stream = self.parts.partitions[partition].stream;
        self.replans.insert(stream);
    }

    /// Plans again, now, each stream whose source said its partitions changed, where the run
    /// follows its source and is not stopping; a run that does not follow plans only at phases.
    pub(super) async fn replan_signalled(&mut self) -> Result<(), Error> {
        let replans = std::mem::take(&mut self.replans);
        if !self.parts.follow || self.stopping {
            return Ok(());
        }
        for stream in replans {
            if self.parts.streams[stream].phases.is_some() {
                self.replan_stream(stream).await?;
            }
        }
        Ok(())
    }

    /// Records that `partition`'s read is `records` behind its source's newest, unless the read
    /// was asked to stop, and the total its stream's partitions last said, for the report.
    pub(super) fn behind(&mut self, partition: usize, records: u64) {
        let run = &self.parts.partitions[partition];
        if run.stop.is_cancelled() {
            return;
        }
        let stream = run.stream;
        let lag = self.lag.entry(stream).or_default();
        let before = lag.each.insert(run.id.clone(), records).unwrap_or(0);
        lag.total = lag.total - u128::from(before) + u128::from(records);
        self.report_lag(stream);
    }

    /// Forgets how far behind `stream`'s partitions `forgotten` were, as a partition a plan no
    /// longer names, or one of a phase that ended, no longer lags; every partition where `None`.
    pub(super) fn forget_lag(&mut self, stream: usize, forgotten: Option<&BTreeSet<PartitionId>>) {
        let Some(lag) = self.lag.get_mut(&stream) else {
            return;
        };
        match forgotten {
            Some(forgotten) => {
                for id in forgotten {
                    if let Some(records) = lag.each.remove(id) {
                        lag.total -= u128::from(records);
                    }
                }
            }
            None => *lag = Lag::default(),
        }
        self.report_lag(stream);
    }

    /// Reports the total `stream`'s partitions last said they are behind; unknown where none
    /// that still lags said.
    fn report_lag(&self, stream: usize) {
        let name = self.parts.streams[stream].name.clone();
        let lag = self.lag.get(&stream).filter(|lag| !lag.each.is_empty());
        let mut log = self.parts.log.lock();
        match lag {
            Some(lag) => {
                let total = u64::try_from(lag.total).unwrap_or(u64::MAX);
                log.behind.insert(name, total);
            }
            None => {
                log.behind.remove(&name);
            }
        }
    }

    /// Counts that `partition`'s source had dropped where its read would resume, for the report.
    pub(super) fn reset(&mut self, partition: usize) {
        let stream = self.parts.partitions[partition].stream;
        let name = self.parts.streams[stream].name.clone();
        *self
            .parts
            .log
            .lock()
            .retention_resets
            .entry(name)
            .or_default() += 1;
    }
}

/// How far behind a stream's partitions last said they are, and their total, kept as each says:
/// a total of at most a `u64` for each of fewer than `u64::MAX` partitions fits a `u128`.
#[derive(Debug, Default)]
pub(super) struct Lag {
    each: BTreeMap<PartitionId, u64>,
    total: u128,
}
