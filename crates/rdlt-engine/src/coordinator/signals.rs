//! What a source tells the coordinator beside its data: that a stream's partitions changed, how
//! far its reads are behind, and where it read again after its retention dropped their place.

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
        self.lag
            .entry(stream)
            .or_default()
            .insert(run.id.clone(), records);
        self.report_lag(stream);
    }

    /// Forgets how far behind `stream`'s partitions `forgotten` were, as a partition a plan no
    /// longer names, or one of a phase that ended, no longer lags; every partition where `None`.
    pub(super) fn forget_lag(&mut self, stream: usize, forgotten: Option<&[PartitionId]>) {
        let Some(lag) = self.lag.get_mut(&stream) else {
            return;
        };
        match forgotten {
            Some(forgotten) => lag.retain(|id, _| !forgotten.contains(id)),
            None => lag.clear(),
        }
        self.report_lag(stream);
    }

    /// Reports the total `stream`'s partitions last said they are behind; unknown where none
    /// that still lags said.
    fn report_lag(&self, stream: usize) {
        let name = self.parts.streams[stream].name.clone();
        let lag = self.lag.get(&stream).filter(|lag| !lag.is_empty());
        let mut log = self.parts.log.lock();
        match lag {
            Some(lag) => {
                let total = lag.values().copied().fold(0_u64, u64::saturating_add);
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
