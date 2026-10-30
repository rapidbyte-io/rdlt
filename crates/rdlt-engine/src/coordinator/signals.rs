//! What a source tells the coordinator beside its data: that a stream's partitions changed, how
//! far its reads are behind and how much more its retention keeps, and where it read again after
//! its retention dropped their place.

use std::collections::{BTreeMap, BTreeSet};

use rdlt_connector::{Headroom, PartitionId};

use super::Coordinator;
use crate::error::{Error, shown};
use crate::event::EventKind;

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

    /// Records that `partition`'s read is `records` behind its source's newest, with `headroom`
    /// left in its source's retention, unless the read was asked to stop; and the total its
    /// stream's partitions last said, with the least headroom, for the report.
    pub(super) fn behind(&mut self, partition: usize, records: u64, headroom: Headroom) {
        let run = &self.parts.partitions[partition];
        if run.stop.is_cancelled() {
            return;
        }
        let stream = run.stream;
        let lag = self.lag.entry(stream).or_default();
        let (before, held) = lag
            .each
            .insert(run.id.clone(), (records, headroom))
            .unwrap_or_default();
        lag.total = lag.total - u128::from(before) + u128::from(records);
        if held != headroom {
            lag.least();
        }
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
                    if let Some((records, _)) = lag.each.remove(id) {
                        lag.total -= u128::from(records);
                    }
                }
                lag.least();
            }
            None => *lag = Lag::default(),
        }
        self.report_lag(stream);
    }

    /// Reports the total `stream`'s partitions last said they are behind, and the least
    /// headroom they said; unknown where none that still lags said.
    fn report_lag(&self, stream: usize) {
        let name = self.parts.streams[stream].name.clone();
        let lag = self.lag.get(&stream).filter(|lag| !lag.each.is_empty());
        let mut log = self.parts.log.lock();
        match lag {
            Some(lag) => {
                let total = u64::try_from(lag.total).unwrap_or(u64::MAX);
                log.behind.insert(name, (total, lag.headroom));
            }
            None => {
                log.behind.remove(&name);
            }
        }
    }

    /// Counts that `partition`'s source had dropped where its read would resume, for the report,
    /// and tells of it.
    pub(super) fn reset(&mut self, partition: usize) {
        let run = &self.parts.partitions[partition];
        let (name, id) = (self.parts.streams[run.stream].name.clone(), run.id.clone());
        *self
            .parts
            .log
            .lock()
            .retention_resets
            .entry(name.clone())
            .or_default() += 1;
        self.parts.announcer.announce(|| EventKind::RetentionReset {
            stream: shown(&name),
            partition: shown(&id),
        });
    }
}

/// How far behind a stream's partitions last said they are and the headroom each said, their
/// total and the least headroom, kept as each says: a total of at most a `u64` for each of fewer
/// than `u64::MAX` partitions fits a `u128`.
#[derive(Debug, Default)]
pub(super) struct Lag {
    each: BTreeMap<PartitionId, (u64, Headroom)>,
    total: u128,
    headroom: Headroom,
}

impl Lag {
    /// Finds the least headroom its partitions said again, as one's changed.
    fn least(&mut self) {
        let each = self.each.values().map(|(_, headroom)| *headroom);
        self.headroom = each.fold(Headroom::default(), Headroom::least);
    }
}
